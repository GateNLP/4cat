import functools
from urllib.parse import urlparse

import flask
import google.oauth2.credentials

from authlib.integrations.requests_client import OAuth2Session
from flask_login import current_user, login_required
from flask import request
from googleapiclient.discovery import build
from webtool import app

import common.config_manager as config


AUTH_TOKEN_KEY = 'gdrive_auth_token'
AUTH_STATE_KEY = 'gdrive_auth_state'


def no_cache(view):
	@functools.wraps(view)
	def no_cache_impl(*args, **kwargs):
		response = flask.make_response(view(*args, **kwargs))
		response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
		response.headers['Pragma'] = 'no-cache'
		response.headers['Expires'] = '-1'
		return response

	return functools.update_wrapper(no_cache_impl, view)


def build_credentials():

	if AUTH_TOKEN_KEY not in flask.session:
		raise Exception('User must be logged in')

	oauth2_tokens = flask.session[AUTH_TOKEN_KEY]

	return google.oauth2.credentials.Credentials(
		oauth2_tokens['access_token']
	)


@app.route('/google/login')
@login_required
@no_cache
def login_google():
	flask.session["url_redirect"] = urlparse(request.referrer).path if request.referrer else "/"

	# create session to handle oauth2 steps
	session = OAuth2Session(config.get("GOOGLE_CLIENT_ID"),
							config.get("GOOGLE_CLIENT_SECRET"),
							scope='openid email profile https://www.googleapis.com/auth/drive.file',
							redirect_uri=config.get("GOOGLE_CLIENT_AUTH_URI"))

	auth_url, state = session.create_authorization_url(
		"https://accounts.google.com/o/oauth2/v2/auth?access_type=offline&prompt=consent")

	flask.session[AUTH_STATE_KEY] = state
	flask.session.permanent = True

	return flask.redirect(auth_url, code=302)


@app.route('/api/drive/auth/oauthcallback')
@login_required
@no_cache
def google_auth_redirect():

	req_state = flask.request.args.get('state', default=None, type=None)

	if req_state != flask.session[AUTH_STATE_KEY]:
		response = flask.make_response('Invalid state parameter', 401)
		return response

	session = OAuth2Session(config.get("GOOGLE_CLIENT_ID"),
							config.get("GOOGLE_CLIENT_SECRET"),
							scope='openid email profile https://www.googleapis.com/auth/drive.file',
							state=flask.session[AUTH_STATE_KEY],
							redirect_uri=config.get("GOOGLE_CLIENT_AUTH_URI"))

	oauth2_tokens = session.fetch_access_token(
		"https://www.googleapis.com/oauth2/v4/token",
		authorization_response=flask.request.url)

	flask.session[AUTH_TOKEN_KEY] = oauth2_tokens
	oauth2_tokens = flask.session[AUTH_TOKEN_KEY]

	current_user.set_value("gdrive.accesstoken", oauth2_tokens['access_token'])
	current_user.set_value("gdrive.refreshtoken", oauth2_tokens['refresh_token'])

	return flask.redirect('/google/create-fourcat-folder')


@app.route('/google/logout')
@login_required
@no_cache
def logout_google():
	flask.session.pop(AUTH_TOKEN_KEY, None)
	flask.session.pop(AUTH_STATE_KEY, None)

	current_user.remove_value("gdrive.accesstoken")
	current_user.remove_value("gdrive.refreshtoken")

	redirect_url = urlparse(request.referrer).path if request.referrer else "/"
	return flask.redirect(redirect_url, code=302)


@app.route('/google/create-fourcat-folder')
def create_fourcat_folder():

	# set up drive client, check if file exists already
	folder_name = config.get("PATH_GDRIVE_ROOT")
	service = build('drive', 'v3', credentials=build_credentials())

	file_id = search_for_existing_folder(service, folder_name)

	# if you don't find one, create it
	if not file_id:
		file_metadata = {
			'name': folder_name,
			'mimeType': 'application/vnd.google-apps.folder'
		}

		new_root_dir = service.files().create(body=file_metadata, fields='id').execute()
		file_id = new_root_dir["id"]

	current_user.set_value("gdrive.folderId", file_id)
	return flask.redirect(flask.session["url_redirect"] if flask.session["url_redirect"] else "/")


def search_for_existing_folder(service, folder_name):
	files = []
	page_token = None

	while True:

		response = service.files().list(
			q="name = '" + folder_name + "' and mimeType = 'application/vnd.google-apps.folder' and trashed=false",
			spaces='drive',
			fields='nextPageToken, '
				   'files(id, name)',
			pageToken=page_token).execute()

		files.extend(response.get('files', []))
		page_token = response.get('nextPageToken', None)

		if page_token is None:
			break

	return files[0]["id"] if len(files) else None
