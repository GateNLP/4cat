"""
Upload specific file to google drive
"""
import time

import googleapiclient
from googleapiclient.http import MediaIoBaseUpload

from backend.abstract.processor import BasicProcessor
from common.lib.subfile import Subfile
from common.lib.user_input import UserInput

__author__ = "Muneerah Patel"
__credits__ = ["Muneerah Patel"]
__maintainer__ = "Muneerah Patel"
__email__ = "m.a.patel+4cat@sheffield.ac.uk"


class UploadToGDrive(BasicProcessor):
	"""
	Processor to upload to google drive
	"""
	type = "upload-to-gdrive"  # job type ID
	category = "Visual" # category
	title = "Upload to Google Drive"  # title displayed in UI
	description = "Upload given file or directory to google drive. If no details are provided, for now, the results" \
				  "file from the main dataset will be uploaded."  # description displayed in UI
	extension = "csv"  # extension of result file, used internally and in UI

	options = {
		"file-path": {
			"type": UserInput.OPTION_TEXT,
			"default": "",
			"help": "File to upload"
		},
		"uploaded-file-name": {
			"type": UserInput.OPTION_TEXT,
			"default": "",
			"help": "Uploaded name"
		},
		"mime-type": {
			"type": UserInput.OPTION_TEXT,
			"default": "",
			"help": "File type"
		}
	}

	@classmethod
	def is_compatible_with(cls, module=None):
		"""
		Determine if processor is compatible with dataset

		:param module: Dataset or processor to determine compatibility with
		"""
		return module.type == "telegram-search"


	def process(self):
		"""
		This takes a file path and a mime type, and uploads the file to google drive under the
		given file name
		"""
		processed = 0
		file_name = self.parameters.get("file-path")
		mime_type = self.parameters.get("mime-type")
		uploaded_filename = self.parameters.get("uploaded-file-name")

		self.dataset.update_status("Uploading file to drive")

		try:
			if not file_name:
				file_name = str(self.dataset.get_parent().get_results_path())
				uploaded_filename = str(self.dataset.get_parent().get_results_path().name)
				mime_type = "application/x-ndjson"

			# upload all subfiles
			credentials = self.dataset.get_owner_drive_credentials()
			drive_api = googleapiclient.discovery.build('drive', 'v3', credentials=credentials).files()

			file_data = open(file_name, 'rb')
			parent_dir = self.dataset.get_parent().get_drive_dir_id()

			body = {'name': uploaded_filename, 'mimeType': mime_type, 'parents': [parent_dir]}
			media_body = MediaIoBaseUpload(file_data, mimetype=mime_type, resumable=True)

			drive_api.create(body=body, media_body=media_body,
							 fields='id,name,mimeType,createdTime,modifiedTime').execute()

			subfile_record = Subfile(db=self.db, key=self.dataset.get_parent().key, file_path=uploaded_filename)
			subfile_record.change_uploaded_date(int(time.time()))

			processed+=1

		except KeyError as e:
			self.dataset.update_status("Cannot find drive. It looks like you might need to log into google")
		except Exception as e:
			self.dataset.update_status(e)


		# done!
		self.dataset.update_status("Finished.")
		self.dataset.finish(num_rows=processed)

