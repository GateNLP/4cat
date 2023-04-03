"""
Download images from Telegram message attachments
"""
import asyncio
import hashlib
import json

import googleapiclient

from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from telethon import TelegramClient, utils, types
from zipfile import ZipFile

import common.config_manager as config
from backend.abstract.processor import BasicProcessor
from common.lib.exceptions import ProcessorInterruptedException
from common.lib.helpers import UserInput
from common.lib.dataset import DataSet

__author__ = "Stijn Peeters"
__credits__ = ["Stijn Peeters"]
__maintainer__ = "Stijn Peeters"
__email__ = "4cat@oilab.eu"


class TelegramImageDownloader(BasicProcessor):
    """
    Telegram image downloader

    Downloads attached images from Telegram messages and saves as zip archive
    """
    type = "image-downloader-telegram"  # job type ID
    category = "Visual"  # category
    title = "Download Telegram images"  # title displayed in UI
    description = "Download images and store in a zip file. Downloads through the Telegram API might take a while. " \
                  "Note that not always all images can be retrieved. A JSON metadata file is included in the output " \
                  "archive."  # description displayed in UI
    extension = "zip"  # extension of result file, used internally and in UI
    flawless = True

    config = {
        'image-downloader-telegram.MAX_NUMBER_IMAGES': {
            'type': UserInput.OPTION_TEXT,
            'default' : "1000",
            'help': 'Max images',
            'tooltip': "Maxmimum number of Telegram images a user can download.",
            },
        }

    @classmethod
    def get_options(cls, parent_dataset=None, user=None):
        """
        Get processor options

        Give the user the choice of where to upload the dataset, if multiple
        TCAT servers are configured. Otherwise, no options are given since
        there is nothing to choose.

        :param DataSet parent_dataset:  Dataset that will be uploaded
        :param User user:  User that will be uploading it
        :return dict:  Option definition
        """
        max_number_images = int(config.get('image-downloader-telegram.MAX_NUMBER_IMAGES', 1000))

        return {
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "help": "No. of images (max %s)" % max_number_images,
                "default": 100,
                "min": 0,
                "max": max_number_images
            },
            "video-thumbnails": {
                "type": UserInput.OPTION_TOGGLE,
                "help": "Include videos (as thumbnails)",
                "default": False
            },
            "upload-to-drive": {
                "type": UserInput.OPTION_TOGGLE,
                "help": "Upload to drive (ensure you are logged into drive via option above)",
                "default": True
            }
        }


    @classmethod
    def is_compatible_with(cls, module=None):
        """
        Allow processor on Telegram datasets with required info

        :param module: Dataset or processor to determine compatibility with
        """
        if type(module) is DataSet:
            # we need these to actually instantiate a telegram client and
            # download the images
            return module.type == "telegram-search" and \
                   "api_phone" in module.parameters and \
                   "api_id" in module.parameters and \
                   "api_hash" in module.parameters
        else:
            return module.type == "telegram-search"

    def process(self):
        """
        Prepare and asynchronously call method to download images
        """
        self.staging_area = self.dataset.get_staging_area()
        self.eventloop = None
        self.metadata = {}

        asyncio.run(self.get_images())

        # finish up
        with self.staging_area.joinpath(".metadata.json").open("w", encoding="utf-8") as outfile:
            json.dump(self.metadata, outfile)

        self.dataset.update_status("Compressing images")
        self.write_archive_and_finish(self.staging_area)

    async def get_images(self):
        """
        Get images for messages

        Separate method because this needs to be run asynchronously. Looks for
        messages in the dataset with photo attachments, then loads those
        messages from the client, then downloads the attachments of those
        messages and saves them as .jpeg files.
        """
        # prepare telegram client parameters
        query = self.source_dataset.top_parent().parameters
        hash_base = query["api_phone"].replace("+", "") + query["api_id"] + query["api_hash"]
        session_id = hashlib.blake2b(hash_base.encode("ascii")).hexdigest()
        session_code = query.get("session-code", None)
        session_id = session_id if not session_code else session_id + session_code

        session_path = Path(config.get('PATH_ROOT')).joinpath(config.get('PATH_SESSIONS'), session_id + ".session")
        amount = self.parameters.get("amount")
        with_thumbnails = self.parameters.get("video-thumbnails")
        upload_to_drive = self.parameters.get("upload-to-drive")
        client = None
        drive_client = None

        # we need a session file, otherwise we can't retrieve the necessary data
        if not session_path.exists():
            self.dataset.update_status("Telegram session file missing. Cannot download images.", is_final=True)
            return []

        if upload_to_drive:
            try:
                credentials = self.dataset.get_owner_drive_credentials()
                drive_client = build('drive', 'v3', credentials=credentials).files()
            except Exception:
                upload_to_drive = False
                self.dataset.update_status("Could not validate with google. Nothing will be uploaded to google drive.")

        # instantiate client
        try:
            client = TelegramClient(str(session_path), int(query.get("api_id")), query.get("api_hash"),
                                    loop=self.eventloop)
            await client.start(phone=TelegramImageDownloader.cancel_start)
        except RuntimeError:
            # session is no longer usable
            self.dataset.update_status(
                "Session is not authenticated: login security code may have expired. You need to  create a new "
                "dataset to download images from and re-enter the security code", is_final=True)

        # figure out which messages from the dataset we need to download media
        # for. Right now, that's everything with a non-empty `photo` attachment
        # or `video` if we're also including thumbnails
        messages_with_photos = {}
        downloadable_types = ("photo",) if not with_thumbnails else ("photo", "video")
        total_media = 0
        self.dataset.update_status("Finding messages with image attachments")
        for message in self.source_dataset.iterate_items(self):
            if self.interrupted:
                raise ProcessorInterruptedException("Interrupted while processing messages")

            if not message.get("attachment_data") or message.get("attachment_type") not in downloadable_types:
                continue

            # probably should make this backward compatible
            key_to_use = "thread_num_id" if "thread_num_id" in message.keys() else message["chat"]

            if message[key_to_use] not in messages_with_photos:
                messages_with_photos[message[key_to_use]] = []

            messages_with_photos[message[key_to_use]].append(int(message["id"]))
            total_media += 1

            if amount and total_media >= amount:
                break

        # don't forget the subfiles!
        if self.dataset.get_subfile_paths():
            for path in self.source_dataset.get_subfile_paths():
                for message in self.source_dataset.iterate_items(subfile=path):
                    if self.interrupted:
                        raise ProcessorInterruptedException("Interrupted while processing messages")

                    if not message.get("attachment_data") or message.get("attachment_type") not in downloadable_types:
                        continue

                    # probably should make this backward compatible
                    key_to_use = "thread_num_id" if "thread_num_id" in message.keys() else message["chat"]

                    if message[key_to_use] not in messages_with_photos:
                        messages_with_photos[message[key_to_use]] = []

                    messages_with_photos[message[key_to_use]].append(int(message["id"]))
                    total_media += 1

                    if amount and total_media >= amount:
                        break


        # now actually download the images
        # todo: investigate if we can directly instantiate a MessageMediaPhoto instead of fetching messages
        media_done = 1
        zip_file_count = 1

        for entity, message_ids in messages_with_photos.items():
            try:
                channel_to_add = utils.get_peer_id(types.PeerChannel(entity))
                self.dataset.log("resolved entity to get images from is is %s" % (str(channel_to_add)))

                async for message in client.iter_messages(entity=channel_to_add, ids=message_ids):
                    if self.interrupted:
                        raise ProcessorInterruptedException("Interrupted while downloading images")

                    success = False
                    try:
                        if upload_to_drive and (media_done - 1) and (media_done - 1) % 100 == 0:
                            self.save_to_gdrive(drive_client, zip_file_count)
                            zip_file_count += 1

                        # it's actually unclear if images are always jpegs, but this
                        # seems to work
                        self.dataset.update_status("Downloading media %i/%i" % (media_done, total_media))
                        self.dataset.update_progress(media_done / total_media)

                        path = self.staging_area.joinpath("%s-%i.jpeg" % (entity, message.id))
                        filename = path.name
                        if hasattr(message.media, "photo"):
                            await message.download_media(str(path))
                        else:
                            # video thumbnail
                            await client.download_media(message, str(path), thumb=-1)
                        msg_id = message.id
                        success = True
                    except (AttributeError, RuntimeError, ValueError, TypeError) as e:
                        filename = "%s-index-%i" % (entity, media_done)
                        msg_id = str(message.id) if hasattr(message, "id") else "with index %i" % media_done
                        self.dataset.log("Could not download image for message %s (%s)" % (msg_id, str(e)))
                        self.flawless = False

                    media_done += 1
                    self.metadata[filename] = {
                        "filename": filename,
                        "success": success,
                        "from_dataset": self.source_dataset.key,
                        "post_ids": [msg_id]
                    }
                    
            except ValueError as e:
                self.dataset.log("Couldn't retrieve images for %s, it probably does not exist anymore (%s)" % (entity, str(e)))
                self.flawless = False

        if upload_to_drive:
            self.save_to_gdrive(drive_client, zip_file_count)

        client.disconnect()

    @staticmethod
    def cancel_start():
        """
        Replace interactive phone number input in Telethon

        By default, if Telethon cannot use the given session file to
        authenticate, it will interactively prompt the user for a phone
        number on the command line. That is not useful here, so instead
        raise a RuntimeError. This will be caught and the user will be
        told they need to re-authenticate via 4CAT.
        """
        raise RuntimeError("Connection cancelled")

    def create_zip_file(self, zip_filename):
        """
		Create zip file with all images from current images staging area

		:param zip_file_count: num of zip files already created + 1, used for file name
		"""
        downloaded_image_files = self.staging_area.glob("*")

        zip_obj = ZipFile(zip_filename, 'w')

        for img_file in downloaded_image_files:
            zip_obj.write(img_file, img_file.name)

        zip_obj.close()

    def save_to_gdrive(self, drive_client, zip_file_count):
        """
		Create zip file with all images from current images staging area

		:param drive_client: google drive client object
		:param zip_file_count: num of zip files already created + 1, used for file name
		"""

        self.dataset.update_status("Saving collected images to google drive")

        try:
            zip_postfix = str(self.dataset.get_parent().get_results_path().stem)
            zip_filename = "images-" + str(zip_file_count) + "-" + zip_postfix + ".zip"
            self.create_zip_file(zip_filename);
            zip_to_upload = open(zip_filename, 'rb')

            # get the directory in which we want to upload this zip
            parent_dir = self.dataset.get_parent().get_drive_dir_id()

            # package up and upload folder
            body = {'name': zip_to_upload.name, 'mimeType': "application/zip", 'parents': [parent_dir]}
            media_body = MediaIoBaseUpload(zip_to_upload, mimetype="application/zip", resumable=True)

            drive_client.create(body=body, media_body=media_body,
                                fields='id,name,mimeType,createdTime,modifiedTime').execute()

            # delete all the images in the staging area
            # only do once files are actually uploaded, in case of error!
            downloaded_image_files = self.staging_area.glob("*")
            for img_file in downloaded_image_files:
                img_file.unlink()

            self.dataset.update_status("Finished saving collected images to drive")

        except Exception as e:
            self.dataset.update_status("Failed to write zip file %i to google drive" % zip_file_count)
            self.dataset.update_status("Error is %s" % str(e))
