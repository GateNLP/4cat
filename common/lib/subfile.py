from distutils.command.config import config
from pathlib import Path

from common.lib.fourcat_module import FourcatModule


class Subfile():
	"""
	Provide interface to safely register subfiles

	A subfile is uniquely identifiable in relation to a dataset by
	- its key, which is the same as that of the dataset it is linked to
	- the file name
	"""
	key = ""

	db = None
	key = ""
	file_path = ""
	file_type = None
	saved_date = None
	uploaded_date = None
	owner = ""
	is_new = True

	def __init__(self, db=None, key=None, file_path="", file_type=None, saved_date=None, uploaded_date=None,
				 owner="anonymous"):
		"""
		Create new subfile object

		If the dataset is not in the database yet, it is added.

		:param db:  Database connection
		:param key: key of parent dataset
		:param file_path: filepath
		:param file_type: file type
		:param saved_date: date/time the file was saved to disk
		:param uploaded_date: date/time uploaded to drive. null if not unloaded
		"""
		self.db = db
		current = self.db.fetchone("SELECT * FROM subfiles WHERE key = %s AND file_path = %s", (key, file_path))

		self.key = current["key"] if current else key
		self.file_path = current["file_path"] if current else file_path
		self.file_type = current["file_type"] if current else file_type
		self.saved_date = current["saved_date"] if current else saved_date
		self.uploaded_date = current["uploaded_date"] if current else uploaded_date
		self.owner = current["owner"] if current else owner

		if current:
			self.is_new = False

		else:
			data = {
				"key": self.key,
				"file_path": self.file_path,
				"file_type": self.file_type,
				"uploaded_date": self.uploaded_date,
				"saved_date": self.saved_date,
				"owner": self.owner
			}

			self.db.insert("subfiles", data=data)

	def change_uploaded_date(self, uploaded_date):
		"""
		Change the uploaded date of this subfile

		:param int uploaded_date:  new uploaded date
		:return int: the new uploaded date
		"""

		self.uploaded_date = uploaded_date
		self.db.update("subfiles", data={"uploaded_date": uploaded_date}, where={"key": self.key, "file_path": self.file_path})

		return uploaded_date