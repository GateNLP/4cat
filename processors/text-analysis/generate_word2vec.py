"""
Generate interval-based Word2Vec models for sentences
"""
import shutil
import pickle
import json

from gensim.models import Word2Vec, Phrases
from pathlib import Path

from backend.lib.helpers import UserInput, convert_to_int
from backend.abstract.processor import BasicProcessor
from backend.lib.exceptions import ProcessorInterruptedException

__author__ = "Sal Hagen"
__credits__ = ["Sal Hagen", "Stijn Peeters", "Tom Willaert"]
__maintainer__ = "Sal Hagen"
__email__ = "4cat@oilab.eu"


class GenerateWord2Vec(BasicProcessor):
	"""
	Generate Word2Vec models
	"""
	type = "generate-word2vec"  # job type ID
	category = "Text analysis"  # category
	title = "Generate Word2Vec models"  # title displayed in UI
	description = "Generates Word2Vec word embedding models for the sentences, per chosen time interval. These can then be used to analyse semantic word associations within the corpus. Note that good models require large(r) datasets."  # description displayed in UI
	extension = "zip"  # extension of result file, used internally and in UI

	accepts = ["tokenise-posts"]

	input = "zip"
	output = "zip"

	references = [
		"[Mikolov, Tomas, Ilya Sutskever, Kai Chen, Greg Corrado, and Jeffrey Dean. 2013. “Distributed Representations of Words and Phrases and Their Compositionality.” Advances in Neural Information Processing Systems, 2013: 3111-3119.](https://papers.nips.cc/paper/5021-distributed-representations-of-words-and-phrases-and-their-compositionality.pdf)",
		"[Mikolov, Tomas, Kai Chen, Greg Corrado, and Jeffrey Dean. 2013. “Efficient Estimation of Word Representations in Vector Space.” ICLR Workshop Papers, 2013: 1-12.](https://arxiv.org/pdf/1301.3781.pdf)",
		"[word2vec - Google Code](https://code.google.com/archive/p/word2vec/)",
		"[word2vec - Gensim documentation](https://radimrehurek.com/gensim/models/word2vec.html)",
		"[A Beginner's Guide to Word Embedding with Gensim Word2Vec Model - Towards Data Science](https://towardsdatascience.com/a-beginners-guide-to-word-embedding-with-gensim-word2vec-model-5970fa56cc92)"
	]

	options = {
		"algorithm": {
			"type": UserInput.OPTION_CHOICE,
			"default": "cbow",
			"options": {
				"cbow": "Continuous Bag of Words (CBOW)",
				"skipgram": "Skip-gram"
			},
			"help": "Training algorithm",
			"tooltip": "See processor references for a more detailed explanation."
		},
		"window": {
			"type": UserInput.OPTION_CHOICE,
			"default": "5",
			"options": {"3": 3, "4": 4, "5": 5, "6": 6, "7": 7},
			"help": "Window",
			"tooltip": "Maximum distance between the current and predicted word within a sentence"
		},
		"dimensionality": {
			"type": UserInput.OPTION_TEXT,
			"default": 100,
			"min": 50,
			"max": 1000,
			"help": "Dimensionality of the word vectors"
		},
		"negative": {
			"type": UserInput.OPTION_TOGGLE,
			"default": True,
			"help": "Use negative sampling"
		},
		"min_count": {
			"type": UserInput.OPTION_TEXT,
			"default": 5,
			"help": "Minimum word occurrence",
			"tooltip": "How often a word should occur in the corpus to be included"
		}
	}

	def process(self):
		"""
		This takes a 4CAT results file as input, and outputs a number of files containing
		tokenised posts, grouped per time unit as specified in the parameters.
		"""
		self.dataset.update_status("Processing sentences")

		use_skipgram = 1 if self.parameters.get("algorithm") == "skipgram" else 0
		window = min(10, max(1, convert_to_int(self.parameters.get("window"), self.options["window"]["default"])))
		use_negative = 5 if self.parameters.get("negative") else 0
		min_count = max(1, convert_to_int(self.parameters.get("min_count"), self.options["min_count"]["default"]))
		dimensionality = convert_to_int(self.parameters.get("dimensionality"), 100)

		staging_area = self.dataset.get_staging_area()

		# go through all archived token sets and vectorise them
		models = 0
		for temp_file in self.iterate_archive_contents(self.source_file):
			# use the "list of lists" as input for the word2vec model
			# by default the tokeniser generates one list of tokens per
			# post... which may actually be preferable for short
			# 4chan-style posts. But alternatively it could generate one
			# list per sentence - this processor is agnostic in that regard
			token_set_name = temp_file.name
			self.dataset.update_status("Extracting common phrases from token set %s..." % token_set_name)
			bigram_transformer = Phrases(self.tokens_from_file(temp_file, staging_area))

			self.dataset.update_status("Training Word2vec model for token set %s..." % token_set_name)
			try:
				model = Word2Vec(bigram_transformer[self.tokens_from_file(temp_file, staging_area)], negative=use_negative, size=dimensionality, sg=use_skipgram, window=window, workers=3, min_count=min_count)
			except RuntimeError as e:
				if "you must first build vocabulary before training the model" in str(e):
					# not enough data. Skip - if this happens for all models
					# an error will be generated later
					continue
				else:
					raise e

			# save - we only save the KeyedVectors for the model, this
			# saves space and we don't need to re-train the model later
			model_name = token_set_name.split(".")[0] + ".model"
			model.wv.save(str(staging_area.joinpath(model_name)))

			# save vocabulary too, some processors need it
			del model
			models += 1

		if models == 0:
			self.dataset.update_status("Not enough data in source file to train Word2Vec models.")
			shutil.rmtree(staging_area)
			self.dataset.finish(0)

		# create another archive with all model files in it
		self.write_archive_and_finish(staging_area)

	def tokens_from_file(self, file, staging_area):
		"""
		Read tokens from token dump

		If the tokens were saved as JSON, take advantage of this and return
		them as a generator, reducing memory usage and allowing interruption.

		:param Path file:
		:param Path staging_area:  Path to staging area, so it can be cleaned
		up when the processor is interrupted
		:return list:  A set of tokens
		"""
		if file.suffix == "pb":
			with file.open("rb") as input:
				return pickle.load(input)

		with file.open("r") as input:
			input.seek(1)
			while True:
				line = input.readline()
				if line is None:
					break

				if self.interrupted:
					shutil.rmtree(staging_area)
					raise ProcessorInterruptedException("Interrupted while reading tokens")

				if line == "]":
					# this marks the end of the file
					raise StopIteration

				try:
					# the tokeniser dumps the json with one set of tokens per
					# line, ending with a comma
					token_set = json.loads(line.strip()[:-1])
					yield token_set
				except json.JSONDecodeError:
					# old-format json dumps are not suitable for the generator
					# approach
					input.seek(0)
					return json.load(input)