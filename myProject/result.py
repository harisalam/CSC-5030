class Result(object):
	def __init__(self, word: str, posTag: str, sentence: str):
		self.word = word
		self.posTag = posTag
		self.sentence = sentence
		# print(word, posTag, sentence)