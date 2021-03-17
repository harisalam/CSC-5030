import re
import string
from corpy.udpipe import Model
from corpy.udpipe import pprint
from typing import List
from result import Result
from store import Store
from util import db_config,language_dict
from mysql.connector import errorcode


class Udtrain(object):
    def __init__(self,modelName,corpusName,language):
        self.modelName = modelName
        self.corpusName = corpusName
        self.language = language

        try:
            self.storeData = Store(
                db_config['user'],
                db_config['password'],
                dbHost=db_config['db_host'],
                dbName=db_config['db_name'])

            self.cursor = self.storeData.dbConnect().cursor()
        # self.cursor = conn.cursor()

        except Exception as e:
            print(e)

    def loadData(self):
        try:
            with open(self.corpusName,'r',encoding="utf8") as file:
                return self.cleanData(file.read())
        except Exception as e:
            print('File cant be opened cox of {}'.format(e))

    def cleanData(self,data: str) -> str:
        data = re.sub("[\n\t]","",data)
        return data

    def doTrain(self) -> List[Result]:
        model = Model(self.modelName)
        corpusData = self.loadData()
        word_pos = list(model.process(corpusData))
        line_no = 0
        for index,sentence in enumerate(word_pos):
            singleSentence = self.extractSingleSentence(sentence)
            singleWord = self.extractSingleWord(sentence,singleSentence)
            self.storeData.insertData(self.cursor, singleWord,self.language)
            print('line %d, batch %d for %s written succeed' % (line_no,index,self.language))
        line_no += 1

    def extractSingleSentence(self,sentence) -> str:
        comment = "".join(sentence.comments)
        cs = re.findall(r'text = (.*)',comment)[0]
        return cs

    def extractSingleWord(self,sentence,sentence_text) -> [Result]:
        combinedData = []
        for word in sentence.words:
            if word.lemma not in string.punctuation:
                if word.lemma and word.upostag and sentence_text:
                    combinedData.append(Result(word.lemma,word.upostag,sentence_text))
        return combinedData


if __name__ == "__main__":
    udt = Udtrain(r"C:\Users\haris\Desktop\wordFinder\english-ewt-ud-2.5-191206.udpipe",
                  r"C:\Users\haris\Desktop\wordFinder\haris.txt", "English")
    udt.doTrain()
# storeData = Store()
