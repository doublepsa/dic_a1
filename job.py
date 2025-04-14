from mrjob.job import MRJob
from mrjob.step import MRStep

import re

STOPWORDS_PATH = './stopwords.txt'


class Task1(MRJob):
    stopword_set: set[str] = set()

    def init(self):
        # load stopwords
        with open(STOPWORDS_PATH, 'r') as stopword_file:
            self.stopword_set = set()
            for line in stopword_file:
                self.stopword_set.add(line.strip().lower())

    def preprocess(self, text: str):
        # case folding
        text = text.lower()

        # tokenization
        tokenization_patter = r'[ \t\d\(\)\[\]\{\}\.\!\?\,;\:\+\=\-\_"\'`~#@&\*\%€\$§\\/]+'
        token_list = re.split(tokenization_patter, text)

        # stopword removal
        token_list = [token for token in token_list if token and token not in self.stopword_set]

        return token_list

    def mapper(self, _, line):
        # TODO
        pass


    def reducer_count(self, word, counts):
        # TODO
        pass

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_count)
        ]


if __name__ == '__main__':
    Task1.run()
