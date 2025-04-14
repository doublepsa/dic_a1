from mrjob.job import MRJob
from mrjob.step import MRStep

import re
import json

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

    def mapper(self, _, line: str):
        amazon_dict = json.loads(line)
        category = amazon_dict['category']
        review_text = amazon_dict['reviewText']

        token_list = self.preprocess(review_text)

        for token in token_list:
            yield (category, token), 1

    def combiner(self, key: tuple[str, str], counts: list[int]):
        yield key, sum(counts)

    def reducer(self, _, value):
        # TODO
        pass

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer
            )
        ]


if __name__ == '__main__':
    Task1.run()
