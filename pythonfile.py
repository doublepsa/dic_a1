from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

# Load stopwords from the file once
with open('stopwords.txt', 'r') as f:
    STOPWORDS = set(word.strip().lower() for word in f.readlines())

# Tokenization using the specified delimiters
TOKEN_RE = re.compile(r"[()\[\]{}.!?,;:+=\-_\"'`~#@&*%€$§\\/0-9\s]+")

class MRPreprocessAndCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

    def mapper(self, _, line):
        try:
            data = json.loads(line)
            category = data.get('category')
            text = data.get('reviewText', '')

            if not category or not text:
                return

            # Tokenization + cleaning
            tokens = set([
                token.lower() for token in TOKEN_RE.split(text)
                if token and token.lower() not in STOPWORDS and len(token) > 1
            ])

            # Count term in category
            for token in tokens:
                yield ((token, category), 1)
                yield ((token, '*'), 1)  # global DF

            # Track document count per category
            yield (('DOC_COUNT', category), 1)

            # Track total document count
            yield (('TOTAL_DOCS', '*'), 1)

        except:
            self.increment_counter("WARN", "BadJSON", 1)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRPreprocessAndCount.run()

