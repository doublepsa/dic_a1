from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

from collections import defaultdict, OrderedDict
import heapq
import itertools
import json
import re

STOPWORDS_PATH = './stopwords.txt'


class Task1(MRJob):
    OUTPUT_PROTOCOL = RawValueProtocol
    stopword_set: set[str] = set()

    def mapper_init(self):
        # load stopwords
        with open(STOPWORDS_PATH, 'r') as stopword_file:
            self.stopword_set = set()
            for line in stopword_file:
                self.stopword_set.add(line.strip().lower())

    def preprocess(self, text: str):
        # case folding
        text = text.lower()

        # tokenization
        tokenization_pattern = r'[ \t\d\(\)\[\]\{\}\.\!\?\,;\:\+\=\-\_\"\'`~#@&*%€$§/]+'
        token_list = re.split(tokenization_pattern, text)

        # stopword removal
        token_list = set([token for token in token_list if token and token not in self.stopword_set])

        return token_list

    def mapper(self, _, line: str):
        try:
            amazon_dict = json.loads(line)
            category = amazon_dict['category']
            review_text = amazon_dict['reviewText']

            token_list = self.preprocess(review_text)

            # Count term in category
            for token in token_list:
                yield (token, category), 1
                yield (token, '*'), 1  # total term occurrence

            # Track review count per category
            yield ('REVIEW_COUNT', category), 1

        except Exception:
            self.increment_counter("WARN", "BadJSON", 1)

    # optimisation
    def combiner(self, key, counts):
        # sum the keys we've seen so far
        yield key, sum(counts)

    # send all (key,count) pairs to the same reducer.
    def reducer_counter(self, key, counts):
        # sum all the results for each key
        yield None, (key, sum(counts))

    # Since all input to this step has the same key (None), a single reducer task will get all rows
    def reducer_chisquare(self, _, key_count):
        N = 0
        category_count = defaultdict(int)
        term_count = defaultdict(int)
        term_category_count = defaultdict(int)

        for key, count in key_count:
            term, cat = key
            # if term == 'TOTAL_REVIEWS':
            #     N = count
            if term == 'REVIEW_COUNT':
                N += count
                category_count[cat] += count
            elif cat == '*':
                term_count[term] = count
            else:
                term_category_count[(term, cat)] = count

        # 1. calculate chi2 of all terms for each category
        chi_square_cat_term = {}
        for term, cat in term_category_count:
            # how many times term appears in category
            A = term_category_count[(term, cat)]
            # how many times does term appear in other categories
            B = term_count[term] - A
            # all the terms in the category that are not term
            C = category_count[cat] - A
            # all the reviews not in category without term
            D = N - (A + B + C)
            chi_square = (N * (A * D - B * C) ** 2) / ((A + B) * (A + C) * (B + D) * (C + D))
            if cat not in chi_square_cat_term:
                chi_square_cat_term[cat] = {}
            chi_square_cat_term[cat][term] = chi_square
        # sort categories alphabetically
        chi_square_cat_term = OrderedDict(sorted(chi_square_cat_term.items()))

        # the top 75 most discriminative terms for the category according to the chi-square test in descending order
        for cat, terms in chi_square_cat_term.items():
            chi_square_cat_term[cat] = dict(heapq.nlargest(75, terms.items(), key=lambda k: k[1]))
            if not chi_square_cat_term[cat]:
                del chi_square_cat_term[cat]

        # output for each product category with top 75 most discriminative terms
        for cat, terms in chi_square_cat_term.items():
            yield None, str(cat) + " " + " ".join(f"{term}:{chi_square}" for term, chi_square in terms.items())
        # output for all top 75 most discriminative terms in each category
        yield None, " ".join(sorted(list(itertools.chain.from_iterable(chi_square_cat_term.values()))))

    # Multi-step jobs as we need to aggregate counts for keys first before we calculate chi_square values
    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_counter),
            MRStep(reducer=self.reducer_chisquare)
        ]


if __name__ == '__main__':
    Task1.run()
