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
    """
    Executes Task 1 of Data-Intensive Computing. This job runs in two MRStep stages: the first aggregates all
    necessary counts, and the second computes the chi-squared statistic for each term in each review category.
    """
    OUTPUT_PROTOCOL = RawValueProtocol
    stopword_set: set[str] = set()

    def mapper_init(self):
        """
        Initializes the mapper by loading stopwords from file and converting them to lowercase
        for later filtering.
        """
        with open(STOPWORDS_PATH, 'r') as stopword_file:
            self.stopword_set = set()
            for line in stopword_file:
                self.stopword_set.add(line.strip().lower())

        # ensure the stop-word file was actually read
        if not self.stopword_set:
            raise RuntimeError("Stopword list is empty or missing")

    def preprocess(self, text: str):
        """
        Preprocesses review text to extract meaningful tokens. The text is converted to lowercase,
        split according to punctuation, digits, and whitespace, and then any stopwords are removed.

        :param text: the review text to preprocess
        :return: a set of filtered tokens
        """
        text = text.lower()

        # tokenization
        tokenization_pattern = r'[ \t\d\(\)\[\]\{\}\.\!\?\,;\:\+\=\-\_\"\'`~#@&*%€$§/]+'
        token_list = re.split(tokenization_pattern, text)

        # stopword removal + singel-char removal
        token_list = set([
            token for token in token_list
            if len(token) < 1 and token not in self.stopword_set
        ])

        return token_list

    def mapper(self, _, line: str):
        """
        Loads each input record, parses its JSON, and preprocesses the review text. Emits counts for each term
        in its category, total term occurrences across all categories (keyed with '*'), and the review count
        for each category.
        """
        try:
            amazon_dict = json.loads(line)
            category = amazon_dict['category']
            review_text = amazon_dict['reviewText']

            token_list = self.preprocess(review_text)

            # Count term in category
            for token in token_list:
                yield (token, category), 1
                yield (token, '*'), 1

            # Track review count per category
            yield ('REVIEW_COUNT', category), 1

        except Exception:
            self.increment_counter("WARN", "BadJSON", 1)

    def combiner(self, key, counts):
        """
        Aggregates counts locally in each mapper to reduce data transfer before the reducer.
        """
        yield key, sum(counts)

    def reducer_counter(self, key, counts):
        """
        Sums all counts for each key from the combiners and emits them with a None key so they are
        collected by the final reducer.
        """
        yield None, (key, sum(counts))

    def reducer_chisquare(self, _, key_count):
        """
        Takes all aggregated counts and computes the chi-squared statistic for each term/category pair.
        Identifies the top 75 most discriminative terms per category and outputs them, followed by a
        combined list of all top terms across categories.
        """
        N = 0
        category_count = defaultdict(int)
        term_count = defaultdict(int)
        term_category_count = defaultdict(int)

        for key, count in key_count:
            term, cat = key
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

    def steps(self):
        """
        Defines the two-step MapReduce workflow: the first step aggregates counts per term and category,
        and the second step computes chi-squared statistics based on those aggregates.
        """
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_counter),
            MRStep(reducer=self.reducer_chisquare)
        ]


if __name__ == '__main__':
    Task1.run()
