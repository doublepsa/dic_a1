from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol

from collections import defaultdict, OrderedDict
import heapq
import itertools
import json
import re
from typing import Iterable, Dict, Set, Generator

STOPWORDS_PATH = './stopwords.txt'


class Task1(MRJob):
    """
    Executes Task 1 of Data-Intensive Computing. This job runs in two MRStep stages: the first aggregates all
    necessary counts, and the second computes the chi-squared statistic for each term in each review category.
    """
    OUTPUT_PROTOCOL = RawValueProtocol
    stopword_set: Set[str]

    def mapper_init(self) -> None:
        """
        Initializes the mapper by loading stopwords from file and converting them to lowercase
        for later filtering.

        :return: None
        """
        with open(STOPWORDS_PATH, 'r') as stopword_file:
            self.stopword_set = set()
            for line in stopword_file:
                self.stopword_set.add(line.strip().lower())

        # ensure the stop-word file was actually read
        if not self.stopword_set:
            raise RuntimeError("Stopword list is empty or missing")

    def preprocess(self, text: str) -> set[str]:
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

        # stopword removal
        token_list = set([
            token for token in token_list
            if token not in self.stopword_set
        ])

        return token_list

    def mapper(self, _: str, line: str) -> Generator[tuple[tuple[str, str], int], None, None]:
        """
        Loads each input record, parses its JSON, and preprocesses the review text. Emits counts for each term
        in its category, total term occurrences across all categories (marked with '*'), and the review count
        for each category.

        :param _: the unused input key
        :param line: one line of JSON from the Amazon review dataset
        :return: ((term, category), 1) pairs
        """
        amazon_dict = json.loads(line)
        category: str = amazon_dict['category']
        review_text: str = amazon_dict['reviewText']

        token_list = self.preprocess(review_text)

        # Count term in category
        for token in token_list:
            yield (token, category), 1
            yield (token, '*'), 1

        # Track review count per category
        yield ('REVIEW_COUNT', category), 1

    def combiner(self, key: tuple[str, str], counts: Iterable[int]) -> Generator[
        tuple[tuple[str, str], int], None, None]:
        """
        Aggregates counts locally in each mapper to reduce data transfer before the reducer.

        :param key: either (term, category) or ('REVIEW_COUNT', category) or (term, '*')
        :param counts: stream of counts to be summed
        :return: the aggregated count for this key
        """
        yield key, sum(counts)

    def reducer_counter(self, key: tuple[str, str], counts: Iterable[int]) -> Generator[
        tuple[None, tuple[tuple[str, str] | str, int]], None, None]:
        """
        Sums all counts for each key from the combiners and emits them with a None key so they are
        collected by the final reducer.

        :param key: same key emitted by the combiner
        :param counts: aggregated counts for this key
        :return: (None, (key, total_count)) for single reducer in next step
        """
        yield None, (key, sum(counts))

    def reducer_chisquare(
            self,
            _: None,
            key_count: Iterable[tuple[tuple[str, str], int]]
    ) -> Iterable[tuple[None, str]]:
        """
        Takes all aggregated counts and computes the chi-squared statistic for each term/category pair.
        Identifies the top 75 most discriminative terms per category and outputs them, followed by a
        combined list of all top terms across categories.

        :param _: always None from previous reducer
        :param key_count: iterator of ((term, category), count) tuples
        :return: strings representing category-specific and global top-term lists
        """
        N = 0
        category_count: Dict[str, int] = defaultdict(int)
        term_count: Dict[str, int] = defaultdict(int)
        term_category_count: Dict[tuple[str, str], int] = defaultdict(int)

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
        chi_square_cat_term: Dict[str, Dict[str, float]] = {}
        for term, cat in term_category_count:
            # how many times term appears in category
            A = term_category_count[(term, cat)]
            # how many times does term appear in other categories
            B = term_count[term] - A
            # all the terms in the category that are not term
            C = category_count[cat] - A
            # all the reviews not in category without term
            D = N - (A + B + C)

            denominator = (A + B) * (A + C) * (B + D) * (C + D)
            if denominator == 0:
                continue

            chi_square = (N * (A * D - B * C) ** 2) / denominator
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
            yield None, f"{cat} " + " ".join(f"{term}:{chi_square}" for term, chi_square in terms.items())

        # output for all top 75 most discriminative terms in each category
        yield None, " ".join(sorted(list(itertools.chain.from_iterable(chi_square_cat_term.values()))))

    def steps(self):
        """
        Defines the two-step MapReduce workflow: the first step aggregates counts per term and category,
        and the second step computes chi-squared statistics based on those aggregates.

        :return: a list containing the two MRSteps composing the job
        """
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer_counter
            ),
            MRStep(reducer=self.reducer_chisquare)
        ]


if __name__ == '__main__':
    Task1.run()
