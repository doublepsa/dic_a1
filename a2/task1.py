#!/usr/bin/env python3
import json
import re
import heapq
import itertools
from collections import defaultdict, OrderedDict
from typing import Generator, Any, Tuple

from pyspark.sql import SparkSession

STOPWORDS_PATH = "./stopwords.txt"
INPUT = "hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json" # devset
OUTPUT    = "output_rdd.txt"

def load_stopwords(path: str) -> set[str]:
    """
    Loading stopwords from file and converting them to lowercase
    for later filtering.

    :return: Set of the stopwords
    """
    with open(path, 'r') as stopword_file:
        stopword_set = set()
        for line in stopword_file:
            stopword_set.add(line.strip().lower())

    # assert the stop-word file was actually read
    if not stopword_set:
        raise RuntimeError("Stopword list is empty or missing")
    return stopword_set

def preprocess(text: str, stopwords: set[str]) -> set[str]:
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
        if token not in stopwords
    ])

    return token_list


def extract_term_counts(
        record: Any,
        stopwords) -> Generator[Tuple[Tuple[str,str], int], None, None]:

    """
    For each review, yield the three kinds of count events we need:
      - (term, category)
      - (term, '*')
      - ('REVIEW_COUNT', category)
    """
    tokens = preprocess(record['reviewText'], stopwords.value)
    category = record['category']

    # Count term in category
    for term in tokens:
        yield (term, category), 1
        yield (term, '*'), 1

    # Track review count per category
    yield ('REVIEW_COUNT', category), 1


def main():
    spark = SparkSession.builder.appName("Task1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # broadcast stopwords, so every executor has the list
    stopwords = sc.broadcast(load_stopwords(STOPWORDS_PATH))

    # reading in the json lines and converting do python dict
    reviews = sc.textFile(INPUT).map(json.loads)

    # create key value pairs
    pairs = reviews.flatMap(lambda rec: extract_term_counts(rec, stopwords))

    # sum up the counts
    counts = pairs.reduceByKey(lambda x, y: x + y)
    all_counts = counts.collect()

    # Begin calculating the chi-square values
    N = 0
    category_count = defaultdict(int)
    term_count = defaultdict(int)
    term_category_count = defaultdict(int)

    for (term, cat), cnt in all_counts:
        if term == 'REVIEW_COUNT':
            category_count[cat] += cnt
            N += cnt
        elif cat == '*':
            term_count[term] = cnt
        else:
            term_category_count[(term, cat)] = cnt
    
    # calculating chi2 of all terms for each category
    chi2_per_cat = defaultdict(dict)
    for (term, cat), A in term_category_count.items():
        B = term_count[term] - A
        C = category_count[cat] - A
        D = N - (A + B + C)
        denom = (A + B) * (A + C) * (B + D) * (C + D)
        if denom <= 0:
            continue
        chi2 = (N * (A*D - B*C)**2) / denom
        chi2_per_cat[cat][term] = chi2

    # the top 75 most discriminative terms for the category according to the chi-square test in descending order
    top75_per_cat = OrderedDict()
    for cat in sorted(chi2_per_cat):
        top = heapq.nlargest(75, chi2_per_cat[cat].items(), key=lambda kv: kv[1])
        if top:
            top75_per_cat[cat] = top

    # output for each product category with top 75 most discriminative terms
    out_lines = []
    for cat, terms in top75_per_cat.items():
        terms_line = f"{cat} " + " ".join(f"{term}:{chi2_per_cat[cat][term]}" for term, _ in terms)
        out_lines.append(terms_line)
        
    # combined global list (just the sorted terms)
    all_terms = sorted(
        itertools.chain.from_iterable(
            [ [t for t,_ in terms] for _, terms in top75_per_cat.items() ]
        )
    )
    out_lines.append("GLOBAL " + " ".join(all_terms))

    sc.parallelize(out_lines, 1).saveAsTextFile(OUTPUT)

    spark.stop()

if __name__ == "__main__":
    main()
