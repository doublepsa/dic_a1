#!/usr/bin/env python3
import json
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    CountVectorizer,
    IDF,
    StringIndexer,
    ChiSqSelector
)

STOPWORDS_PATH = "./stopwords.txt"
DEV_INPUT       = "hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json"
OUTPUT          = "output_ds.txt"


def main():
    spark = SparkSession.builder.appName("Task2_DS_Pipeline").getOrCreate()
#   spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    df = spark.read.json(DEV_INPUT).select("reviewText", "category")

    regex_tokenizer = RegexTokenizer(
        inputCol="reviewText",
        outputCol="tokens",
        pattern=r"[\s\d\(\)\[\]\{\}\.!\?,;:\+=\-_'\"`~#@&*%€\$§\\/]+",
        toLowercase=True
    )

    # todo
    stop_remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens"
    )

    count_vec = CountVectorizer(
        inputCol="filtered_tokens",
        outputCol="rawFeatures",
        vocabSize=100000,
        minDF=2
    )

    idf = IDF(
        inputCol="rawFeatures",
        outputCol="features"
    )

    indexer = StringIndexer(
        inputCol="category",
        outputCol="label"
    )

    selector = ChiSqSelector(
        numTopFeatures=2000,
        featuresCol="features",
        labelCol="label",
        outputCol="selectedFeatures"
    )

    pipeline = Pipeline(stages=[
        regex_tokenizer,
        stop_remover,
        count_vec,
        idf,
        indexer,
        selector
    ])
    model = pipeline.fit(df)

    cv_model = model.stages[2]       
    sel_model = model.stages[5]      
    vocab = cv_model.vocabulary      
    selected_indices = sel_model.selectedFeatures  

    selected_terms = sorted([vocab[i] for i in selected_indices])

    sc.parallelize(selected_terms, 1).saveAsTextFile(OUTPUT)

    spark.stop()


if __name__ == "__main__":
    main()
