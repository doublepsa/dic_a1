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
    ChiSqSelector,
    Normalizer
)
from pyspark.ml.classification import LinearSVC, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

STOPWORDS_PATH = "./stopwords.txt"
SEED = 11924605

DEV = True

if DEV:
    # dev-dataset
    INPUT = "hdfs:///user/dic25_shared/amazon-reviews/full/reviews_devset.json"
    OUTPUT = "output_ds_dev.txt"
else:
    INPUT = "hdfs:///user/dic25_shared/amazon-reviews/full/reviewscombined.json"
    OUTPUT = "output_ds.txt"


def main():
    spark = SparkSession.builder.appName("Task2_DS_Pipeline").getOrCreate()
    #   spark.sparkContext.setLogLevel("ERROR")
    sc = spark.sparkContext

    # Task 2
    df = spark.read.json(INPUT).select("reviewText", "category")

    regex_tokenizer = RegexTokenizer(
        inputCol="reviewText",
        outputCol="tokens",
        pattern=r"[\s\d\(\)\[\]\{\}\.!\?,;:\+=\-_'\"`~#@&*%€\$§\\/]+",
        toLowercase=True
    )

    # load stopwords
    with open(STOPWORDS_PATH, "r") as stopword_file:
        stopwords = [word.strip().lower() for word in stopword_file if word.strip()]

    # stop_word_remover = StopWordsRemover(
    #     inputCol="tokens",
    #     outputCol="filteredTokens",
    #     stopwords=stopwords
    # )

    stop_word_remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filteredTokens"
    )

    count_vectorizer = CountVectorizer(
        inputCol="filteredTokens",
        outputCol="rawFeatures",
        vocabSize=100000,
        minDF=2
    )

    idf = IDF(
        inputCol="rawFeatures",
        outputCol="features"
    )

    string_indexer = StringIndexer(
        inputCol="category",
        outputCol="label"
    )

    chi_sq_selector = ChiSqSelector(
        numTopFeatures=2000,
        featuresCol="features",
        labelCol="label",
        outputCol="selectedFeatures"
    )

    pipeline = Pipeline(stages=[
        regex_tokenizer,
        stop_word_remover,
        count_vectorizer,
        idf,
        string_indexer,
        chi_sq_selector
    ])
    model = pipeline.fit(df)

    # get the CountVectorizer
    cv_model = model.stages[2]

    # get the ChiSqSelector
    sel_model = model.stages[5]
    vocab = cv_model.vocabulary
    selected_indices = sel_model.selectedFeatures

    # get the selected terms
    selected_terms = sorted([vocab[i] for i in selected_indices])

    # save the file output_ds.txt
    with open(OUTPUT, "w", encoding="utf-8") as f:
        for term in selected_terms:
            f.write(term + "\n")

    # Task 3

    # split into test and train/val
    train_val_df, test_df = df.randomSplit([0.8, 0.2], seed=SEED)

    # L2-Norm normalizer
    normalizer = Normalizer(
        inputCol="selectedFeatures",
        outputCol="normalizedFeatures",
        p=2.0
    )

    base_svc = LinearSVC(
        featuresCol="normalizedFeatures",
        labelCol="label",
        seed=SEED
    )

    # for multiclass calssification we chose the one vs rest strategy
    one_vs_rest = OneVsRest(
        classifier=base_svc,
        featuresCol="normalizedFeatures",
        labelCol="label"
    )

    clf_pipeline = Pipeline(stages=[
        regex_tokenizer,
        stop_word_remover,
        count_vectorizer,
        idf,
        string_indexer,
        chi_sq_selector,
        normalizer,
        one_vs_rest
    ])

    # define the grid for grid search
    param_grid = (ParamGridBuilder()
                  .addGrid(clf_pipeline.stages[5].numTopFeatures, [2000, 500])
                  .addGrid(one_vs_rest.classifier.regParam, [0.01, 0.1, 1.0])
                  .addGrid(one_vs_rest.classifier.standardization, [True, False])
                  .addGrid(one_vs_rest.classifier.maxIter, [50, 100])
                  .build()
                  )

    evaluator = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )

    train_validation_split = TrainValidationSplit(
        estimator=clf_pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        # here the train/val df gets split up into 80% train + 20% val
        trainRatio=0.8,
        seed=SEED,
        parallelism=4
    )

    tvs_model = train_validation_split.fit(train_val_df)

    best_index = tvs_model.bestIndex
    best_f1 = tvs_model.validationMetrics[best_index]
    print(f"Best F1 score = {best_f1}")

    best_params = tvs_model.getEstimatorParamMaps()[best_index]
    print("\nBest hyperparameters:")
    for p, v in best_params.items():
        print(f"{p.name} = {v}")

    # evaluation on the test set
    best_model = tvs_model.bestModel
    test_f1 = evaluator.evaluate(best_model.transform(test_df))
    print(f"\nF1 score on the test set = {test_f1}")

    spark.stop()


if __name__ == "__main__":
    main()
