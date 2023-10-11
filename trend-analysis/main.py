import findspark

findspark.init()
import pyspark
from pyspark.sql import SparkSession
import sys
import argparse
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, split, explode, window, lower, regexp_replace, to_timestamp, date_format, desc, \
    count, lit, first, sum
from datetime import datetime, timedelta


def get_spark_session():
    """
    This method create a spark session base on execution type.
    :return: Spark Session
    """
    local_execution = True  # This value can ONLY be True in local executions.
    if local_execution:
        spark = (
            SparkSession.builder
            .appName("Trend Analysis")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.driver.maxResultSize", "5g")
            .config("spark.sql.autoBroadcastJoinThreshold", 10485760)
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get('AWS_ACCESS_KEY_ID'))
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('AWS_SECRET_ACCESS_KEY'))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
                    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.version", "3.1.2")
            .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder
            .appName("Trend analysis")
            # .config("hive.metastore.connect.retries", 3)
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
                    "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
            # .config("spark.driver.maxResultSize", "5g")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            # .config("spark.sql.autoBroadcastJoinThreshold", 10485760)
            .enableHiveSupport()
            .getOrCreate()
        )
    return spark


def clean_data(_sc, _bucket_name, _folder_path, _dataset):
    # Read the JSON data from S3
    initialDf = _sc.read.json(f"s3a://{_bucket_name}/{_folder_path}")
    if _dataset == "sample.json":
        current_format = "EEE MMM dd HH:mm:ss Z yyyy"
        initialDf = initialDf.select("created_at", "text") \
            .withColumn("created_at", to_timestamp(col("created_at"), current_format)) \
            .dropna(subset='text').dropna(subset='created_at') \
            .withColumn("text", lower(regexp_replace(col("text"), r'[^a-zA-Z0-9\s]', ''))) \
            .withColumnRenamed("text", "tweet_text") \
            .withColumnRenamed("created_at", "timestamp") \
            .withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd")) \
            .orderBy(col("timestamp").desc())

        initialDf.show()
        return initialDf, datetime(2011, 2, 27)
    else:
        current_format = "EEE, dd MMM yyyy HH:mm:ss Z"
        initialDf = initialDf.select(col("twitter.created_at").alias("created_at"), col("twitter.text").alias("text")) \
            .withColumn("created_at", to_timestamp(col("created_at"), current_format)) \
            .dropna(subset='text').dropna(subset='created_at') \
            .withColumn("text", lower(regexp_replace(col("text"), r'[^a-zA-Z0-9\s]', ''))) \
            .withColumnRenamed("text", "tweet_text") \
            .withColumnRenamed("created_at", "timestamp") \
            .withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd")) \
            .orderBy(col("timestamp").desc())

        initialDf.select("timestamp").distinct().show()
        return initialDf, datetime(2012, 2, 5)


if __name__ == '__main__':
    sc = get_spark_session()
    # parser = argparse.ArgumentParser(description='Trend analysis module')
    # parser.add_argument('--env', required=False, default='dev')
    # parser.add_argument('--user_topic_1', required=True, default=None)
    # parser.add_argument('--user_topic_2', required=True, default=None)
    # args, unknown = parser.parse_known_args()
    print(os.environ.get('ENV'))
    print(os.environ.get('DATASET'))
    print(os.environ.get('USER_TOPIC_1'))
    print(os.environ.get('USER_TOPIC_2'))

    # TODO this should go in the task definition
    actual_date = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    bucket_name = f"trend-analysis-{os.environ.get('ENV')}"
    folder_path = f"input-data/{os.environ.get('DATASET')}"
    dataset_name = os.environ.get('DATASET').split(".")[0]
    output_path = f"s3a://trend-analysis-{os.environ.get('ENV')}/output-data/{dataset_name}/{actual_date}"

    twitter_data, end_date = clean_data(sc, bucket_name, folder_path, os.environ.get('DATASET'))
    # Define the start and end dates for the last week
    start_date = end_date - timedelta(days=7)

    tokenizer = Tokenizer(inputCol="tweet_text", outputCol="words")
    stop_words_en = StopWordsRemover().getStopWords()
    stop_words_es = ["este", "es", "un", "de", "en", "o", "que", "el", "ella", "ellos", "ellas", "y", "e", "u", "ya",
                     "im", "d", "e", "se", "da", " ", "  ", "com", "por", "um", " ", "  "]
    stop_words_combined = stop_words_en + stop_words_es
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words_combined)
    twitter_data = tokenizer.transform(twitter_data)
    twitter_data = remover.transform(twitter_data)

    twitter_data.show()

    # Filtra los datos para el rango de fechas deseado
    filtered_data = twitter_data.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd")) \
        .filter((col("timestamp") >= start_date) & (col("timestamp") <= end_date)) \
        .select("timestamp", explode(col("filtered_words")).alias("word")) \
        .filter((col("word") == os.environ.get('USER_TOPIC_1')) | (col("word") == os.environ.get('USER_TOPIC_2'))) \
        .groupBy("timestamp", "word").agg(count("*").alias("occurrences")) \
        .orderBy(col("occurrences").desc())

    print("filtered_data.show")
    filtered_data.show()

    pivot_df = filtered_data.groupBy("word", date_format("timestamp", "yyyy-MM-dd").alias("date")) \
        .agg(sum("occurrences").alias("occurrences")) \
        .groupBy("word").pivot("date").agg(first("occurrences")).fillna(0)

    pivot_df.show()

    pivot_df.write.mode("overwrite").csv(output_path, header=True)
