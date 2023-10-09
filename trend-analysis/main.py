import sys
import logging
import argparse
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, split, explode, window, lower, regexp_replace, to_timestamp, date_format, desc, \
    count, lit, first, sum
from datetime import datetime, timedelta


# Init logger
logging.basicConfig(format="%(asctime)s %(levelname)s python: %(message)s", datefmt="%y/%m/%d %H:%M:%S")
logger = logging.getLogger(__name__)
logger.info("Python version: " + sys.version)

try:
    import pyspark
except:
    import findspark

    findspark.init()
    import pyspark

from pyspark.sql import SparkSession


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
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID'))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY'))
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.version", "3.1.2")
            .getOrCreate()
        )
    else:
        spark = (
            SparkSession.builder
            .appName("Trend analysis")
            .config("hive.metastore.connect.retries", 3)
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.driver.maxResultSize", "5g")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.autoBroadcastJoinThreshold", 10485760)
            .enableHiveSupport()
            .getOrCreate()
        )
    return spark


def preprocess_json_data(data):
    # Define a function to convert date/time strings to datetime objects
    def parse_datetime(dt_str):
        try:
            return datetime.strptime(dt_str, "%a %b %d %H:%M:%S %z %Y")
        except ValueError:
            return None

    # Your preprocessing logic goes here
    if "created_at" in data["user"]:
        data["user"]["created_at"] = parse_datetime(data["user"]["created_at"])

    # Handle nullable values
    nullable_fields = ["user.following", "user.follow_request_sent", "in_reply_to_status_id", "in_reply_to_user_id",
                       "contributors", "user.utc_offset", "url", "place", "coordinates"]

    # Nullable fields are handled by setting their values to None if they are missing in the input data.
    for field_path in nullable_fields:
        fields = field_path.split(".")
        current_data = data
        for field in fields[:-1]:
            current_data = current_data.get(field, {})
        current_data[fields[-1]] = current_data.get(fields[-1], None)

    return data


def clean_data(_sc, _bucket_name, _folder_path):
    # Read the JSON data from S3
    date_format = "EEE MMM dd HH:mm:ss Z yyyy"
    # Clean text data (e.g., remove special characters and extra whitespace)
    # Convert 'created_at' column to datetime
    initialDf = _sc.read.json(f"s3a://{_bucket_name}/{_folder_path}") \
        .select("created_at", "text") \
        .dropna(subset='text') \
        .withColumn("created_at", to_timestamp(col("created_at"), date_format)) \
        .withColumn("text", lower(regexp_replace(col("text"), r'[^a-zA-Z0-9\s]', ''))) \
        .withColumnRenamed("text", "tweet_text") \
        .withColumnRenamed("created_at", "timestamp") \
        .orderBy(col("timestamp").desc())

    return initialDf


if __name__ == '__main__':
    sc = get_spark_session()
    parser = argparse.ArgumentParser(description='Trend analysis module')
    parser.add_argument('--env', required=False, default='dev')
    parser.add_argument('--user_topic_1', required=True, default=None)
    parser.add_argument('--user_topic_2', required=True, default=None)
    args, unknown = parser.parse_known_args()
    logger.info("Called with arguments: %s" % args)
    logger.info("Called with unknown arguments: %s" % unknown)
    print("Called with arguments: %s" % args)

    bucket_name = f"trend-analysis-{args.env}"
    folder_path = 'input-data/sample/sample.json'  # TODO this should go in the task definition
    output_path = "s3a://trend-analysis-dev/output-data/"

    twitter_data = clean_data(sc, bucket_name, folder_path)
    # Define the start and end dates for the last week
    end_date = datetime(2011, 2, 27)
    start_date = end_date - timedelta(days=7)
    date_range = [start_date + timedelta(days=i) for i in range(7)]
    date_df = sc.createDataFrame([(date,) for date in date_range], ["date"])\
        .withColumn("date", date_format(col("date"), "yyyy-MM-dd"))

    date_df.show()
    date_columns = date_df.rdd.flatMap(lambda x: x).collect()
    print(" end_date : " + str(end_date))
    print(" start_date : " + str(start_date))

    # Filter tweets from the last week
    # twitter_data = twitter_data.filter((col("timestamp") >= start_date) & (col("timestamp") <= end_date))
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
        .filter((col("word") == args.user_topic_1) | (col("word") == args.user_topic_2)) \
        .groupBy("timestamp", "word").agg(count("*").alias("occurrences")) \
        .orderBy(col("occurrences").desc())

    print("filtered_data.show")
    filtered_data.show()

    pivot_df = filtered_data.groupBy("word", date_format("timestamp", "yyyy-MM-dd").alias("date"))\
        .agg(sum("occurrences").alias("occurrences"))\
        .groupBy("word").pivot("date").agg(first("occurrences")).fillna(0)

    pivot_df.show()

    pivot_df.write.mode("overwrite").csv(output_path, header=True)
