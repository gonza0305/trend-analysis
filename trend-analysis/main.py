import findspark

findspark.init()
from pyspark.sql import SparkSession
import os
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, split, explode, window, lower, regexp_replace, to_timestamp, date_format, desc, \
    count, lit, first, sum
from datetime import datetime, timedelta


def get_spark_session():
    """
    This method create a spark session.
    :return: Spark Session
    """
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
    return spark


def clean_data(_sc, _bucket_name, _folder_path, _dataset):
    """
        Cleans and transforms the input JSON data from S3 based on the dataset type.

        Args:
            _sc (SparkSession): The Spark session to read and manipulate data.
            _bucket_name (str): The name of the S3 bucket.
            _folder_path (str): The path to the data folder within the bucket.
            _dataset (str): The name of the dataset being processed.

        Returns:
            DataFrame: The cleaned and transformed DataFrame.
            datetime: The end date associated with the dataset.

        Raises:
            None

        """
    try:
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
            initialDf = initialDf.select(col("twitter.created_at").alias("created_at"),
                                         col("twitter.text").alias("text")) \
                .withColumn("created_at", to_timestamp(col("created_at"), current_format)) \
                .dropna(subset='text').dropna(subset='created_at') \
                .withColumn("text", lower(regexp_replace(col("text"), r'[^a-zA-Z0-9\s]', ''))) \
                .withColumnRenamed("text", "tweet_text") \
                .withColumnRenamed("created_at", "timestamp") \
                .withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd")) \
                .orderBy(col("timestamp").desc())

            initialDf.show()
            return initialDf, datetime(2012, 2, 5)

    except Exception as e:
        print(f"An error occurred in clean_data: {str(e)}")
        return None, None


def preprocess_data(data):
    """
        Preprocesses the input data by tokenizing and removing stop words in multiple languages.

        Args:
            data (DataFrame): The input DataFrame containing text data.

        Returns:
            DataFrame: The preprocessed DataFrame with tokenized and filtered words.

        Raises:
            None

        """
    try:
        tokenizer = Tokenizer(inputCol="tweet_text", outputCol="words")
        stop_words_en = StopWordsRemover().getStopWords()
        stop_words_es = ["este", "es", "un", "de", "en", "o", "que", "el", "ella", "ellos", "ellas", "y", "e", "u",
                         "ya",
                         "im", "d", "e", "se", "da", " ", "  ", "com", "por", "um", " ", "  "]
        stop_words_combined = stop_words_en + stop_words_es
        remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words_combined)
        data_processed = tokenizer.transform(data)
        data_processed = remover.transform(data_processed)
        return data_processed

    except Exception as e:
        print(f"An error occurred in preprocess_data: {str(e)}")
        return None


if __name__ == '__main__':
    sc = get_spark_session()

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

    processed_data = preprocess_data(twitter_data)

    processed_data.show()

    # Filtra los datos para el rango de fechas deseado
    filtered_data = processed_data.withColumn("timestamp", date_format(col("timestamp"), "yyyy-MM-dd")) \
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
