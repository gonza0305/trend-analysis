import os
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from main import clean_data, get_spark_session


class TestYourCode(unittest.TestCase):

    def setUp(self):
        # Set up a Spark session for testing
        self.spark = get_spark_session()

    def tearDown(self):
        # Stop the Spark session after each test
        self.spark.stop()

    def test_clean_data_sample_json(self):
        # Test case for the 'clean_data' function with 'sample.json' dataset
        bucket_name = f"trend-analysis-dev"
        folder_path = f"input-data/sample.json"
        dataset = "sample.json"

        result_df, end_date = clean_data(self.spark, bucket_name, folder_path, dataset)
        self.assertTrue("timestamp" in result_df.columns)
        self.assertTrue("tweet_text" in result_df.columns)
        self.assertIsNotNone(result_df)
        self.assertIsInstance(end_date, datetime)
        self.assertGreater(result_df.count(), 0)


if __name__ == '__main__':
    unittest.main()
