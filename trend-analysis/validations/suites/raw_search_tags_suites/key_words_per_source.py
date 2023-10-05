from pyspark.sql.functions import col
from pyspark.sql.functions import explode
from validations.suites.suite_base import SuiteBase


class KeyWordsPerSource(SuiteBase):

    def __init__(self, context_, spark_, args_):
        self.context = context_
        self.spark = spark_
        self.DATASOURCE_NAME = "raw_search_tags.keywords_per_source"
        self.SUITE_NAME = "suite_to_validate_key_words_per_source_on_raw_search_tags_data"
        self.current_raw_search_tags_path = f"s3a://datlinq-datastore-{args_.env}-custom-features/raw_search_tags/data/{args_.current_date}"
        self.before_raw_search_tags_path = f"s3a://datlinq-datastore-{args_.env}-custom-features/raw_search_tags/data/{args_.previous_date}"

        self.add_expectation_suite(self.SUITE_NAME)
        self.batch_request = self.get_batch_request(self.get_dataframe_to_validate(), self.DATASOURCE_NAME)
        self.add_validator_to_expectation_suite(self.batch_request)

    def get_dataframe_to_validate(self):
        """
        Prepare the data to validate, it returns a dataframe with the total of keywords per source.
        :return: pandas dataframe
        """
        current_raw_search_tags = self.spark.read.parquet(self.current_raw_search_tags_path)
        current_key_words_per_source_df = current_raw_search_tags\
            .select(current_raw_search_tags.do_id, current_raw_search_tags.source,explode(current_raw_search_tags.keyword).alias("keyword"))\
            .groupBy("source").count()\
            .withColumnRenamed("count", "count_current")

        before_raw_search_tags = self.spark.read.parquet(self.before_raw_search_tags_path)
        before_key_words_per_source_df = before_raw_search_tags\
            .select(before_raw_search_tags.do_id, before_raw_search_tags.source,explode(before_raw_search_tags.keyword).alias("keyword"))\
            .groupBy("source").count()\
            .withColumnRenamed("count", "count_before")

        return before_key_words_per_source_df \
            .join(current_key_words_per_source_df, on=["source"], how="left") \
            .withColumn("diff_percent", (col("count_current") * 100) / col("count_before")).toPandas()

    def add_validator_to_expectation_suite(self, batch_request_):
        """
        Add a validator to the great expectation suite
        :param batch_request_: BatchRequest object
        """
        validator = self.context.get_validator(batch_request=batch_request_, expectation_suite_name=self.SUITE_NAME)

        # Add expectations to the validator
        validator.expect_column_values_to_be_between(
            column="diff_percent",
            min_value=95, max_value=105,
            result_format="COMPLETE",
            include_config=True,
            meta={
                "notes": {
                    "format": "markdown",
                    "content": "Validate the percent of the difference between the previous and the current " +
                               "execution checking the amount of keywords per source. \n " +
                               f"- Current data: {self.current_raw_search_tags_path} \n " +
                               f"- Previous data: {self.before_raw_search_tags_path} "
                }
            }
        )
        validator.save_expectation_suite(discard_failed_expectations=False)
