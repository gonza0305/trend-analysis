from pyspark.sql.functions import col, lit
from validations.suites.suite_base import SuiteBase


class ColumnCountsPerCountry(SuiteBase):

    def __init__(self, context_, spark_, args_):
        self.context = context_
        self.spark = spark_
        self.DATASOURCE_NAME = "fc_potential_features.column_counts_per_country"
        self.SUITE_NAME = "suite_to_validate_fc_potential_features_data"
        self.current_fc_potential_features_path = f"s3a://datlinq-datastore-{args_.env}-custom-features/fc_potential_features/data/{args_.current_date}"
        self.before_fc_potential_features_path = f"s3a://datlinq-datastore-{args_.env}-custom-features/fc_potential_features/data/{args_.previous_date}"

        self.add_expectation_suite(self.SUITE_NAME)
        self.batch_request = self.get_batch_request(self.get_dataframe_to_validate(), self.DATASOURCE_NAME)
        self.add_validator_to_expectation_suite(self.batch_request)

    def get_dataframe_to_validate(self):
        """
        Prepare the data to validate, it returns a dataframe with the total do_id per country with a
        value of debic_class, cream_class, cuisine_class in separate rows
        :return: pandas dataframe
        """
        current_fc_potential_features = self.spark.read.parquet(self.current_fc_potential_features_path).persist()
        before_fc_potential_features = self.spark.read.parquet(self.before_fc_potential_features_path).persist()

        current_counts_df = (
            current_fc_potential_features.filter(col("cream_class").isNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('cream_class')).withColumnRenamed("count", "current_count")
        ).union(
            current_fc_potential_features.filter(col("cuisine_class").isNotNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('cuisine_class')).withColumnRenamed("count", "current_count")
        ).union(
            current_fc_potential_features.filter(col("debic_class").isNotNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('debic_class')).withColumnRenamed("count", "current_count")
        )

        before_counts_df = (
            before_fc_potential_features.filter(col("cream_class").isNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('cream_class')).withColumnRenamed("count", "before_count")
        ).union(
            before_fc_potential_features.filter(col("cuisine_class").isNotNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('cuisine_class')).withColumnRenamed("count", "before_count")
        ).union(
            before_fc_potential_features.filter(col("debic_class").isNotNull()).groupBy("country_code").count()
            .withColumn("column_name", lit('debic_class')).withColumnRenamed("count", "before_count")
        )

        return before_counts_df \
            .join(current_counts_df, on=["country_code", "column_name"], how="left") \
            .withColumn("diff_percent", (col("current_count") * 100) / col("before_count")).toPandas()

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
                               "execution, checking the count of cream_class, cuisine_class and debic_class columns " +
                               "per country. \n" +
                               f"- Current data: {self.current_fc_potential_features_path} \n" +
                               f"- Previous data: {self.before_fc_potential_features_path} "
                }
            }
        )
        validator.save_expectation_suite(discard_failed_expectations=False)
