from abc import abstractmethod


class SuiteBase:

    def __init__(self, context_, spark_, args_):
        self.context = context_
        self.spark = spark_
        self.args_ = args_

    def add_expectation_suite(self, suite_name_):
        """
        Add or update a new ExpectationSuite on the context.
        :param suite_name_: str: expectation suite name
        """
        self.context.add_or_update_expectation_suite(expectation_suite_name=suite_name_)

    def get_batch_request(self, pandas_df_, datasource_name_, asset_name_=""):
        """
        Create a BatchRequest based on a pandas dataframe.
        :param pandas_df_: pandas dataframe
        :param datasource_name_: str: data source name. This can be any arbitrary string
        :param asset_name_: str: The name of the Dataframe asset. This can be any arbitrary string
        :return: A BatchRequest object
        """
        dataframe_asset = self.context.sources \
            .add_pandas(datasource_name_) \
            .add_dataframe_asset(name=asset_name_, dataframe=pandas_df_)

        return dataframe_asset.build_batch_request()

    @abstractmethod
    def add_validator_to_expectation_suite(self, batch_request_):
        pass

    @abstractmethod
    def get_dataframe_to_validate(self):
        pass
