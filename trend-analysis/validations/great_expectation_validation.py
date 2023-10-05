from validations.suites.raw_search_tags_suites.key_words_per_country import KeyWordsPerCountry
from validations.suites.raw_search_tags_suites.key_words_per_source import KeyWordsPerSource
from validations.suites.fc_potential_features_suites.column_counts_per_country import ColumnCountsPerCountry


class GreatExpectationsValidation:
    """
    This class contains the great expectation validations to the output of the Job1 abd Job2
    """

    def __init__(self, context_, spark_, args_):
        self.suites = [
            KeyWordsPerCountry(context_, spark_, args_),
            KeyWordsPerSource(context_, spark_, args_),
            ColumnCountsPerCountry(context_, spark_, args_)
        ]

    def get_validations(self):
        """
        This method return a list of validations
        :return: List[dict]: List of validations
        """
        return list(map(lambda suite: {
            "batch_request": suite.batch_request,
            "expectation_suite_name": suite.SUITE_NAME}, self.suites))
