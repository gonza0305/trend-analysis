from great_expectations.data_context.types.base import DataContextConfig


class GreatExpectationsConfiguration:
    def __init__(self, environment_):
        self.environment = environment_
        self.bucket_result = f"datlinq-datastore-{self.environment}-custom-features"

    def get_data_context_config(self):
        data_context_config = DataContextConfig()
        data_context_config["stores"] = {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.bucket_result,
                    "prefix": "/trend-analysis-results/result-expectations-store/"
                }
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.bucket_result,
                    "prefix": "/trend-analysis-results/result-validations-store/"
                }
            },
            "evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.bucket_result,
                    "prefix": "/trend-analysis-results/result-checkpoint-store/"
                }
            }
        }
        data_context_config["data_docs_sites"] = {
            "Customer enrichers validation": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": self.bucket_result,
                    "prefix": "/trend-analysis-results/data-docs-sites/"
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder"
                }
            }
        }
        data_context_config["expectations_store_name"] = "expectations_store"
        data_context_config["validations_store_name"] = "validations_store"
        data_context_config["evaluation_parameter_store_name"] = "evaluation_parameter_store"
        return data_context_config
