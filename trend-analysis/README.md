# Great expectation validations

This module uses Great Expectations(GE) to ensure data quality. [Notion page](https://www.notion.so/roamler/Data-Quality-Great-expectations-610002a0a3714c33902766b63dcdacb0)

## Description

The main.py class in the entry point executed by the EMR job launched from Airflow.
This class will create a great expectation context based on the configurations (great_expectations_configurations.py),
then will add the suitcase with all the validation in there and finally creates a checkpoint to execute the validation.
The results are stored on S3 as is set in the configuration.


## How to deploy it?
In order to deploy any change made in the great expectation validations, you need to Execute the 
*datalabs-enrichers-customer-great-exp-{env}* Codebuild with the branch that you want to deploy. 
To 'prod' environment you must use the master branch.
   
This codebuild will copy the python code used by the EMR cluster in the 
*'s3://datlinq-datalabs-{env}-jobs-python/great-expectations/'* bucket.

The EMR cluster is triggered from an Airflow DAG, and will execute the main.py class. 

The DAG code is in the 'Airflow' project.

## How to run it locally?
1. Set on True the *local_execution* variable in the main.py class.
2. Configure the AWS credentials on your local machine.
3. Execute the  main.py class.

Once the execution finish, you can see the results on the 
*s3://datlinq-datastore-dev-custom-features/great-expectations-results/* bucket
