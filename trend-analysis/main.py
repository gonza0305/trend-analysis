import sys
import os
import logging
import argparse
import boto3
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
import os
import io
import traceback
from multiprocessing import cpu_count
import boto3
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from pyathena import connect
import matplotlib.pyplot as plt
from configurations.great_expectations_configuration import GreatExpectationsConfiguration
from validations.great_expectation_validation import GreatExpectationsValidation

# from great_expectations.data_context import BaseDataContext
# from great_expectations.checkpoint import Checkpoint

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
            .appName("Customer Enrichers")
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.driver.maxResultSize", "5g")
            .config('spark.sql.autoBroadcastJoinThreshold', 10485760)
            .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                    'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
            .config('spark.hadoop.fs.s3a.access.key', os.getenv('AWS_ACCESS_KEY_ID'))
            .config('spark.hadoop.fs.s3a.secret.key', os.getenv('AWS_SECRET_ACCESS_KEY'))
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
            .config("spark.sql.autoBroadcastJoinThreshold", 10485760)
            .enableHiveSupport()
            .getOrCreate()
        )
    return spark


def list_folder_s3(s3, **base_kwargs):
    # list files in a folder of s3
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')


def read_files_parallel(bucket, ifile):
    # read s3 files in parallel
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        return read_file_s3(s3, bucket, ifile)
    except Exception as e:
        print('[error] reading file %s' % ifile)
        print(e)


def read_file_s3(s3: 'obj', bucket: str, file_name: str) -> 'df':
    """
    Read file in s3.
    s3 -- boto3 connection.
    bucket -- bucket name.
    file_name -- path of file to be read.
    return -- read df.
    """
    print('[info] reading file %s' % file_name)
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    return pd.read_csv(
        io.BytesIO(obj['Body'].read()),
        sep="\t", error_bad_lines=False, names=["url", "keyword", "type", "score", "category"])


def find_previous_date(date_list, target_date_str):
    """
        Find the latest date in the provided list that is less than the target date.

        Args:
            date_list (list): List of date strings in the format 'YYYY/MM/DD'.
            target_date_str (str): Target date string in the format 'YYYY/MM/DD'.

        Returns:
            str or None: The largest date in 'YYYY/MM/DD' format that is less than the target date,
                         or None if no such date is found in the list.
    """
    target_date = datetime.strptime(target_date_str, '%Y/%m/%d')

    date_list = [datetime.strptime(date, '%Y/%m/%d') for date in date_list]
    filtered_dates = [date for date in date_list if date < target_date]

    if filtered_dates:
        return max(filtered_dates).strftime('%Y/%m/%d')
    else:
        logger.info("There is no previous date with data")
        return None


def clean_data(bucket_name, folder_path, current_date_str):
    """
    Find the previous date with data in the specified S3 bucket and folder path.

    Args:
        bucket_name (str): Name of the S3 bucket.
        folder_path (str): Path to the folder within the S3 bucket.
        current_date_str (str): Current date in the format 'YYYY/MM/DD'.

    Returns:
        str or None: The previous date with data in the specified format 'YYYY/MM/DD',
                     or None if no data is found within the specified time range.
    """
    current_date = datetime.strptime(current_date_str, '%Y/%m/%d')
    year = current_date.strftime('%Y')
    year_path = f"{folder_path}/{year}/"
    # continuation token is used because list_objects_v2 returns a limit of 1000 records.
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=year_path)
    continuation_token = True
    unique_dates = set()
    while continuation_token:

        if 'Contents' in response:
            for obj in response['Contents']:
                key_parts = obj['Key'].split('/')
                if len(key_parts) >= 4 and len(key_parts[3]) == 2:
                    unique_dates.add(key_parts[2] + "/" + key_parts[3] + "/" + key_parts[4])

        continuation_token = 'NextContinuationToken' in response

        if continuation_token:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=year_path,
                                                 ContinuationToken=response['NextContinuationToken'])

    if len(unique_dates) > 1:
        return find_previous_date(list(unique_dates), current_date_str)
    else:
        logger.info("There is no previous date with data")
        return None


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Trend analysis module')
    parser.add_argument('--env', required=False, default='dev')
    parser.add_argument('--current-date', required=True, default=None)
    args, unknown = parser.parse_known_args()
    s3_client = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                             aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))

    bucket_name = f"trend-analysis-{args.env}"
    folder_path = 'input-data/sample/'

    cleaned_data = clean_data(bucket_name, folder_path, args.current_date)

    logger.info("Called with arguments: %s" % args)
    logger.info("Called with unknown arguments: %s" % unknown)
    args.previous_date = previous_date_with_data
    # Create a great expectation context
    ge_context = BaseDataContext(project_config=GreatExpectationsConfiguration(args.env).get_data_context_config())

    # Execute the checkpoint with all the validations
    result = Checkpoint(
        name="customer_enricher_checkpoint",
        data_context=ge_context,
        run_name_template=f"{args.env}-%Y-%m-%d %H:%M:%S",
        validations=GreatExpectationsValidation(ge_context, get_spark_session(), args).get_validations()
    ).run()

    # Generate documentation
    ge_context.build_data_docs()

    if result['success']:
        logger.info("Validations finished successfully.")
    else:
        logger.error("Some validations have failed..")

Kringloop
