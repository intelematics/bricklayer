"""
    Module to distribute the S3 download over a spark cluster
    Useful when the data is highly partitioned and unable to be loaded by standard methods
    Results end up in a table
    Usage:
    ```
    from file_scanner import DbricksFileScanner
    # define the aws_bucket and output_dir for the s3_fetch to start
    aws_bucket = "service-trips"
    output_dir = "/tmp/"
    # define the target df awaiting to be parse the path
    df = Spark.createDataFrame()
    # export the fetched contents dataframe
    output_df = DbricksFileScanner.download_file(df, aws_bucket, output_dir, path_column)
    ```
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import DataFrame
import logging
import os
import boto3
import csv


class DbricksFileScanner:
    @staticmethod
    def download_file(df: DataFrame, aws_bucket: str, output_dir: str, path_column: str, max_workers: int = 32):
        """encapsulate the pandas udf function as a static method

        Args:
            df (DataFrame): target dataframe
            aws_bucket (str): aws bucket stored all the small files
            output_dir (str): temporary output dir
            path_column (str): path column in the target dataframe
            max_workers (int): number of processors
        Returns:
            [DataFrame]: [output dataframe with downloaded content]
        """
        @pandas_udf('string', PandasUDFType.SCALAR)
        def s3_fetch(paths):
            def download_one_file(bucket: str, output: str, client: boto3.client, s3_file: str):
                """
                Download a single file from S3
                Args:
                    bucket (str): S3 bucket where images are hosted
                    output (str): Dir to store the images
                    client (boto3.client): S3 client
                    s3_file (str): S3 object name
                """
                client.download_file(
                    Bucket=bucket, Key=s3_file,
                    Filename=os.path.join(output, s3_file.replace('/', '_'))
                )

            files_to_download = paths
            # Creating only one session and one client
            session = boto3.Session()
            client = session.client("s3")
            # The client is shared between threads
            func = partial(download_one_file, aws_bucket, output_dir, client)

            # List for storing possible failed downloads to retry later
            failed_downloads = []

            with ThreadPoolExecutor(max_workers) as executor:
                # Using a dict for preserving the downloaded file for each future
                # to store it as a failure if we need that
                futures = {
                    executor.submit(func, file_to_download): 
                    file_to_download for file_to_download in files_to_download
                }
                for future in as_completed(futures):
                    if future.exception():
                        failed_downloads.append(futures[future])
            if len(failed_downloads) > 0:
                with open(
                    os.path.join(output_dir, "failed_downloads.csv"), "w", newline=""
                ) as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                    writer.writerow(failed_downloads)
                   
            def read_file_and_return_contents(path):
                try:
                    with open(output_dir + path.replace('/', '_'), 'r') as file:
                        logging.info(f"Read {file} and return its value")
                        return file.read()
                except FileNotFoundError:
                    logging.warning("Messages is failed to download from s3")
                    return None

            return paths.apply(read_file_and_return_contents)

        return df.withColumn('downloaded_content', s3_fetch(path_column))
