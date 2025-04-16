#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Tranforms Parquet file based on specifications passed in command line

Usage:
    $ spark-submit --deploy-mode client transform_parquet.py ORIGINAL_PARQUET_FILEPATH

    ORIGINAL_PARQUET_FILEPATH should look like:
    hdfs:/user/USERID_nyu_edu/peopleSmall.parquet

    Your new file will save to:
    hdfs:/user/USERID_nyu_edu/peopleSmallNew.parquet

Arguments:
--sort_by COLUMN_NAME
--repartition_by_num NUM_PARTITIONS
--repartition_by_col COLUMN_NAME

Example: sorting peopleBig.parquet based on the column 'orders':

    $ spark-submit --deploy-mode client transform_parquet.py hdfs:/user/USERID_nyu_edu/peopleBig.parquet --sort_by orders

You can add multiple argument flags to create a file that is sorted and repartitioned:

    $ spark-submit --deploy-mode client transform_parquet.py hdfs:/user/USERID_nyu_edu/peopleBig.parquet --sort_by orders --repartition_by_num 10

'''


import sys
import os
import argparse
from pyspark.sql import SparkSession



def main(spark):
    '''
    Parameters
    ----------
    spark : SparkSession object

    '''
    parser = argparse.ArgumentParser(description="Sort a Parquet file based on a column.")
    
    parser.add_argument("file_path", help="Path to the Parquet file")
    parser.add_argument("--sort_by", help="Column to sort by", default=None)
    parser.add_argument("--repartition_by_num", help="Number of desired partitions",  default=None)
    parser.add_argument("--repartition_by_col", help="Column to partition by",  default=None)

    args = parser.parse_args()

    file_path = args.file_path
    directory_path = os.path.dirname(file_path)
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Load parquet file into DataFrame
    df = spark.read.parquet(file_path)

    # Get the specified optimization from command line
    optimization = sys.argv[2]

    if args.sort_by:
        df = df.orderBy(args.sort_by)
    if args.repartition_by_num and args.repartition_by_col:
        df = df.repartition(args.repartition_by_num, args.repartition_by_col)
    elif args.repartition_by_num:
        df = df.repartition(args.repartition_by_num)
    elif args.repartition_by_col:
        df = df.repartition(args.repartition_by_col)

    df.write.mode("overwrite").parquet(f"{directory_path}/{file_name}New.parquet")


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
