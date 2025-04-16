#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Usage:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py CSV_FILE_PATH
    
Example:
    $ spark-submit --deploy-mode client lab_3_storage_template_code.py hdfs:/user/jg5059_nyu_edu/filename.csv

    The above code will create a parquet file from filename.csv called filename.parquet and save it to hdfs:/user/jg5059_nyu_edu/
    It can be accessed from hdfs:/user/jg5059_nyu_edu/filename.parquet
'''


# Import command line arguments and helper functions(if necessary)
import sys
import os

# And pyspark.sql to get the spark session
from pyspark.sql import SparkSession



def main(spark):
    '''Main routine for run for Storage optimization template.
    Parameters
    ----------
    spark : SparkSession object

    '''

    filepath = sys.argv[1] 
    filename = os.path.splitext(os.path.basename(filepath))[0]
    directorypath = os.path.dirname(filepath)

    df = spark.read.csv(filepath, header=True, inferSchema=True)

    df.write.parquet(f"{directorypath}/{filename}.parquet")


# Only enter this block if we're in main
if __name__ == "__main__":

    # Create the spark session object
    spark = SparkSession.builder.appName('part2').getOrCreate()

    #If you wish to command line arguments, look into the sys library(primarily sys.argv)
    #Details are here: https://docs.python.org/3/library/sys.html
    #If using command line arguments, be sure to add them to main function

    main(spark)
    
