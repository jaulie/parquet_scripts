# Parquet Tools

Some helpful tools for working with Parquet files in an Apache Spark environment.

- csv_to_parquet.py converts a .csv file to a .parquet format for easier processing
- transform_parquet.py transforms a Parquet file according to the specified arguments
    - --sort_by COLUMN_NAME sorts the file based on the specified column
    - --partition_by_col COLUMN_NAME repartitions the file based on the specified column
    - --partition_by_num NUM repartitions the file based on the specified number of partitions
