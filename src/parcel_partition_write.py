from pyspark.sql import SparkSession
import os

def parcel_partition_write(input_file = 'resources/data/filtered_core_logic.txt', 
                         partition_column = "PROPERTY INDICATOR CODE"):

    try:
        output_directory = f"resources/data/parition_{partition_column}"
        os.makedirs(output_directory, exist_ok=True)
        
        spark = SparkSession.builder.appName("SparkPartitioning").master("local[*]").getOrCreate()
        
        df = spark.read.option("delimiter", "|") \
            .option("header", True) \
            .option("inferSchema", True) \
            .csv(input_file)
        
        df.write \
            .option("header", True) \
            .option("delimiter", "|") \
            .mode("overwrite") \
            .partitionBy(partition_column) \
            .csv(output_directory)
    
        return 'success'
    except:
        return 'failure'

if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('input_file', help='string')
    parser.add_argument('partition_column', help='string')
    args = parser.parse_args()

    status = parcel_partition_write(input_file = args.input_file, partition_column = args.partition_column)