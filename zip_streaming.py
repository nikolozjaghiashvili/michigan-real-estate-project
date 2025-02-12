from pyspark.sql import SparkSession
import zipfile
from itertools import islice


def read_zipped_file(zip_path, file_name, chunk_size=1000):
    with zipfile.ZipFile(zip_path, 'r') as z:
        with z.open(file_name) as f:
            while True:
                chunk = list(islice(f, chunk_size))
                if not chunk:
                    break
                yield [line.decode('utf-8').strip() for line in chunk]

def zip_streaming(zip_file_path = 'resources/data/coreLogic.zip', 
                 file_name_in_zip = 'coreLogic.txt',
                 output_path = 'resources/data/filtered_core_logic.txt',
                 chunk_size = 10000,
                 state = 'MI'):
    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("Dummy Spark application")
            .getOrCreate()
        )
        
        header = None
        mailing_state_index = None
        with open(output_path, 'w') as output_file:
            for chunk in read_zipped_file(zip_file_path, file_name_in_zip, chunk_size=chunk_size):
                
                if header is None:
                    header = chunk[0].split('|') 
                    mailing_state_index = header.index("SITUS STATE") 
                    output_file.write('|'.join(header) + '\n')
                    chunk = chunk[1:]  
        
                rdd = spark.sparkContext.parallelize(chunk)
                formatted_rows = (
                    rdd.map(lambda line: line.split('|'))
                       .filter(lambda row: row[mailing_state_index] == state)
                       .map(lambda row: '|'.join(row) + '\n')
                       .aggregate("", lambda a,b: a+b, lambda a,b: a+b)
                )
                
                output_file.write(formatted_rows)
        return 'success'
    except:
        return 'failure'


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('zip_file_path', help='string')
    parser.add_argument('file_name_in_zip', help='string')
    parser.add_argument('output_path', help='string')
    parser.add_argument('chunk_size', help='integer')
    parser.add_argument('state', help='string')
    args = parser.parse_args()

    zip_status = zip_streaming(zip_file_path = args.zip_file_path, file_name_in_zip = args.file_name_in_zip, output_path = args.output_path
                              , chunk_size = args.chunk_size, state = args.state)