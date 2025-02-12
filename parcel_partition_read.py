from pyspark.sql import SparkSession
import geopandas as gpd

def parcel_partition_clean(df, formated_columns):
    df.columns = formated_columns
    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['block_level_latitude'], df['block_level_longitude']), crs="EPSG:4326")
    return df

    

def parcel_partition_read(partition_folder, partition_value,
                         pkl_path, pkl_save = False):
    
    spark = SparkSession.builder.appName("ReadPartition").master("local[*]").getOrCreate()
    df_spark = spark.read.option("delimiter", "|").option("header", True).csv(partition_folder)

    selected_columns = ['CLIP','BLOCK LEVEL LATITUDE','BLOCK LEVEL LONGITUDE','TOTAL VALUE CALCULATED',
                        'STATE USE DESCRIPTION','ACRES','UNIVERSAL BUILDING SQUARE FEET','LIVING SQUARE FEET - ALL BUILDINGS']
    formated_columns = ['clip','block_level_latitude','block_level_longitude', 'total_value_calculated', 
                        'status_use_description', 'size_parcel','size_building', 'size_living']

    df_parcel = df_spark.select(selected_columns).toPandas()
    df_parcel = parcel_partition_clean(df_parcel, formated_columns)
    
    if pkl_save:
        df_parcel.to_pickle(pkl_path)
    return df_parcel

if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('partition_folder', help='string')
    parser.add_argument('partition_value', help='string')
    parser.add_argument('pkl_path', help='string')
    parser.add_argument('pkl_save', help='boolean')
    args = parser.parse_args()

    df_parcel = parcel_partition_read(partition_folder = args.partition_folder, partition_value = args.partition_value,
                                      pkl_path = args.pkl_path, pkl_save = args.pkl_save)