import configparser
from datetime import datetime
import shutil
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long, TimestampType as Ts


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
        Description: This function can be used to create a spark session.
        Arguments:
            None
        Returns:
            SparkSession
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data):
    '''
        Description: This function can be used to load the immigration data from the input S3 bucket
                     and write the parquet files to the output S3 bucket.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
        Returns:
            None
    '''
    # Read in the data
    # extractLabel (Default: false): Boolean: extract column labels as column comments for Parquet/Hive
    # inferInt (Default: false): Boolean: infer numeric columns with <=4 bytes, format width >0 and format precision =0, as Int
    # inferLong (Default: false): Boolean: infer numeric columns with <=8 bytes, format width >0 and format precision =0, as Long
#     fname_i94 = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
#     df_spark = spark.read
#                     .format('com.github.saurfang.sas.spark')
#                     .option("extractLabel", true)
#                     .option("inferInt", true)
#                     .option("inferLong", true)
#                     .load(fname_i94)
    dirpath = output_data+"immigration_data"

    
    df_spark = spark.read\
                    .format("csv")\
                    .option("header", "true")\
                    .load("immigration_data_sample.csv")\
                    .drop('count')
    df_spark_cols_renamed = df_spark.withColumnRenamed('i94yr','year')\
                                    .withColumnRenamed('i94mon','month')\
                                    .withColumnRenamed('i94cit','birth_country')\
                                    .withColumnRenamed('i94res','residence_country')\
                                    .withColumnRenamed('i94port','port')\
                                    .withColumnRenamed('arrdate','arrival_date')\
                                    .withColumnRenamed('i94mode','arrival_mode')\
                                    .withColumnRenamed('i94addr','us_state')\
                                    .withColumnRenamed('depdate','departure_date')\
                                    .withColumnRenamed('i94bir','repondent_age')\
                                    .withColumnRenamed('i94visa','visa_code')\
                                    .withColumnRenamed('dtadfile','date_added')\
                                    .withColumnRenamed('visapost','visa_issued_department')\
                                    .withColumnRenamed('occup','occupation')\
                                    .withColumnRenamed('entdepa','arrival_flag')\
                                    .withColumnRenamed('entdepd','departure_flag')\
                                    .withColumnRenamed('entdepu','update_flag')\
                                    .withColumnRenamed('matflag','match_arrival_departure_fag')\
                                    .withColumnRenamed('biryear','birth_year')\
                                    .withColumnRenamed('dtaddto','allowed_date')\
                                    .withColumnRenamed('insnum','ins_number')\
                                    .withColumnRenamed('airline','airline')\
                                    .withColumnRenamed('admnum','admission_number')\
                                    .withColumnRenamed('fltno','flight_number')\
                                    .withColumnRenamed('visatype','visa_type')\
                                    .drop(df_spark.columns[0])
                                    

    # write data to parquet and partition by year and and month
    df_spark_cols_renamed.write.mode('overwrite').partitionBy("year", "month").parquet(dirpath)


def process_mappings(spark, input_data, output_data, file_name, column_names, dimension, separator):
    df = pd.read_csv(input_data+file_name, sep=separator, header=None, engine='python',  names = column_names, skipinitialspace = True) 
    print(df.head())
    df.iloc[ : , 1 ] = df.iloc[ : , 1 ].str.replace("'", "")
    if(dimension == 'us_state'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "").str.replace("\t", "")
    print(df.head())
    print(df.dtypes)
    table = spark.createDataFrame(df).write.mode('overwrite').parquet(output_data + dimension)
     

def main():
    spark = create_spark_session()
        
#    input_data = "s3a://udacity-dend/"
    input_data = ""
#    output_data = "s3a://immigration-data-lake/"
    output_data = ""
    process_mappings(spark, input_data, output_data, 'i94cntyl.txt', ["code", "country"], "country", " =  ")
    process_mappings(spark, input_data, output_data, 'i94addrl.txt', ["code", "state"], "us_state", "=")

    
    demographicsSchema = R([
    Fld("city",Str()),
    Fld("state",Str()),
    Fld("median_age",Dbl()),
    Fld("male_population",Str()),
    Fld("female_population",Str()),
    Fld("total _population",Int()),
    Fld("number_of_veterans",Int()),
    Fld("number_of_foreign_born",Int()),
    Fld("average_household_size",Dbl()),
    Fld("state_code",Str()),
    Fld("race",Str()),
    Fld("count",Int()) 
])
    
    df_demographics = spark.read.csv('us-cities-demographics.csv', header='true', sep=";", schema=demographicsSchema).distinct()
    print(df_demographics.count())
    print(df_demographics.show(5, truncate=False))
    
#     process_immigration_data(spark, input_data, output_data)    
#     process_us_cities_demographics_data(spark, input_data, output_data)

# i94addrl
   


if __name__ == "__main__":
    main()