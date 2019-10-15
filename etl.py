import configparser
from datetime import datetime
import shutil
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
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


def process_immigration_data(spark, input_data, output_data, dimension, df_country, df_us_state, df_visa, df_mode):
    '''
        Description: This function can be used to load the immigration data from the current machine
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
        
    df_spark = spark.read\
                    .format("csv")\
                    .option("header", "true")\
                    .load("immigration_data_sample.csv")\
                    .drop('count')    

    
    print('df_spark ', df_spark.count())

    df_spark.createOrReplaceTempView("df_spark")
    df_us_state.createOrReplaceTempView("df_us_state")
    df_visa.createOrReplaceTempView("df_visa")
    df_mode.createOrReplaceTempView("df_mode")    

    df_immigration_clean = spark.sql("""
                                        select 
                                                i.i94yr as year,
                                                i.i94mon as month,
                                                i.i94cit as birth_country,
                                                i.i94res as residence_country,
                                                i.i94port as port,
                                                i.arrdate as arrival_date,
                                                coalesce(m.mode, 'Not reported') as arrival_mode,
                                                coalesce(c.state_code, '99') as us_state,
                                                i.depdate as departure_date,
                                                i.i94bir as repondent_age,
                                                coalesce(v.visa, 'Other') as visa_type_code,
                                                i.dtadfile as date_added,
                                                i.visapost as visa_issued_department,
                                                i.occup as occupation,
                                                i.entdepa as arrival_flag,
                                                i.entdepd as departure_flag,
                                                i.entdepu as update_flag,
                                                i.matflag as match_arrival_departure_fag,
                                                i.biryear as birth_year,
                                                i.dtaddto as allowed_date,
                                                i.insnum as ins_number,
                                                i.airline as airline,
                                                i.admnum as admission_number,
                                                i.fltno as flight_number,
                                                i.visatype as visa_type
                                            from df_spark i left join df_us_state c on i.i94addr=c.state_code
                                                left join df_visa v on i.i94visa=v.visa_code
                                                left join df_mode m on i.i94mode=m.mode_code
                                        """)

    print('df_immigration_clean ',df_immigration_clean.count())
    print('df_immigration_clean ', df_immigration_clean.count())
    print(df_immigration_clean.show(5, truncate=False))
    
    # write data to parquet and partition by year and and month
    dirpath = output_data + dimension
    df_immigration_clean.write.mode("overwrite").partitionBy("us_state", "arrival_mode", "port", "year", "month").parquet(dirpath)


def process_mappings(spark, input_data, output_data, column_names, dimension, separator):
    '''
        Description: This function can be used to process the mapping files from the current machine, cleans them
                     and returns a Spark dataframe.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
            column_names: name of the columns that will be used for the dataframe schema
            dimension: name of the dimension
            separator: separator to be used when reading the input data
        Returns:
            Spark dataframe
    '''
    
    dirpath = output_data + dimension
    
    df = pd.read_csv(input_data, sep=separator, header=None, engine='python',  names = column_names, skipinitialspace = True) 
    print(df.head())
    
    # remove single quotes from the column at index 1
    df.iloc[ : , 1 ] = df.iloc[ : , 1 ].str.replace("'", "")
    
    if(dimension == 'country'):
        df["country"] = df["country"].replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*"], value="Other", regex=True)

    if(dimension == 'us_state'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "").str.replace("\t", "")
        
    if(dimension == 'us_port'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "")
        #splitting city and state by ", " from the city column
        new = df["city"].str.split(", ", n = 1, expand = True) 
        # making separate state column from new data frame 
        df["state"]= new[1].str.strip()
        # replacing the value of city column from new data frame 
        df["city"]= new[0] 
        
    df_spark = spark.createDataFrame(df)
    
    print(df_spark.show(5, truncate=False))
    df_spark.printSchema()
    return df_spark

def process_airports(spark, input_data, output_data, dimension):
    '''
        Description: This function can be used to load the airports data from the current machine
                     and write the parquet files to the output S3 bucket.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
            dimension: name of the dimension
        Returns:
            None
    '''
    airportSchema = R([
                        Fld("airport_id",Str()),
                        Fld("type",Str()),
                        Fld("name",Str()),
                        Fld("elevation_ft",Str()),
                        Fld("continent",Str()),
                        Fld("iso_country",Str()),
                        Fld("iso_region",Str()),
                        Fld("municipality",Str()),
                        Fld("gps_code",Str()),
                        Fld("iata_code",Str()),
                        Fld("local_code",Str()),
                        Fld("coordinates",Str())
                        ])

    df_airport = spark.read.csv(input_data, header='true', schema=airportSchema).distinct()
    print('df_airport ', df_airport.count())
    
    # clean the data
    # filtering only US airports, iata_code not null, data splitting country and state by "-" from the iso_region column & dropping old iso_region column
    df_airport_clean = df_airport.filter("iso_country == 'US'")\
                                 .filter(col("iata_code").isNotNull())\
                                 .withColumn("state", split(col("iso_region"), "-")[1])\
                                 .withColumn("latitude", split(col("coordinates"), ",")[0].cast(Dbl()))\
                                 .withColumn("longitude", split(col("coordinates"), ",")[1].cast(Dbl()))\
                                 .drop("coordinates")\
                                 .drop("iso_region")                                         
    
    print('df_airport_clean ', df_airport_clean.count())
    print(df_airport_clean.show(5, truncate=False)) 
    
    dirpath = output_data + dimension
    df_airport_clean.write.mode("overwrite").partitionBy("state").parquet(dirpath)
    
def process_us_cities_demographics(spark, input_data, output_data, dimension):
    '''
        Description: This function can be used to load the US cities demographics data from the current machine
                     and write the parquet files to the output S3 bucket.
        Arguments:
            spark: SparkSession
            input_data: location for the input data
            output_data: location for the output data
            dimension: name of the dimension
        Returns:
            None
    '''
    
    demographicsSchema = R([
                            Fld("city",Str()),
                            Fld("state",Str()),
                            Fld("median_age",Dbl()),
                            Fld("male_population",Str()),
                            Fld("female_population",Str()),
                            Fld("total_population",Int()),
                            Fld("number_of_veterans",Int()),
                            Fld("number_of_foreign_born",Int()),
                            Fld("average_household_size",Dbl()),
                            Fld("state_code",Str()),
                            Fld("race",Str()),
                            Fld("count",Int()) 
                            ])
    
    df_demographics = spark.read.csv(input_data, header='true', sep=";", schema=demographicsSchema)
    print('df_demographics ', df_demographics.count())
    
    # clean the data
    df_demographics_clean = df_demographics.filter(df_demographics.state.isNotNull())\
                           .dropDuplicates(subset=['state'])
                                                   
    print('df_demographics_clean ', df_demographics_clean.count())
    print(df_demographics_clean.show(5, truncate=False))
    
    dirpath = output_data + dimension
    df_demographics_clean.write.mode("overwrite").partitionBy("state").parquet(dirpath)

def main():
    spark = create_spark_session()
#     KEY                    = config.get('AWS','KEY')
#     SECRET                 = config.get('AWS','SECRET')
#     S3_BUCKET              = config.get('AWS','S3')

#     s3 = boto3.client('s3',
#                            region_name="us-west-2",
#                            aws_access_key_id=KEY,
#                            aws_secret_access_key=SECRET
#                          )

#     bucket_name = "udacity-dend"
        
#    input_data = "s3a://udacity-dend/"
    input_data = ""
#    output_data = "s3a://immigration-data-lake/"
    output_data = ""
    
    df_country = process_mappings(spark, 'i94cntyl.txt', output_data, ["country_code", "country"], "country", " =  ")
    df_us_state = process_mappings(spark, 'i94addrl.txt', output_data, ["state_code", "state"], "us_state", "=")
    df_us_port = process_mappings(spark, 'i94prtl.txt', output_data, ["city_code", "city"], "us_port", "	=	")
    df_visa = process_mappings(spark, 'I94VISA.txt', output_data, ["visa_code", "visa"], "visa", " = ")
    df_mode = process_mappings(spark, 'i94model.txt', output_data, ["mode_code", "mode"], "mode", " = ")
    
    df_country.write.mode("overwrite").parquet(output_data + "country")
    df_us_state.write.mode("overwrite").parquet(output_data + "us_state")
    df_us_port.write.mode("overwrite").parquet(output_data + "us_port")
    
    process_airports(spark, 'airport-codes_csv.csv', output_data, "airports")       
    process_us_cities_demographics(spark, 'us-cities-demographics.csv', output_data, "us_cities_demographic") 
    process_immigration_data(spark, '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', output_data, "immigration_data", df_country, df_us_state, df_visa, df_mode)  

if __name__ == "__main__":
    main()