import configparser
from datetime import datetime
import shutil
import os
import boto3
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf
import datetime as dt
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long


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
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_immigration_data(spark, input_data, output_data, dimension, df_us_state, df_visa, df_mode):
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
   
    df_spark = spark.read\
                    .format('com.github.saurfang.sas.spark')\
                    .load(input_data)    
   
    # get datetime from arrdate column value
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    df_spark = df_spark.withColumn("arrdate", get_date(df_spark.arrdate))
    
#     # testing with the immigration_data_sample
#     df_spark = spark.read\
#                     .format("csv")\
#                     .option("header", "true")\
#                     .load("immigration_data_sample.csv")\
#                     .drop('count') 
    
    print('df_spark ', df_spark.count())

    df_spark.createOrReplaceTempView("df_spark")
    df_us_state.createOrReplaceTempView("df_us_state")
    df_visa.createOrReplaceTempView("df_visa")
    df_mode.createOrReplaceTempView("df_mode")   
  
    # all missing states are 99, get the arrival mode and the visa type from the mappings
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
    # perform data quality checks
    print('df_immigration_clean ',df_immigration_clean.count())
    print(df_immigration_clean.show(5, truncate=False))
    df_immigration_clean.printSchema()
    
    # write data to parquet and partition by year and and month
    dirpath = output_data + dimension
    df_immigration_clean.write.mode("overwrite").partitionBy("year", "month", "us_state").parquet(dirpath+"_us_state")
    df_immigration_clean.write.mode("overwrite").partitionBy("year", "month", "arrival_mode", "port").parquet(dirpath+"_arrival_mode")


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
    
    # replace invalid codes with Other
    if(dimension == 'country'):
        df["country"] = df["country"].replace(to_replace=["No Country.*", "INVALID.*", "Collapsed.*"], value="Other", regex=True)

    if(dimension == 'us_state'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "").str.replace("\t", "")
        
    if(dimension == 'us_port'):
        df.iloc[ : , 0 ] = df.iloc[ : , 0].str.replace("'", "")
        # splitting city and state by ", " from the city column
        new = df["city"].str.split(", ", n = 1, expand = True) 
        # making separate state column from new data frame 
        df["state"]= new[1].str.strip()
        # replacing the value of city column from new data frame 
        df["city"]= new[0] 
    
    # convert pandas dataframe to spark dataframe
    df_spark = spark.createDataFrame(df)
    
    # perform data quality checks
    print(dimension,df_spark.count())
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
    # filtering only US airports, data splitting country and state by "-" from the iso_region column & dropping old iso_region column    
    df_airport_coord = df_airport.filter("iso_country == 'US'")\
                                 .withColumn("state", split(col("iso_region"), "-")[1])\
                                 .withColumn("latitude", split(col("coordinates"), ",")[0].cast(Dbl()))\
                                 .withColumn("longitude", split(col("coordinates"), ",")[1].cast(Dbl()))\
                                 .drop("coordinates")\
                                 .drop("iso_region")\
                                 .drop("continent")
    
    df_airport_coord.createOrReplaceTempView("df_airports") 
    
    # the column airport_code is created by performing a union (so that we only get the distinct values) of the iata_codes and local_code
    df_airport_clean = spark.sql("""
                                select airport_id, type, name, elevation_ft, iso_country, state, municipality, gps_code, iata_code as airport_code, latitude, longitude
                                    from df_airports
                                    where iata_code is not null
                                union
                                select airport_id, type, name, elevation_ft, iso_country, state, municipality, gps_code, local_code  as airport_code, latitude, longitude
                                    from df_airports
                                    where local_code is not null
                                """)
    # the cleaning operations - union included -  reduces the data from 55075 records to 21693
    # perform data quality checks
    print('df_airport_clean ', df_airport_clean.count())
    print(df_airport_clean.show(5, truncate=False)) 
    df_airport_clean.printSchema()
    
    dirpath = output_data + dimension
    df_airport_clean.repartitionByRange(3, "airport_code", "state").write.mode("overwrite").parquet(dirpath)

    
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
    
    # clean the data - no duplicates 2891 records before and after clean-up
    df_demographics_clean = df_demographics.filter(df_demographics.state.isNotNull())\
                           .dropDuplicates(subset=['state', 'city', 'race'])
    
    # perform data quality checks
    print('df_demographics_clean ', df_demographics_clean.count())
    print(df_demographics_clean.show(5, truncate=False))
    df_demographics_clean.printSchema()
    
    dirpath = output_data + dimension
    df_demographics_clean.write.mode("overwrite").partitionBy("state").parquet(dirpath)

# adapted from https://www.developerfiles.com/upload-files-to-s3-with-python-keeping-the-original-folder-structure/
def upload_files(s3, S3_BUCKET, path):
    for subdir, dirs, files in os.walk(path):
        for file in files:
            full_path = os.path.join(subdir, file)
            with open(full_path, 'rb') as data:
                S3_BUCKET.put_object(Key=full_path[len(path)+1:], Body=data)

def main():
    spark = create_spark_session()
        
    input_data = ""
    output_data = "2016_04/"
#     output_data = ""

    KEY                    = config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    S3_BUCKET              = config.get('AWS','S3')

    session = boto3.Session(
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name='us-west-2'
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(S3_BUCKET)

    
    df_country = process_mappings(spark, 'mappings/i94cntyl.txt', output_data, ["country_code", "country"], "country", " =  ")
    df_us_state = process_mappings(spark, 'mappings/i94addrl.txt', output_data, ["state_code", "state"], "us_state", "=")
    df_us_port = process_mappings(spark, 'mappings/i94prtl.txt', output_data, ["port_code", "city"], "us_port", "	=	")
    df_visa = process_mappings(spark, 'mappings/I94VISA.txt', output_data, ["visa_code", "visa"], "visa", " = ")
    df_mode = process_mappings(spark, 'mappings/i94model.txt', output_data, ["mode_code", "mode"], "mode", " = ")
    
    df_country.write.mode("overwrite").parquet(output_data + "countries")
    df_us_state.write.mode("overwrite").parquet(output_data + "us_states")
    df_us_port.write.mode("overwrite").parquet(output_data + "us_ports")
    
    process_airports(spark, 'airport-codes_csv.csv', output_data, "us_airports")       
    process_us_cities_demographics(spark, 'us-cities-demographics.csv', output_data, "us_cities_demographics") 
    process_immigration_data(spark, '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat', output_data, "immigration_data", df_us_state, df_visa, df_mode)
    
    # upload to S3
    upload_files(s3, bucket, './2016_04')      
    

if __name__ == "__main__":
    main()