import pandas as pd
import os
import configparser

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import when,col,udf,initcap,split
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id,to_date
config = configparser.ConfigParser()
config.read('credentials.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']

def create_spark_session():
    """
    Creates and returns a spark session to process data
    """
    spark = SparkSession.builder\
            .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
            .config("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
            .config("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
            .enableHiveSupport().getOrCreate()

    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def read_immigration_data(input_data,fname,spark):
    """
    Reads and returns immigration data in spark dataframe
    """
    immigration_data = os.path.join(input_data,"18-83510-I94-Data-2016/",fname)
    print("Reading immigration data file: ", immigration_data)
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)
    return df_spark
    
    
def clean_immigration_data(df_spark, spark):
    """
    Performs cleaning steps on immigration dataframe
    """
    print("Cleaning immigration data..")
    print("Number of rows in immigration dataframe: ",df_spark.count())
    
    # Remove any records with null value for arrival date
    arrdate_null_count = df_spark.filter(df_spark.arrdate.isNull()).count()
    print("Number of rows with null value in arrival date: ",arrdate_null_count)
    
    if arrdate_null_count > 0:
        print("Removing records with null arrival date..")
        df_spark = df_spark.filter(df_spark.arrdate.isNotNull())
    
    # Remove any records with null value for departure date
    depdate_null_count = df_spark.filter(df_spark.depdate.isNull()).count()
    print("Number of rows with null value in departure date: ",depdate_null_count)
    
    if depdate_null_count > 0:
        print("Removing records with null departure date..")
        df_spark = df_spark.filter(df_spark.depdate.isNotNull())
        
    # Remove any records with null value for i94mode
    i94mode_null_count = df_spark.filter(df_spark.i94mode.isNull()).count()
    print("Number of rows with null value in i94mode:",i94mode_null_count)

    if i94mode_null_count > 0:
        print("Removing records with null value for i94mode..")
        df_spark = df_spark.filter(df_spark.i94mode.isNotNull())
       
    # Remove any records with null value for i94visa
    i94visa_null_count = df_spark.filter(df_spark.i94visa.isNull()).count()
    print("Number of rows with null value in i94visa:",i94visa_null_count)

    
    if i94visa_null_count > 0:
        print("Removing records with null value for i94visa..")
        df_spark = df_spark.filter(df_spark.i94visa.isNotNull())
    
    # Remove any records with null value for birth year
    biryear_null_count = df_spark.filter(df_spark.biryear.isNull()).count()
    print("Number of rows with null value in biryear:" ,biryear_null_count)

    if biryear_null_count > 0:
        print("Removing records with null value for biryear..")
        df_spark = df_spark.filter(df_spark.biryear.isNotNull())
      
    # Remove any records with null value for gender
    gender_null_count = df_spark.filter(df_spark.gender.isNull()).count()
    print("Number of rows with null value in gender:", gender_null_count)

    if gender_null_count > 0:
        print("Removing records with null value for gender..")
        df_spark = df_spark.filter(df_spark.gender.isNotNull())
        
    print("Total number of rows in immigration dataframe after removing null values: ",df_spark.count())

    return df_spark


def process_immigration_data(df_spark, output_data, spark):
    """
    Process immigration data to generate immigration fact table
    Converts arrival date, departure date to datetime format
    Maps i94cit and i94res columns to country names
    Maps i94mode column to respecive travel mode
    Maps i94visa column to respective visa category
    Writes the final immigration fact table to s3
    """
    
    epoch = datetime(1960, 1, 1).date()
    get_timestamp = udf(lambda x : (epoch + timedelta(int(x))).isoformat() if x else None )
    
    # converting arrival and departure dates in sas format to datetime
    print("Converting arrival and departure dates to datetime..")
    df_spark = df_spark.withColumn('arrival_date', to_date(get_timestamp(df_spark.arrdate)))
    df_spark = df_spark.withColumn('departure_date', to_date(get_timestamp(df_spark.depdate)))
    
    # map i94cit and i94res columns to respective country names
    print("Mapping i94cit and i94res columns to respective country names..")
    df_valid_i94cit_i94res = spark.read.csv("valid_i94cit_i94res.txt",sep=";",header=True,inferSchema=True)
    df_spark = df_spark.join(df_valid_i94cit_i94res.select(['code','description']),\
                             df_spark.i94cit == df_valid_i94cit_i94res.code,\
                             how = 'left').drop('code')
    
    df_spark = df_spark.withColumn("country", initcap(col("description"))).drop('description')
    
    df_spark = df_spark.join(df_valid_i94cit_i94res.select(['code','description']),\
                             df_spark.i94res == df_valid_i94cit_i94res.code,\
                             how = 'left').drop('code')
    
    df_spark = df_spark.withColumn("country_of_residence", initcap("description")).drop('description')
    
    # map i94mode column to respective travel modes  
    print("Mapping i94mode column to respective mode of travel..")
    df_spark = df_spark.withColumn("travel_mode",\
                                   when(col("i94mode") == 1, "Air").\
                                   when(col("i94mode")==2,"Sea" ).\
                                   when(col("i94mode")==3, "Land").\
                                   otherwise("Not Reported"))
    
    # map i94visa to respective visa category
    print("Mapping i94visa to respective visa category..")
    df_spark = df_spark.withColumn("visa_category",\
                                   when(col("i94visa") == 1, "Business").\
                                   when(col("i94visa")==2,"Pleasure" ).\
                                   when(col("i94visa")==3, "Student").\
                                   otherwise("Unknown"))
    
    # filter columns for final fact table
    immigration_table = df_spark.select(\
                                        col("cicid").alias("immigration_id"),\
                                        col("i94yr").alias("arrival_year"),\
                                        col("i94mon").alias("arrival_month"),
                                        col("i94port").alias("port_of_entry"),\
                                        col("travel_mode"),\
                                        col("i94addr").alias("destination_state"),\
                                        to_date(col("arrival_date")).alias("arrival_date"),\
                                        to_date(col("departure_date")).alias("departure_date"),\
                                        col("country"),\
                                        col("country_of_residence"),\
                                        col("visatype"),\
                                        col("visa_category"),\
                                        col("biryear").alias("birth_year"),\
                                        col("gender"))
    
    # write to s3
    print("Writing immigration fact table to s3..")
    immigration_output = os.path.join(output_data, 'immigration/immigration.parquet')
    immigration_table.write.partitionBy("arrival_year", "arrival_month").parquet(immigration_output,'overwrite')

    return immigration_table
    

def create_arrival_dim(immigration_table, output_data, spark):
    """
    Creates and writes arrival_time dimension table to s3
    """
    print("Filtering immigration table to generate arrival time dimension table..")
    arrival_time = immigration_table.select(\
                                               to_date(col("arrival_date")).alias("arrival_date"),\
                                               dayofmonth(immigration_table.arrival_date).alias('day'),\
                                               weekofyear(immigration_table.arrival_date).alias('week'),\
                                               month(immigration_table.arrival_date).alias('month'),\
                                               year(immigration_table.arrival_date).alias('year'),
                                               dayofweek(immigration_table.arrival_date).alias('weekday')).dropDuplicates()
    
    arrival_time_dim = arrival_time.withColumn("id",monotonically_increasing_id())
    print("Writing arrival time dimension table to parquet..")
    arrival_dim_output = os.path.join(output_data, 'arrivals/arrival_time.parquet')
    arrival_time_dim.write.parquet(arrival_dim_output,'overwrite')
    return arrival_time_dim

def process_temperature_data(input_data,fname,output_data,spark):
    """
    Processes global temperature data by city to generate 
    aggregated average temperature for each country.
    Writes temperature dimension table to s3.
    """
    temperature_data_file = os.path.join(input_data,fname)
    print("Reading temperature data from file ", temperature_data_file)
    df_spark = spark.read.csv(temperature_data_file,header=True,inferSchema=True)
    print("Total number of rows in temperature dataframe:", df_spark.count())
    
    avg_temp_null_count = df_spark.filter(df_spark.AverageTemperature.isNull()).count()
    print("Number of rows with null value for average temperature: ", avg_temp_null_count)
    
    print("Filtering out the rows with null value for average temperature..")
    df_spark = df_spark.dropna(subset=['AverageTemperature'])
    
    print("Removing duplicate records for subset dt, Country, City")
    df_spark = df_spark.dropDuplicates(subset=['dt','City','Country'])
    
    print("Generating average temperature by grouping on country..")
    df_temperature = df_spark.groupby("Country").avg("AverageTemperature")
    temperature_table = df_temperature.withColumnRenamed("Country","country")\
                                      .withColumnRenamed("avg(AverageTemperature)","average_temperature")
    
    temperature_table = temperature_table.withColumn("id",monotonically_increasing_id())
    print("Writing temperature dimension table to parquet..")
    temperature_dim_output = os.path.join(output_data, 'temperature/temperature.parquet')
    temperature_table.write.parquet(temperature_dim_output,'overwrite')
    return temperature_table


def process_demographic_data(input_data,fname,output_data,spark):
    """
    Process demographic data to generate state level population demographics.
    Writes demographics dimension table to s3
    """
    demographics_data_file = os.path.join(input_data,fname)
    print("Reading demographics data file" ,demographics_data_file)
    df_spark = spark.read.csv(demographics_data_file, sep=";",header=True, inferSchema=True)
    
    print("Total number of rows in demographics data: ", df_spark.count())
    
    print("Aggregating demographics data for each state..")
    population_demographics  = df_spark.groupby("State Code").\
                                        agg({"Total Population" :'sum',\
                                             "Male Population":'sum',\
                                             "Female Population":'sum',\
                                             "Number Of Veterans":'sum',\
                                             "Foreign-born" :'sum'})
    
    print("Number of rows in population demographics by state: ",population_demographics.count())
    
    population_demographics = population_demographics.withColumnRenamed("State Code", "state_code")\
                                                    .withColumnRenamed("sum(Total Population)", "total_population")\
                                                    .withColumnRenamed("sum(Number Of Veterans)", "number_of_veterans")\
                                                    .withColumnRenamed("sum(Female Population)", "female_population")\
                                                    .withColumnRenamed("sum(Male Population)", "male_population")\
                                                    .withColumnRenamed("sum(Foreign-born)", "foreign_born")
    print("Reading state latitude, longitude data..")
    df_state_lat_long = spark.read.csv("statelatlong.csv",header=True,inferSchema=True)
    
    population_demographics = population_demographics.join(df_state_lat_long.select(['State','Latitude','Longitude']),  \
                                                            population_demographics.state_code == df_state_lat_long.State, \
                                                            how = 'left').drop('State')
    
    # Replace null values for number of veterans and foreign born with 0 
    population_demographics = population_demographics.fillna(0,subset=["number_of_veterans","foreign_born"])
    
    population_demographics_table = population_demographics.select(\
                                                                  col("state_code"),\
                                                                  col("total_population"),\
                                                                  col("male_population"),\
                                                                  col("female_population"),\
                                                                  col("number_of_veterans"),\
                                                                  col("foreign_born"),\
                                                                  col("Latitude").alias("latitude"),\
                                                                  col("Longitude").alias("longitude"))\
                                                                  .withColumn("id",monotonically_increasing_id())
    
    print("Writing demographics dimension table to parquet..")
    demographics_output = os.path.join(output_data, 'demographics/demographics.parquet')
    population_demographics_table.write.parquet(demographics_output,'overwrite')
    return population_demographics_table
    

def process_airport_codes_data(input_data,fname, output_data,spark):
    """
    Process airport codes data to generate us airports table 
    Write airports dimension table to s3
    """
    airport_codes_data_file = os.path.join(input_data,fname)
    print("Reading airport codes data from ",airport_codes_data_file)
    df_spark = spark.read.csv("airport-codes_csv.csv",header=True)
    
    print("Total number of rows in airport codes data:", df_spark.count())
    
    print("Filtering out airport codes for US which have valid iata_code..")
    valid_airport_codes = df_spark.where("iso_country = 'US'")
    valid_airport_codes = valid_airport_codes.filter(valid_airport_codes.iata_code.isNotNull())
    
    print("Total number of rows for valid airport codes: ", valid_airport_codes.count())
    
    print("Formatting iso_region column to get state code..")
    format_region = udf(lambda x : x[3:])
    valid_airport_codes = valid_airport_codes.withColumn('state_code', format_region(valid_airport_codes.iso_region)).drop("iso_region")
    
    print("Formatting coordinates column to get latitude and longitude..")
    valid_airport_codes = valid_airport_codes.withColumn("latitude", (split(col("coordinates"),",").getItem(0)).cast('double'))\
                                .withColumn("longitude", (split(col("coordinates"),", ").getItem(1)).cast('double'))\
                                .drop("coordinates")
    
    airports_table = valid_airport_codes.select(\
                                           col("ident"),\
                                           col("name"),\
                                           col("iata_code"),\
                                           col("state_code"),\
                                           col("latitude"),\
                                           col("longitude")).withColumn("id",monotonically_increasing_id())
    
    print("Writing airports dimension table to parquet..")
    airports_output = os.path.join(output_data, 'airports/airports.parquet')
    airports_table.write.parquet(airports_output,'overwrite')
    return airports_table
    
def run_data_quality_check(table,table_name):
    """
    Run data quality check
    """
    print("Checking ",table_name)
    number_of_rows = table.count()
    if number_of_rows == 0:
        print("Data quality check failed for ",table_name)
    else:
        print("Data quality check passed for", table_name ,"contains, ", number_of_rows, "rows")
    
    
def main():
    """
    Initiallize input and output data paths
    Calls process functions for song and log data
    """
    spark = create_spark_session()
    
    # initialize file input path to read immigration data
    input_data = "../../data/"
    filename = "i94_apr16_sub.sas7bdat"
    #ilename = "i94_jul16_sub.sas7bdat"
    
    # read immigration data
    df = read_immigration_data(input_data,filename, spark)
    
    # clean immigration data
    df_immigration = clean_immigration_data(df, spark)
    
    # output path for writing parquet files
    output_data = "s3a://capstone-project-latest/"
    immigration_table = process_immigration_data(df_immigration, output_data, spark)
    
    # create arrival_time dimension table
    arrival_time_table = create_arrival_dim(immigration_table, output_data,spark)
    
    # process temperature data
    input_data = "../../data2"
    fname = "GlobalLandTemperaturesByCity.csv"
    temperature_table = process_temperature_data(input_data, fname, output_data, spark)
    
    # process demographics data
    input_data = "./"
    fname = "us-cities-demographics.csv"
    us_population_demographics_table = process_demographic_data(input_data, fname, output_data, spark)
    
    # process airport codes data
    input_data = "./"
    fname = "airport-codes_csv.csv"
    airports_table = process_airport_codes_data(input_data,fname,output_data,spark)
    
    # run data quality checks
    run_data_quality_check(immigration_table,"immigration_table")
    run_data_quality_check(arrival_time_table,"arrival_time_table")
    run_data_quality_check(temperature_table,"temperature_table")
    run_data_quality_check(us_population_demographics_table,"us_population_demographics_table")
    run_data_quality_check(airports_table,"airports_table")

    
if __name__ == "__main__":
    main()