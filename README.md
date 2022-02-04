# The Immigration Project

## Data Engineering Capstone Project

The Immigration Project aims at creating an end-to-end pipeline for the given immigration dataset. This is a capstone project for data engineering nanodegree.
The project follows following steps:

- Step 1: Scope the Project and Gather Data
- Step 2: Explore and Assess the Data
- Step 3: Define the Data Model
- Step 4: Run ETL to model the Data
- Step 5: Complete Project Write Up


### Step 1: Scope the Project and Gather Data
​
#### Scope 
The scope of this project is to explore the I94 data, US city demographics data, and Global temperatures data. After completing thorough inspection of data we will come up with a data model that can best answer the analysts questions such as, 
​
- How many people arrive in US within a given timeframe?
- What is the most favourite visa category to travel to US? 
- How does weather affect immigrations? 
- What is the most favourite destination state after arriving in US?
- What is immigrants most favourable mode of travel, air, sea or land?
- For air travellers, which states have most number of visits from buisness category visa holders?
​
These are a few example questions that can be asked. We can always come up with more questions and interesting insights in the data. For the sake of this project we are going to limit analysis to few questions and focus on creating an user friendly, easy to maintain analytic database. 
​
#### Describe and Gather Data 
​
We will be using following datasets for this project.
​
- **I94 Immigration Data:**
    This data comes from the US National Tourism and Trade Office. More information about this data can be found [here](https://travel.trade.gov/research/reports/i94/historical/2016.html)
    `I94_SAS_Labels_Descriptions.SAS` file describes each field and its meaning. 
    
- **World Temperature Data:**
    We will be using this dataset from kaggle. It can be downloaded from [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
    The data provides average temperatures for countries starting from 1750's. 
    
- **U.S city Demographic Data:**
    This data comes from opensoft. It can be downloaded from [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)
    This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
    
- **Airport Code Table:**
    This is a simple table of airport codes and corresponding cities. It is provided in the workspace. 
    

### Step 2: Explore and Assess the Data

In this step we will explore each dataset for total number of rows, missing values, duplicate data.

1. Immigration data:

    A small subset of records have null value in their departure date column. We will delete these records as part of cleaning the immigration data step. 
    Records which have i94mode, biryear, gender value null are also removed. 
    
    As part of cleaning step data columns in immigration data are mapped to their respective values. 
    The arrival date and departure date columns are in SAS date format, we convert these to datetime object as part of the cleaning step.
    The i94cit and i94res columns are codes used for processing. We will change these columns to have proper country names. 
    Mapping for these codes can be found in `I94_SAS_Labels_Descriptions.SAS`.
    A simple text file was created for all valid codes. After joining the immigration data and valid codes dataframes, invalid codes in the immigration data will be mapped to null. 
    
    Other columns mapped to their respective description are i94mode and i94visa. 
    
2. Temperature data:

    Temperature data by cities contains average temperature recorded for global cities dating as far back as 1750's. There are a lot of rows with null value for average temperature. As part of    the data cleaning step we remove these rows. Also we check for duplicates for subset of columns, which are dt, city, country. We remove these duplicate records. After these steps, we get the average of average temperature column by grouping on country column. 
    

3. US City demographics data:
    
    City demographics data provides information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. 
    We generate aggregate dataframe of different columns in this data such as total population, male population, female population etc. by grouping on state code. 
    For states with null values for number of veterans and foreign born columns, are set to 0. 
    This aggregate dataset is joined with state latitude longitude information as provided in `statelatlong.csv`
    
4. Airport codes data:

     This is a simple table of airport codes and corresponding cities. As part of cleaning step, we generate a datframe containing only valid airport codes from US. A valid airport code is considered a non-null value for iata_code column. 
    
    
### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model


![Data Model](https://lh3.googleusercontent.com/gDDqi8KOSfahKNd5WHjO9HfFQxXF2WAqyKFHxPMkJlnLZg7HaC-LEnGXfnyU1-tyfp5lkakqrLfBdcWdst7XHG7pB_uABJD3I0mC7CnaCC_LLOaY_yRPtotIx4juy9svGRfQ8wPqK05m5hH8bOCsrEA27aLItiZWghg4ojrNc3oHM1jcBA5SMUuJWbDlziOcFoXRywvEA4PenMIsbB1nswxv73-Sz3TxmOreza8DHaSjpwUGY1NKaEs3LSJe6IHtMvp-_Ec8PbGbZXxLxvo5uAuyECGxVDgafVy8llQEuzr6J2goQiVNzKVGvdkkUL1U4HqxqjWfnCvEPSHbLjetgxU_O8GBbFyutGNuE26E3CdiTVIkXqdY0tNtJuUD0O-YLxbBKIOWzI2pOBFXDHv0MEdwaPS0K-C4XnW9b4a2UDCk3TgzhFwLldHR-RaliKcn9nPwP5YhFZIspOmOOHMLp1T5hjqy2eMP6sUOQTgQ6WNdGZ5L0Wqv82N6XWB34mIHXp9O6NL4SwFd1fDAShcrRJT9VzTOcnbRbeXYFaTsihEfqtVNx6au8Ehrt59unQB6o6QiIUI_-7mEVrhlFZo-CbcYaS3vh_2kqnxJXoaTj7DXcuM4xEFpqyZolfSeGp4FPO9zY43ywuhO1VFlXiPWm0FKd4Ca6G2XRW7f_YtAEwKiMhiQSWvLPpJ0EXU4Kg=w1340-h873-no?authuser=0)

![Data Model](https://github.com/mmali1/ImmigrationProject/blob/ebcfd165a061a7ebac69a5b53b3b6f94e4d259be/data_model.png?raw=true)

   The data model chosen for this project represents star schema. We have one immigration_fact table which stores each unique immigration record with facts such as visa type, country of residence, destination state, arrival date, departure date etc. 

The arrival_time dimension table is derived from fact table. It stores each unique arrival date from immigration_fact table. 

The country dimension table is derived from global temperature data. The original dataset contains average temperature recorded for each country across different city over a period of time. For the sake of this project we are aggregating these temperatures to country level. The country column in the country dimension table links to the country_of_residence column in immigration_fact table. This can be used to understand if there is any correlation between global climate change and immigrations. 

The us_population_demographics represents the aggregation of us city demographics data. The total population, male population, female population, number of veterans and foreign born column values are basically aggregate values of each respective column for each state. The state_code column in this table links to the destination_state column in fact table.We can try to see if there are any particular states that serve as hotspots for immigration. This data is also combined with state latitiude, longitude coordinates for easy visualisation on maps.


The us_airports dimension table represents each unique record of airport codes.The iata_code column in this table is linked to the port of entry column in fact table. With this dimension table, we can answer questions such as are there any favourite port of entry for business category visa holders and so on. 

The choice of star schema is made based on it's efficiency to query large data sets and support for OLAP cubes, and analytic applications. 

Below are detailed steps of the data pipeline to generate above data model:

1. Read immigration data
2. Perform cleaning and mapping steps on immigration data that result into final clean immigration data table. 
3. Write the fact table generated in step 2 to s3 bucket in parquet format, partitioned by year and month.
4. Filter unique arrival dates from clean immigration data to generate arrival dimension table.
5. Write arrival dimension table to s3 bucket in parquet format.
6. Read temperature data.
7. Perform cleaning steps on temperature data.
8. Generate a new table from clean temperature data by aggregating average temperature for each country.
9. Write country temperature dimension table to s3 in parquet format.
9. Read US city demographics data.
10. Perform cleaning and mapping steps on demographics data.
11. Generate a new table from clean demographics data aggregating total population, male population, female population, number of veterans and foreign born for each state.
12. Write population demographics by state table to s3 in parquet format.
13. Read airport codes data.
14. Perform cleaning and mapping steps on airport codes data.
15. Write airport dimension table to s3 bucket in parquet format. 


### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

The data pipeline script `etl.py` can be run to create the data model. 

To run the script simply inside the workspace execute `python etl.py` in terminal. Please update `credentials.cfg` to the aws access code and secret for your account before executing `etl.py`

A more elegant solution would be to move all input files to s3. Modify the `etl.py` script to read data from s3. Create an **EMR Cluster**. Coonect to the master node using `ssh`. Copy the `etl.py` script to master node using `scp`

Then run `spark-submit --master yarn etl.py` command in terminal to run the script as spark job. 

#### 4.2 Data Quality Checks

Following data quality checks are incorporated in `etl.py`

 * Source/Count checks to ensure completeness
 
#### 4.3 Data dictionary 

Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

1. immigration_fact

The immigration fact table is derived from immigration data. Meaning of each field is determined and reported here as described in `I94_SAS_Labels_Descriptions.SAS`
A subset of information provided in this file is used for mapping i94cit and i94res columns in immigration data to country names. Mapping file is saved as `valid_i94cit_i94res.txt`

| Column | Derived from | Description |
| ----------- | ----------- | ----------- |
| immigration_id | cicid | unique record identifier for immigration table |
| arrival_year | i94yr | arrival year |
| arrival_month | i94mon | arrival month |
| port_of_entry | i94port | port where the immigrant arrived |
| travel_mode | i94mode | Air if i94mode = 1, Sea if i94 mode = 2, Land if i94mode = 3 and Not reported if i94mode = 9 |
| destination_state | i94addr | destination state after arriving in US |
| arrival_date | arrdate | arrdate column converted to datetime |
| departure_date | depdate | depdate column converted to datetime |
| country | i94cit | mapping numeric country codes to country names |
| country_of_residence | i94res | mapping numeric country codes to country names |
| visatype | visatype | class of admission legally admitting the non-immigrant to temporarily stay in US |
| visa_category | i94visa | visa codes 1 = Business, 2 = pleasure and 3 = student |
| birth_year | biryear | 4 digit year of birth |
| gender | gender | gender of non-immigrant |

2. arrival_time 

The arrival_time dimension table is derived from immigration fact table. 

| Column | Derived from | Description |
| ----------- | ----------- | ----------- |
| id | primary key | unique record identifier for arrival_time table |
| arrival_date | arrival_date | arrival date in US |
| day | dayofmonth | arrival day |
| week | weekofyear | week of arrival |
| month | month | arrival month |
| year | year | arrival year |
| weekday | dayofweek | arrival day of the week |

3. country 

The country dimension table is derived from global temperature data.

| Column | Derived from | Description |
| ----------- | ----------- | ----------- |
| id | primary key | unique record identifier for country dimension table |
| country | Country | name of the country, foreign key to the country and country_of_residence column in immigration_fact table |
| average_temperature | avg(AverageTemperature | average temperature for country for given time period |

4. us_population_demographics 

The us_population_demographics table is derived from us city population demographics. This data is then combined with `statelatlong.csv` to get latitude and longitude values for each state in population demographics.

| Column | Derived from | Description |
| ----------- | ----------- | ----------- |
| id | primary key | unique record identifier for us population dimension table |
| state_code | State Code | two letter state abbreviation, foreign key to the destination_state column in immigration_fact table |
| total_population | Sum(Total Population) | sum of total population for cities in a state |
| male_population | Sum(Male Population) | sum of male population for cities in a state |
| female_population | Sum(Female Population) | sum of female population for cities in a state |
| number_of_veterans | Sum(Number Of Veterans) | sum of number of veterans for cities in a state, 0 if None |
| foreign_born | Sum(Foreign-born) | sum of foreign born for cities in a state, 0 if None |
| latitude | Latitude | latitude for state |
| longitude | Longitude | longitude for state |

5. us_airports

The us_airports table is derived from airport codes data.

| Column | Derived from | Description |
| ----------- | ----------- | ----------- |
| id | primary key | unique record identifier for us airports dimension table |
| state_code | iso_region | two letter abbreviated state code, foreign key to the destination_state column in immigration fact table |
| ident | ident | identifier for airport |
| name | name | name of airport |
| iata_code | iata_code | valid 3 letter port code, foreign key to the port_of_entry column in immigation_fact table |
| latitude | coordinates | first value after splitting coordinates on "," |
| longitude | coordinates | second value after splitting coordinates on "," |


#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.

The target end product of this project is a data lake using spark as it provides easy transformation of raw data into structured data ready for SQL analytics. Any and all data types can be collected and retained indefinitely in a data lake. Spark is chosen as it allows rapid distributed processing of data in data lake. 

The resulting data tables are stored in Amazon s3 as it provides cost effective object storage for data lakes. 

* Propose how often the data should be updated and why.

The data should be updated as and when the new data comes in. Assuming new data comes in each month, we will have to run our ETL pipeline monthly to get the most up to date data. 

* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 
 *An EMR cluster of nodes running spark can be used if the data was increased by 100x.*
 
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 
 *The ETL pipeline can be integrated with Airflow. A DAG script can be written which will break each task of the pipeline in seperate operators. This DAG can be scheduled to run every morning and update the dashboard.*
 
 * The database needed to be accessed by 100+ people.
 
 *In this case, we can move our analytics tables on Amazon Redshift data warehouse. Amazon Redshift provides fast scaling up or down by quickly activating individual nodes of varying sizes. Redshhift's massive parallel processing lets multiple queries run across multiple nodes.*
