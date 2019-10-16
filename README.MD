## Scoping the Project

#### Explore the Data & Cleaning Steps

- **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. This is where the data comes from. immigration_data_sample.csv contains the sample data.
- **U.S. City Demographic Data**: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This data comes from the US Census Bureau's 2015 American Community Survey. 
- **Airport Code Table**: This data comes from [ourairports](http://ourairports.com/data/airports.csv). It is a simple table of airport codes and corresponding cities. It contains the list of all airport codes, the attributes are identified in datapackage description. Some of the columns contain attributes identifying airport locations, other codes (IATA, local if exist) that are relevant to identification of an airport.

##### Immigration Data
- I94YR - 4 digit year
- I94MON - Numeric month
- I94CIT & I94RES - This format shows all the valid and invalid codes for processing
- I94PORT - This format shows all the valid and invalid codes for processing
- ARRDATE is the Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94MODE - There are missing values as well as not reported (9)
- I94ADDR - There is lots of invalid codes in this variable and the list below 
   shows what we have found to be valid, everything else goes into 'other'
- DEPDATE is the Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you.
- I94BIR - Age of Respondent in Years
- I94VISA - Visa codes collapsed into three categories
- COUNT - Used for summary statistics
- DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use
- VISAPOST - Department of State where where Visa was issued - CIC does not use
- OCCUP - Occupation that will be performed in U.S. - CIC does not use
- ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use
- ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use
- ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use
- MATFLAG - Match flag - Match of arrival and departure records
- BIRYEAR - 4 digit year of birth
- DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use
- GENDER - Non-immigrant sex
- INSNUM - INS number
- AIRLINE - Airline used to arrive in U.S.
- ADMNUM - Admission Number
- FLTNO - Flight number of Airline used to arrive in U.S.
- VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S.

#### Steps
- Process and clean the **countries** data from i94cntyl.txt (all codes that are invalid are now mapped to "Other") and then save them to parquet files 
- Process and clean the **us_states** data from i94addrl.txt and then save them to parquet files 
- Process and clean the **us_ports** data from i94prtl.txt -  splitting city and state and then save them to parquet files 
- Process the mappings tables **visa type** (I94VISA.txt) and **arrival mode** (i94model.txt) that will be later used for joining with the **immigration_data**
- Process and clean **us_airports**, filtering only US airports, data splitting country and state from the iso_region column. The column airport_code is created by performing a union (so that we only get the distinct values) of the iata_codes and local_code. Saving the data to parquet files, partitioning them by airport_code, state
- Process and clean **us-cities-demographics**, and then save them to parquet files, partitioning them by state
- Process and clean **immigration_data**, get datetime from arrdate column value, all missing states or states that cannot be mapped to us_states are set to 99, get the arrival mode and the visa type from the mappings and then save them to 2 sets of parquet files, partitioning them by 1) year, month and us_state and 2) by year, month, arrival_mode,  and port

#### Purpose

The resulting tables:
![schema](./ImmigrationModel.PNG)
The purpose of the model is to analyze the immigration data and the connections between the arrival ports and ports and where the immigrants settle (which can be analyzed in connection to the us_cities_demographics).

## Addressing Other Scenarios
The write up describes a logical approach to this project under the following scenarios:

#### The data was increased by 100x.
We can use a cluster manager such as Yarn and increase the number of nodes depending on the needs.

#### The data populates a dashboard that must be updated on a daily basis by 7am every day.
In this case, the ETL process should be scheduled with Airflow (or a similar tool) and the immigration data set should be updated daily. Since the other datasets wouldn't get updated as often, not all the data transformations will run daily.

#### The database needed to be accessed by 100+ people.
We can use a cluster manager such as Yarn and increase the number of nodes depending on the needs.

## Defending Decisions

#### Clearly state the rationale for the choice of tools and technologies for the project. 
For the small files (mappings) transformations, pandas was used, as it provides easier methods than Spark for regex transformations. The dataframes were then converted to Spark dataframes.
The rest of the files were handled with Apache Spark, as it can manipulate multiple file formats (SAS included) and it provides fast and in-memory data processing. Spark SQL was used to process the the immigration dataset into a dataframe, along with the mappings (visa type and arrival mode) and manipulated via standard SQL join operations. The library [.sas7bdat](https://github.com/saurfang/spark-sas7bdat) was used for reading SAS data (.sas7bdat) with Spark. Schema is automatically inferred from metadata embedded in the SAS file. The data is saved to parquet files and sent to S3 so that is can be easily retrieved for further analysis.

#### Propose how often the data should be updated and why.
The immigration data should be updated monthly. However, if data needs to be analyzed more often (such as in the scenario where the dashboard that must be updated on a daily basis by 7am every day), the data should be updated daily. The other data sets can be updated on a quarterly basis, or more often, if necessary (new countries/ US airports/ US ports appear). The demographics data can be updated on a yearly basis - since the data is not pubicly available more often.
