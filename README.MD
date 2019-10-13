How to upload the sas data from the workspace to S3 directly? also is there any good ideas to read multiple sas files?
for directly uploading the data to S3, you can install CLI first on your workspace, https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html
zip -r /home/workspace/data.zip /data/18-83510-I94-Data-2016 to compress the data into a zip file in the workspace

And then download it directly to your local machine

Hi, I tried to use the notebook functionality in the EMR cluster, but was unable to load in the immigration SAS datasets. I have been getting the same error message after re-creating multiple EMR clusters: "An error occurred while calling o86.load. : java.lang.ClassNotFoundException: Failed to find data source: com.github.saurfang.sas.spark. Please find packages at http://spark.apache.org/third-party-projects.html"

My code is:

from pyspark.sql import SparkSession

spark = SparkSession.builder
.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")
.getOrCreate()

immigration_data_dir = 's3://udacity-capstone-project-12138/immigration-data/i94_feb16_sub.sas7bdat'

df_immigration = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data_dir)

Thank you!

SEPTEMBER 15, 2019
Yung-Chun L.

Mentor

4:47 PM

Hi @AoyiS , according to my knowledge, this package com.github.saurfang.sas.spark does not support directly read a directory.

You should try to read file one by one

Aoyi S.

6:40 PM

I was thinking about reading the whole directory (how the variable is named) but I actually only passed in one file, i94_feb16_sub.sas7bdat. Anything else that I might have done wrong? If the code is correct, is there any specific setting that I need to do when I create the EMR cluster in order to access this external package?

SEPTEMBER 16, 2019
Yung-Chun L.

Mentor

5:23 PM

I see. You should download the package, https://github.com/saurfang/spark-sas7bdat

https://databricks.com/blog/2015/07/28/using-3rd-party-libraries-in-databricks-apache-spark-packages-and-maven-libraries.html

SEPTEMBER 19, 2019
Jun W.

11:16 PM

I think you can connect to EMR EC2 (an EC2 instance starts with EMR clusters by default) through AWS CLI from your local computer. In that way, you can copy files to EC2, and run scripts there through AWS CLI. I haven't tried it. But I will. There is a blog on AWS about that, including some other services. You probably need to modify in order to use in this project.

/* I94YR - 4 digit year */

/* I94MON - Numeric month */
/* I94CIT & I94RES - This format shows all the valid and invalid codes for processing */
/* I94PORT - This format shows all the valid and invalid codes for processing */
/* ARRDATE is the Arrival Date in the USA. It is a SAS date numeric field that a 
   permament format has not been applied.  Please apply whichever date format 
   works for you. */
/* I94MODE - There are missing values as well as not reported (9) */
/* I94ADDR - There is lots of invalid codes in this variable and the list below 
   shows what we have found to be valid, everything else goes into 'other' */
/* DEPDATE is the Departure Date from the USA. It is a SAS date numeric field that 
   a permament format has not been applied.  Please apply whichever date format 
   works for you. */
/* I94BIR - Age of Respondent in Years */
/* I94VISA - Visa codes collapsed into three categories

/* COUNT - Used for summary statistics */


/* DTADFILE - Character Date Field - Date added to I-94 Files - CIC does not use */


/* VISAPOST - Department of State where where Visa was issued - CIC does not use */


/* OCCUP - Occupation that will be performed in U.S. - CIC does not use */


/* ENTDEPA - Arrival Flag - admitted or paroled into the U.S. - CIC does not use */


/* ENTDEPD - Departure Flag - Departed, lost I-94 or is deceased - CIC does not use */


/* ENTDEPU - Update Flag - Either apprehended, overstayed, adjusted to perm residence - CIC does not use */


/* MATFLAG - Match flag - Match of arrival and departure records */


/* BIRYEAR - 4 digit year of birth */


/* DTADDTO - Character Date Field - Date to which admitted to U.S. (allowed to stay until) - CIC does not use */


/* GENDER - Non-immigrant sex */


/* INSNUM - INS number */


/* AIRLINE - Airline used to arrive in U.S. */


/* ADMNUM - Admission Number */


/* FLTNO - Flight number of Airline used to arrive in U.S. */


/* VISATYPE - Class of admission legally admitting the non-immigrant to temporarily stay in U.S. */

Spark SAS Data Source (sas7bdat) - https://github.com/saurfang/spark-sas7bdat
A library for reading SAS data (.sas7bdat) with Spark.
Schema is automatically inferred from metadata embedded in the SAS file. (Behaviour can be customised, see parameters below)