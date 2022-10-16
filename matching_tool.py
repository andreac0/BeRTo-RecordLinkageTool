# File: matching_tool.py
# Last saved: 07 February 2022
# Version: 2.0

#---------------------------------------
#    Launching the PySpark session                     
#---------------------------------------
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark_matching import *

configuration_cluster = (
    SparkConf()
    .set("spark.executor.cores", "3")
    .set("spark.dynamicAllocation.maxExecutors", "20")
    .set("spark.executor.memory", "20g")
    .set("spark.driver.memory", "16g")
    .set("spark.driver.maxResultSize", "8g")
    .set("spark.sql.shuffle.partitions", "200")
    .set("spark.kryoserializer.buffer.max", "1g")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.network.timeout", "180000")
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")
)

#Create SparkSession
spark = (
    SparkSession.builder.appName("name_m")
    .config(conf=configuration_cluster)
    .getOrCreate()
)


  #-------------------------
  # Loading the two datasets
  #-------------------------
  
# Load data and select all the relevant attributes
dataset1_original = spark.read.csv("data/mfi.csv", header=True, inferSchema=True, sep =';')\
                         .select("RIAD_CODE", "NAME", col("COUNTRY_OF_REGISTRATION").alias("COUNTRY"), "ADDRESS", "POSTAL", "CITY")\
                         .filter(col("COUNTRY") == 'IT')
dataset1_original.cache()
dataset1_original.count()

id_data1 = 'RIAD_CODE' 
name_data1 = 'NAME'
country_1 = 'COUNTRY'
street_data1 = 'ADDRESS'
city_data1 = 'CITY'
pstl_data1 = 'POSTAL'


 # Specify which type of country attribute you have. It can differ between the 2 datasets 
 # Possible values: 
 # - 'country_name': if you have the extended name of the country 
 # - 'isocode2': if you have the isocode with 2 digits
 # - 'isocode3': if you have the iscode with 3 digits

country_attribute = 'isocode2'


 #-------------
 # GLEIF
 #-------------
# Load data and select all the relevant attributes
dataset2_original = spark.read.csv("data/gleif_it.csv", header=True)
dataset2_original.cache()
dataset2_original.count()
  
 # GLEIF attribute names
id_data2 = 'LEI'
name_data2 = 'Name'
country_2 = 'Country'
street_data2 = 'Address'
city_data2 = 'City'
pstl_data2 = 'Postcode'


 # Specify which type of country attribute you have. 
country_attribute2 = 'isocode2'


#----------------------------
# Use of Fuzzy-name dictionary
#----------------------------

fuzzy_wuzzy = True 
fuzzy_levels = 3 #possible values = 1,2,3
  
  #If true the fuzzywuzzy dictionary will be applied to the names of the entities
  #Rules of thumb:
  # - use it when the tool is used in its complete version (all attributes)
  # - the higher the level, the less precise and computationally more inefficient the tool will be 
  #   but you may get more matchings (suggested value = 1)
  
#----------------------------
# Hyperparameters of address similarity
#----------------------------
  
  # Use similarity on address
address_similarity = True

  # If you accept differences on the address you can modifiy the following:
  
first_split = 10   # Length of the address to create the first group
diff_group1 = 4    # Dissimilarities accepted for the first group

second_split = 20  # Length of the address to create the second and third groups
diff_group2 = 7    # Dissimilarities accepted for the second group 
diff_group3 = 9    # Dissimilarities accepted for the third group, with length(address) > second split


#-------------------------------------
# Add a column with a similarity score
#-------------------------------------

add_column_score = True

#--------------------------------------
# Define path to save the mapping table
#--------------------------------------

save_table = False # if you don't want to save the table modify with False
path = 'data/' 
table_name = 'riad_gleif_mapping' # name of the table


#------------------------
# --> Run the tool
#------------------------

name_matches = fuzzyNameMatching(spark,
                                 dataset1_original, id_data1, name_data1, country_1, street_data1, city_data1, pstl_data1,country_attribute,
                                 dataset2_original, id_data2, name_data2, country_2, street_data2, city_data2, pstl_data2,country_attribute2,
                                 fuzzy_wuzzy, fuzzy_levels,
                                 address_similarity, first_split, diff_group1, second_split, diff_group2, diff_group3,
                                 add_column_score,
                                 save_table, path, table_name)

n = name_matches.toPandas()
n.to_excel('matches_spark.xlsx')

spark.stop()
