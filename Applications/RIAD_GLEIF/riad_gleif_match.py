# File: matching_tool.py
# Last saved: 26 November 2021
# Version: 2.0

#---------------------------------------
#    Launching the PySpark session                     
#---------------------------------------

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark_matching import *

configuration_cluster = (
    SparkConf()
    .set("spark.executor.cores", "4")
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

spark = (
    SparkSession.builder.appName("riad_gleif_matching")
    .config(conf=configuration_cluster)
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
)

  #-------------------------
  # Loading the two datasets
  #-------------------------
  
 #------------
 # RIAD 
 #------------

# Load data and select all the relevant attributes
dataset1_original = spark.read.parquet("/data/corporate/riad_n_essential/riad_entty_flttnd_essntl_d_1/")\
                         .select('entty_riad_cd', 'nm_entty', 'cntry', 'strt', 'cty', 'pstl_cd').dropDuplicates()
  
 # RIAD attribute names
 # - address, city and postal code are optional attributes
 # - if an optional attribute is not available, write NA in both datasets

id_data1 = 'entty_riad_cd' 
name_data1 = 'nm_entty'
country_1 = 'cntry'
street_data1 = 'strt'
city_data1 = 'cty'
pstl_data1 = 'pstl_cd'


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
dataset2_original = spark.read.parquet("/data/corporate/gleif/gleif_lei2_cdf_public_records/")\
                         .select('lei', 'legal_name', 'la_country', 'la_first_address_line', 'la_city', 'la_postal_code', 'transliterated_name')\
                         .withColumn('legal_name', sf.when(col('transliterated_name').isNotNull(), col('transliterated_name')).otherwise(col('legal_name')))\
                         .drop('transliterated_name').dropDuplicates()

  
 # GLEIF attribute names

id_data2 = 'lei'
name_data2 = 'legal_name'
country_2 = 'la_country'
street_data2 = 'la_first_address_line'
city_data2 = 'la_city'
pstl_data2 = 'la_postal_code'


 # Specify which type of country attribute you have. 
country_attribute2 = 'isocode2'


#----------------------------
# Use of Fuzzy-name dictionary
#----------------------------

fuzzy_wuzzy = True 
fuzzy_levels = 2 #possible values = 1,2,3
  
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

save_table = True # if you don't want to save the table modify with False
disc_lab = 'lab_prj_dcaap_open' # only data lab allowed, no corporate store
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
                                 save_table, disc_lab, table_name)




spark.stop()
