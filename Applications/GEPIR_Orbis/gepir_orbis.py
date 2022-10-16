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
    SparkSession.builder.appName("gepir_orbis")
    .config(conf=configuration_cluster)
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
)

  #-------------------------
  # Loading the two datasets
  #-------------------------
  
 
 #------------
 # GEPIR 
 #------------

gepir_original = pd.read_csv('Applications/GEPIR_Orbis/GEPIR_Firm_Info_to_match.csv')[['name', 'street', 'postcode', 'city', 'isocode', 'company_prefix']]
gepir_original = gepir_original.drop_duplicates()
gepir_original[['name', 'street', 'postcode', 'city', 'isocode', 'company_prefix']] = gepir_original[['name', 'street', 'postcode', 'city', 'isocode', 'company_prefix']].astype(str)
gepir_original = spark.createDataFrame(gepir_original)

dataset1_original = gepir_original
  
 # GEPIR attribute names

id_data1 = 'company_prefix'
name_data1 = 'name'
street_data1 = 'street'
city_data1 = 'city'
pstl_data1 = 'postcode'
country_1 = 'isocode'


 # Specify which type of country attribute you have. It can differ between the 2 datasets 
 # Possible values: 
 # - 'country_name': if you have the extended name of the country 
 # - 'isocode2': if you have the isocode with 2 digits
 # - 'isocode3': if you have the iscode with 3 digits

country_attribute = 'isocode3'

  #------------
  # ORBIS
  #------------
orbis_list_or = spark.read.parquet("/data/corporate/orbis/orbis_bvd_id_and_name/")
orbis_address_or = spark.read.parquet("/data/corporate/orbis/orbis_all_addresses/")

#---------------------------------------------------------------------
# Join Orbis tables to retrieve all needed attributes in the same table
#----------------------------------------------------------------------

orbis_names = orbis_list_or.withColumn('country', sf.substring(col('bvdidnumber'),1,2))\
                           .select('name', 'bvdidnumber', 'country')\
                           .dropDuplicates()


  # Union the address fields to get a unique address attribute
  
orbis_address = orbis_address_or.select('bvdidnumber', 'postcode', \
                                       col('streetnobuildingetcline1').alias('street1'),\
                                       col('streetnobuildingetcline2').alias('street2'),\
                                       col('streetnobuildingetcline3').alias('street3'),\
                                       col('streetnobuildingetcline4').alias('street4'),\
                                      'city')

orbis_address = orbis_address.withColumn('street3', sf.when(col('street4').isNotNull(), sf.concat(col('street3'), sf.lit(' '), col('street4'))).otherwise(col('street3')))\
                             .withColumn('street2', sf.when(col('street3').isNotNull(), sf.concat(col('street2'), sf.lit(' '), col('street3'))).otherwise(col('street2')))\
                             .withColumn('street1', sf.when(col('street2').isNotNull(), sf.concat(col('street1'), sf.lit(' '), col('street2'))).otherwise(col('street1')))\
                             .drop('street2', 'street3', 'street4')\
                             .dropDuplicates()

dataset2_original = orbis_names.join(orbis_address, ['bvdidnumber'], 'inner')\
                               .filter(col('country') == 'DE')\
                               .select('bvdidnumber', 'name', 'street1', 'city', 'postcode', 'country').dropDuplicates()

dataset2_original.cache()
dataset2_original.count()


 # ORBIS attribute names

id_data2 = 'bvdidnumber'
name_data2 = 'name'
street_data2 = 'street1'
city_data2 = 'city'
pstl_data2 = 'postcode'
country_2 = 'country'


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

add_column_score = False

#--------------------------------------
# Define path to save the mapping table
#--------------------------------------

save_table = True # if you don't want to save the table modify with False
disc_lab = 'lab_prj_dcaap_open' # only data lab allowed, no corporate store
table_name = 'gepir_orbis_mapping' # name of the table
  
  
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
