####################
# This script has been used to find out new patterns in order to build the 
# fuzzy dictionary used in the context of the fuzzy name matching algorithm.
# it can be used to find additional patterns to add in the fuzzy name dictionary


#########################################################################################
###############                           Imports                        ################
#########################################################################################
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import Window
import pyspark.sql.functions as sf
from pyspark import SparkConf, SparkContext
import numpy as np
import pandas as pd
from meltfunctions import *
from Generalization.string_match_functions import *


#########################################################################################
###############                         Configuration                    ################
#########################################################################################
configuration_cluster = (
    SparkConf()
    .set("spark.executor.cores", "5")
    .set("spark.dynamicAllocation.maxExecutors", "20")
    .set("spark.executor.memory", "30g")
    .set("spark.driver.memory", "16g")
    .set("spark.driver.maxResultSize", "8g")
    .set("spark.sql.shuffle.partitions", "200")
    .set("spark.kryoserializer.buffer.max", "1g")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.network.timeout", "180000")
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")
)

#########################################################################################
#################                Launching the session                   ################
#########################################################################################

spark = (
    SparkSession.builder.appName("fuzzywuzzy")
    .config(conf=configuration_cluster)
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
)

#########################################################################################
#################              Functions for FuzzyWuzzy                  ################
#########################################################################################
from datetime import datetime
from math import ceil, pow
from difflib import SequenceMatcher, ndiff


def import_my_special_package(x):
    from difflib import SequenceMatcher, ndiff
    return x
 
int_rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
int_rdd.map(lambda x: import_my_special_package(x))
int_rdd.collect()
sqlContext = SQLContext(spark.sparkContext)

def ratio(s1, s2):
    m = SequenceMatcher(None, s1, s2)
    return (100 * m.ratio())
    
    

def similarity_function(a,b):

  sentence = ''.join([i[-1] for i in ndiff(b,a) if i[0] == '+'])
   
  return(sentence)


#TO SPARK
sqlContext.udf.register("ratios", ratio)
sqlContext.udf.register("similarity_function", similarity_function)


def fuzzywuzzy(safe_matches, attribute1, attribute2, similarity):
  
  if similarity == False:

    comparison1 = ["regexp_replace(", attribute1,',',attribute2,",'')"]
    comparison1 = ''.join(comparison1)
    
    comparison2 = ["regexp_replace(", attribute2,',',attribute1,",'')"]
    comparison2 = ''.join(comparison2)
    
    safe_matches = safe_matches.withColumn('diff1', sf.expr(comparison1))\
                               .withColumn('diff2', sf.expr(comparison2))\
                               .filter(((col('diff1') != '') & (col('diff2') !=  '')))
  
  else:
    
    expr_ratio = ["ratios(",attribute1,',', attribute2,")"]
    expr_ratio = ''.join(expr_ratio)
    
    simil_1 = ["similarity_function(",attribute1,',', attribute2,")"]
    simil_1 = ''.join(simil_1)
    
    simil_2 = ["similarity_function(",attribute2,',', attribute1,")"]
    simil_2 = ''.join(simil_2)
    
    safe_matches = safe_matches.filter(col(attribute1) != col(attribute2))\
                    .withColumn('distance',sf.levenshtein(col(attribute1), col(attribute2)))\
                    .withColumn('ratio', sf.expr(expr_ratio))\
                    .withColumn('diff1',sf.expr(simil_1))\
                    .withColumn('diff2',sf.expr(simil_2))\
                    .filter(((col('diff1') != '') & (col('diff2') !=  '')))
              
              
        
  pattern1 = safe_matches.groupBy('diff1').count()
  pattern1 = pattern1.orderBy(col('count'), ascending = False)

  pattern2 = safe_matches.groupBy('diff2').count()
  pattern2 = pattern2.orderBy(col('count'), ascending = False)

  patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

  patterns = patterns.groupBy('diff1').sum('count')
  patterns = patterns.orderBy(col('sum(count)'), ascending = False)

  return(safe_matches, patterns)



#############################################################################
#####                          LOADING DATA
#############################################################################

 # RIAD 
riad_original = spark.read.parquet("/data/corporate/riad_n_essential/riad_entty_flttnd_essntl_d_1/").filter(col('entty_riad_cd').isNotNull())
  
 # ORBIS
orbis_list_or = spark.read.parquet("/data/corporate/orbis/orbis_bvd_id_and_name/")
orbis_address_or = spark.read.parquet("/data/corporate/orbis/orbis_all_addresses/")


 # Official abbreviations
abbreviations = pd.read_csv('data/dictionary_abbreviations.csv')
abbreviations = spark.createDataFrame(abbreviations)

abbreviations = abbreviations.withColumn('abbr', removeSpaces(removePunctuation(convert_accent(col('abbr')))))\
                             .withColumn('descr', removeSpaces(removePunctuation(convert_accent(col('descr')))))
  
abbreviations.cache()
abbreviations.count()



 # Retrieve data matched with safe National ID 

final_matches = spark.read.parquet('/data/lab/prj_sds/db/riad_orbis_trad')


 # Attach entity names of both datasets

riad_names = riad_original.select('entty_riad_cd', 'nm_entty','strt', 'cty', 'pstl_cd').dropDuplicates()
orbis_names = orbis_list_or.select('bvdidnumber', 'name').dropDuplicates()
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



 # Filter country under analysis

riad_names = riad_names.filter(col('entty_riad_cd').like('AT%'))
orbis_names = orbis_names.filter(col('bvdidnumber').like('AT%'))
orbis_address = orbis_address.filter(col('bvdidnumber').like('AT%'))


# join tables of safe matches

safe_matches = final_matches.drop('country')\
                            .join(orbis_names, 'bvdidnumber' , 'inner' )\
                            .join(riad_names, 'entty_riad_cd', 'inner').drop('country')\
                            .join(orbis_address, 'bvdidnumber', 'inner')

      
safe_matches = safe_matches.withColumn('name_riad', removeSpaces(removePunctuation(convert_accent(col('nm_entty')))))\
                           .withColumn('name_orbis', removeSpaces(removePunctuation(convert_accent(col('name')))))\
                           .withColumn('country', sf.substring(col('entty_riad_cd'),1,2))\
                           .withColumn('add_riad',  countryRulesStreet(removeSpaces(removePunctuation(reorder_street(convert_accent(col('strt')))))))\
                           .withColumn('add_orbis', countryRulesStreet(removeSpaces(removePunctuation(reorder_street(convert_accent(col('street1')))))))\
                           .withColumn('city_riad',removeSpaces(removePunctuation(convert_accent(regexp_replace(col('cty'), '[^a-zA-Z\u00E0-\u00FC ]+', '')))))\
                           .withColumn('city_orbis',removeSpaces(removePunctuation(convert_accent(regexp_replace(col('city'), '[^a-zA-Z\u00E0-\u00FC ]+', '')))))\
                           .withColumn('postcode_riad',reorder_postcode(col('pstl_cd')))\
                           .withColumn('postcode_orbis',reorder_postcode(col('postcode')))\
                           .drop('nm_entty', 'name', 'strt', 'street1','cty','city','pstl_cd','postcode').dropDuplicates()

          
safe_matches = apply_dictionary(safe_matches,country = 'country',column_dict = 'name_riad', \
                                   dictionary = abbreviations, pattern = 'descr', \
                                   replacement = 'abbr',isocode = 'isocode' )

safe_matches = apply_dictionary(safe_matches, country ='country',column_dict = 'name_orbis', \
                                   dictionary = abbreviations, pattern = 'descr', \
                                   replacement = 'abbr',isocode = 'isocode' )



safe_matches.cache()
safe_matches.count()



 # Patterns on name

data, pattern = fuzzywuzzy(safe_matches, 'name_riad', 'name_orbis', similarity = True)

pattern.show()

data, pattern = fuzzywuzzy(safe_matches, 'name_riad', 'name_orbis', similarity = False)



    
    
    
    
    









 ############## SIMPLE PATTERNS ON NAMES

safe_matches = safe_matches.withColumn('diff1', sf.expr("regexp_replace(name_riad,name_orbis,'')"))\
                           .withColumn('diff2', sf.expr("regexp_replace(name_orbis,name_riad,'')"))\
                           .filter(((col('diff1') != '') & (col('diff2') !=  '')))
                           

#safe_matches = safe_matches.join(name_matches, 'entty_riad_cd', 'leftanti')

pattern1 = safe_matches.groupBy('diff1').count()
pattern1 = pattern1.orderBy(col('count'), ascending = False)

pattern2 = safe_matches.groupBy('diff2').count()
pattern2 = pattern2.orderBy(col('count'), ascending = False)

patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

patterns = patterns.groupBy('diff1').sum('count')
patterns = patterns.orderBy(col('sum(count)'), ascending = False)

patterns.show(30,truncate = False)


safe_matches.filter(col('diff1') == 'GELOESCHT').show(5, truncate = False)

    
    
    
    
simil = safe_matches.filter(col('name_riad') != col('name_orbis'))\
                    .withColumn('distance',sf.levenshtein(col('name_riad'), col('name_orbis')))\
                    .withColumn('ratio', sf.expr("ratios(name_riad, name_orbis)"))\
                    .withColumn('diff1',sf.expr("similarity_function(name_riad, name_orbis)"))\
                    .withColumn('diff2',sf.expr("similarity_function(name_orbis, name_riad)"))\
                    .filter(((col('diff1') != '') & (col('diff2') !=  '')))
  
  
simil.cache()
simil.count()

#simil.show()

pattern1 = simil.groupBy('diff1').count()
pattern1 = pattern1.orderBy(col('count'), ascending = False)

pattern2 = simil.groupBy('diff2').count()
pattern2 = pattern2.orderBy(col('count'), ascending = False)

patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

patterns = patterns.groupBy('diff1').sum('count')
patterns = patterns.orderBy(col('sum(count)'), ascending = False)

patterns.show(30,truncate = False)

 # find the abbreviation

simil.filter(col('diff2') == 'CCOMWTTGSELSFTB').show(5, truncate = False)



###### the same for address


# Attach entity names of both datasets

riad_add = riad_original.select('entty_riad_cd', 'strt').dropDuplicates()
orbis_add = orbis_contacts.select('bvdidnumber', col("streetnobuildingetcline1").alias('street')).dropDuplicates()


# Filter country under analysis

riad_add = riad_add.filter(col('entty_riad_cd').like('AT%'))
orbis_add = orbis_add.filter(col('bvdidnumber').like('AT%'))





# join tables of safe matches

safe_matches = final_matches.drop('country').join(orbis_add, 'bvdidnumber' , 'inner' ).join(riad_add, 'entty_riad_cd', 'inner').dropDuplicates().drop('country')


safe_matches = safe_matches.withColumn('add_riad',  countryRulesStreet(removeSpaces(removePunctuation(reorder_street(convert_accent(col('strt')))))))\
                           .withColumn('add_orbis', countryRulesStreet(removeSpaces(removePunctuation(reorder_street(convert_accent(col('street')))))))\
                           .withColumn('country', sf.substring(col('entty_riad_cd'),1,2))\
                           .drop('nm_entty', 'name').dropDuplicates()




safe_matches.cache()
safe_matches.count()




 ############## SIMPLE PATTERNS ON NAMES

safe_matches = safe_matches.withColumn('diff1', sf.expr("regexp_replace(add_riad,add_orbis,'')"))\
                           .withColumn('diff2', sf.expr("regexp_replace(add_orbis,add_riad,'')"))\
                           .filter(((col('diff1') != '') & (col('diff2') !=  '')))
                           

#safe_matches = safe_matches.join(name_matches, 'entty_riad_cd', 'leftanti')

pattern1 = safe_matches.groupBy('diff1').count()
pattern1 = pattern1.orderBy(col('count'), ascending = False)

pattern2 = safe_matches.groupBy('diff2').count()
pattern2 = pattern2.orderBy(col('count'), ascending = False)

patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

patterns = patterns.groupBy('diff1').sum('count')
patterns = patterns.orderBy(col('sum(count)'), ascending = False)

patterns.show(30,truncate = False)


safe_matches.filter(col('diff1') == 'SNC').show(5, truncate = False)





simil = safe_matches.filter(col('add_riad') != col('add_orbis'))\
                    .withColumn('distance',sf.levenshtein(col('add_riad'), col('add_orbis')))\
                    .withColumn('ratio', sf.expr("ratios(add_riad, add_orbis)"))\
                    .withColumn('diff1',sf.expr("similarity_function(add_riad, add_orbis)"))\
                    .withColumn('diff2',sf.expr("similarity_function(add_orbis, add_riad)"))\
                    .filter(((col('diff1') != '') & (col('diff2') !=  '')))
  
  
simil.cache()
simil.count()

#simil.show()

pattern1 = simil.groupBy('diff1').count()
pattern1 = pattern1.orderBy(col('count'), ascending = False)

pattern2 = simil.groupBy('diff2').count()
pattern2 = pattern2.orderBy(col('count'), ascending = False)

patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

patterns = patterns.groupBy('diff1').sum('count')
patterns = patterns.orderBy(col('sum(count)'), ascending = False)

patterns.show(30,truncate = False)




 # find the abbreviation

simil.filter(col('diff1') == '2').show(5, truncate = False)











########################################
remaining = riad_original.join(final_matches,'entty_riad_cd', 'leftanti')\
                         .select('nm_entty', 'cntry', 'entty_riad_cd', 'strt', 'pstl_cd', 'cty')\
                         .dropDuplicates()

remaining.cache()
remaining.count()

# Filter country under analysis

remaining = remaining.filter(col('cntry').like('IT'))

remaining = data_preparation(remaining,  identifier = 'entty_riad_cd', \
                                   name_entity = 'nm_entty', address = 'strt', \
                                   city = 'cty', postal_code = 'pstl_cd', country = 'cntry')


orbis_name_match = data_preparation(orbis_name_and_address, identifier = 'bvdidnumber', \
                                   name_entity = 'name', address = 'street1', \
                                   city = 'city', postal_code = 'postcode', country = 'country')


raw_match = stringMatching(remaining, 'entty_riad_cd', orbis_name_match, 'bvdidnumber')

raw_match.cache()
raw_match.count()



safe_matches = raw_match.drop('country').join(orbis_names, 'bvdidnumber' , 'inner' ).join(riad_names, 'entty_riad_cd', 'inner').dropDuplicates().drop('country')

safe_matches = safe_matches.withColumn('name_riad', removeSpaces(removePunctuation(convert_accent(col('nm_entty')))))\
                           .withColumn('name_orbis', removeSpaces(removePunctuation(convert_accent(col('name_internat')))))\
                           .drop('nm_entty', 'name').dropDuplicates()

simil = safe_matches.filter(col('name_riad') != col('name_orbis'))\
                    .withColumn('name_riad', italy_rules(col('name_riad')))\
                    .withColumn('name_orbis', italy_rules(col('name_orbis')))\
                    .withColumn('distance',sf.levenshtein(col('name_riad'), col('name_orbis')))\
                    .withColumn('ratio', sf.expr("ratios(name_riad, name_orbis)"))\
                    .withColumn('diff1',sf.expr("similarity_function(name_riad, name_orbis)"))\
                    .withColumn('diff2',sf.expr("similarity_function(name_orbis, name_riad)"))\
                    .filter(((col('diff1') != '') & (col('diff2') !=  '')))
  
  
simil.cache()
simil.count()

#simil.show()

pattern1 = simil.groupBy('diff1').count()
pattern1 = pattern1.orderBy(col('count'), ascending = False)

pattern2 = simil.groupBy('diff2').count()
pattern2 = pattern2.orderBy(col('count'), ascending = False)

patterns = pattern1.unionByName(pattern2.withColumnRenamed('diff2', 'diff1'))

patterns = patterns.groupBy('diff1').sum('count')
patterns = patterns.orderBy(col('sum(count)'), ascending = False)

patterns.show(30,truncate = False)