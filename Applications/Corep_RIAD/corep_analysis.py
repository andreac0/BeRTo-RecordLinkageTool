
#--------------------
# Imports
#--------------------

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql import Window
import pyspark.sql.functions as sf
import numpy as np
import pandas as pd
from pyspark_matching import *
import subprocess

# Initialize Spark Session

spark = (
    SparkSession.builder.appName("corep_analysis")
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()
)

  #------------------------------------------------------------
  # Load Corep
  #------------------------------------------------------------
  
from connectors import disc 
# as we have access only to a view, we can't read the parquet files directly but we have to go through the connectors module

query ='''SELECT DISTINCT reported_period, entity_id, name, riad_code, c06.lec as lec, lgs, entity_type,
                       c06.c_0602_c0010 as sub_name, c06.entty_c0602_type_descr as sub_type, 
                       c06.c_0602_c0050 as sub_cntry, if(c06.c_0602_c0025 is not null, c06.c_0602_c0025, c06.lgs) AS sub_lei, 
                       c_0602_c0060 as share
                FROM(
                  SELECT *, if(c_0602_c0035 is not null, c_0602_c0035, c_0602_rx15_c0035) as entty_c0602_type_descr
                  FROM (SELECT reported_period, name, entity_id, riad_code, lgs, lec, entity_type, c_0602_c0010, 
                         c_0602_c0025, c_0602_c0035, c_0602_rx15_c0035, c_0602_c0050, c_0602_c0060
                              FROM crp_suba.suba_c_0602) as corep_table
                ) AS c06
                WHERE reported_period < '2021-06%'

                UNION

                SELECT reported_period, entity_id, name, riad_code, lec as lec, lgs, entity_type,
                       sub_name, sub_type, sub_cntry,
                       if(sub_lei is not null, sub_lei, lgs) AS sub_lei, share
                FROM(
                  SELECT DISTINCT reported_period, entity_id, name, riad_code, lec as lec, entity_type, lgs,
                         coalesce(c_0602_rx15_c0011, c_0602_rx16_c0011) as sub_name, 
                         coalesce(c_0602_rx15_c0027, c_0602_rx16_c0027) as sub_lei,
                       coalesce(c_0602_rx15_c0050, c_0602_rx16_c0050) as sub_cntry,
                       coalesce(c_0602_rx15_c0035, c_0602_rx16_c0035) as sub_type,
                       coalesce(c_0602_rx15_c0060, c_0602_rx16_c0060)  as share
                  FROM crp_suba.suba_c_0602 
                  WHERE reported_period >= '2021-06%'
                ) AS c062'''

corep = disc.read_sql(query)

  # Replacing None and empty values with real null values
  
corep = spark.createDataFrame(corep)\
        .withColumn('reported_period',sf.substring(col('reported_period'),1,10))\
        .withColumn('lec',sf.when(col('lec') == 'None',None).otherwise(col('lec')))\
        .withColumn('lec',sf.when(col('lec') == '',None).otherwise(col('lec')))\
        .withColumn('lgs',sf.when(col('lgs') == 'None',None).otherwise(col('lgs')))\
        .withColumn('lgs',sf.when(col('lgs') == '',None).otherwise(col('lgs')))\
        .withColumn('entity_type',sf.when(col('entity_type') == 'None',None).otherwise(col('entity_type')))\
        .withColumn('entity_type',sf.when(col('entity_type') == '',None).otherwise(col('entity_type')))\
        .withColumn('sub_name',sf.when(col('sub_name') == 'None',None).otherwise(col('sub_name')))\
        .withColumn('sub_name',sf.when(col('sub_name') == '',None).otherwise(col('sub_name')))\
        .withColumn('sub_lei',sf.when(col('sub_lei') == 'None',None).otherwise(col('sub_lei')))\
        .withColumn('sub_lei',sf.when(col('sub_lei') == '',None).otherwise(col('sub_lei')))\
        .withColumn('sub_type',sf.when(col('sub_type') == 'None',None).otherwise(col('sub_type')))\
        .withColumn('sub_type',sf.when(col('sub_type') == '',None).otherwise(col('sub_type')))\
        .withColumn('sub_cntry',sf.when(col('sub_cntry') == 'None',None).otherwise(col('sub_cntry')))\
        .withColumn('sub_cntry',sf.when(col('sub_cntry') == '',None).otherwise(col('sub_cntry')))\
        .withColumn('share',sf.when(col('share') == 'None',None).otherwise(col('share')))\
        .withColumn('share',sf.when(col('share') == 'nan',None).otherwise(col('share')))\
        .withColumn('share',sf.when(col('share') == '',None).otherwise(col('share')))\
        .dropDuplicates()
    
    
    # Cleaning of country and replacing of lei with lgs: the latter has to be repeated as
    # in the original table the field for LEI sometimes it is empty (not a null value) = the initial query will keep the empty values
    
corep = corep.withColumn('sub_cntry', removeSpaces(removePunctuation(convert_accent(col('sub_cntry')))))\
             .withColumn('sub_lei', sf.when(col('sub_lei').isNotNull(), col('sub_lei')).otherwise(col('lgs')))\
             .drop('lgs')\
             .dropDuplicates()
    
corep.cache()
corep.count()


  # Replacing country name with corresponding Isocode = needed in all next steps (RIAD has isocode2 countries)

isocodes = pd.read_csv('data/isocodes.csv')[['name', 'alpha-2', 'alpha-3']]
isocodes[['name', 'alpha-2', 'alpha-3']] = isocodes[['name', 'alpha-2', 'alpha-3']].astype(str)
isocodes = spark.createDataFrame(isocodes)

isocodes = isocodes.withColumn('name', removeSpaces(removePunctuation(convert_accent(col('name')))))\
                   .withColumnRenamed('name', 'country_name')\
                   .withColumnRenamed('alpha-2', 'isocode2')\
                   .withColumnRenamed('alpha-3', 'isocode3')

isocodes.cache()
isocodes.count()
  
corep = corep.join(isocodes, [corep.sub_cntry == isocodes.country_name], 'left')\
                                     .drop('country','country_name', 'isocode3')
  
corep.cache()
corep.count()

#-------------------------------------------------------
  
    # Some statistics on the original table
    
  # Load GLEIF (it should contain all LEIs worldwide)
gleif = spark.read.parquet("/data/corporate/gleif/gleif_lei2_cdf_public_records/")\
                         .select('lei').dropDuplicates()    
    
print(corep.count()) 
# number of obs: 163.125

print(corep.join(gleif, corep.sub_lei == gleif.lei ,'inner').count()) 
#number of LEIs available overall: 74.032

print(corep.filter(col('sub_lei').isNull()).count()) 
# 69.550 sub_lei are null values

 # what happens when sub_lei is not null and neither an LEI?
corep.filter(col('sub_lei').isNotNull()).filter(sf.length(col('sub_lei')) != 20).show()
  
  
  #####################################
  # First step: convert LEIs into RIAD codes, when available in RIAD
  #####################################
  
riad =  spark.read.parquet("/data/corporate/riad_n_essential/riad_entty_flttnd_essntl_d_1/")\
             .filter(col('entty_riad_cd').isNotNull())\
             .select('entty_riad_cd', 'lei')\
             .dropDuplicates()

riad.cache()
riad.count()

corep = corep.join(riad, [corep.sub_lei == riad.lei], 'left').drop('lei').dropDuplicates()

corep.cache()
corep.count()

print(corep.filter(col('entty_riad_cd').isNotNull()).count())  
# 66.000 obs can be translated with RIAD
  
  
  
 ###################################
 #Second step: Add RIAD codes from RIAD - GLEIF identifier and name matching
 ###################################

identifier_matchings = spark.sql('''
        SELECT DISTINCT
            ent.entty_riad_cd, 
            gleif.lei AS gleif_lei

        FROM 
            `crp_riad_n_essential`.`riad_idntfrs_snpsht_d_1` AS cd 
            JOIN crp_riad_n_essential.riad_entty_flttnd_essntl_d_1 AS ent 
                ON cd.entty_riad_id = ent.entty_riad_id
            INNER JOIN  `crp_gleif`.`v_gleif_lei2_cdf_public_records_current` AS gleif
                ON gleif.reg_authority_entity_id = cd.entty_cd 
                    AND gleif.la_country = ent.cntry
            WHERE ent.vld_t = '9999-12-31'
                AND cd.vld_t = '9999-12-31'
                AND cd.typ_entty_cd NOT IN ('RIAD','NCB','UCDB','JST_CD','LQSG_CD','MDG_IBSI_CD','MDG_IMIR_CD','TLTRO_CD')
                AND (ent.lei IS NULL
                    OR ent.lei <> gleif.lei
                    )''')


identifier_matchings.cache()
identifier_matchings.count()


name_matchings =  spark.read.parquet("/data/lab/prj_dcaap_open/db/riad_gleif_mapping/")\
             .select('entty_riad_cd', 'lei')\
             .dropDuplicates()
      
      # Union the two
add_riad = identifier_matchings.withColumnRenamed('gleif_lei', 'lei')\
                               .filter(col('lei').isNotNull())\
                               .unionByName(name_matchings).dropDuplicates()\
                               .withColumnRenamed('entty_riad_cd', 'add_riad')
      
add_riad.cache()
add_riad.count()
    

corep = corep.join(add_riad, [corep.sub_lei == add_riad.lei], 'left').drop('lei').dropDuplicates()\
             .withColumn('entty_riad_cd', sf.when(col('entty_riad_cd').isNull(), col('add_riad')).otherwise(col('entty_riad_cd')))\
             .drop('add_riad').dropDuplicates()


corep.cache()
corep.count()

print(corep.select('sub_name').count()) 
#163.311 names available

print(corep.filter(col('entty_riad_cd').isNotNull()).count())  
#67.320 obs can be translated with RIAD codes



 ###################################
 # Third step: add RIAD code from COREP - RIAD name/country-only matching
 ###################################
  
name_matches = spark.read.parquet("/data/lab/prj_dcaap_open/db/riad_corep_mapping/") 
  # table obtained from the name_matching tool

name_matches.cache()
name_matches.cache()

#print(name_matches.count())
#print(name_matches.select('id_name').dropDuplicates().count())
#print(name_matches.select('entty_riad_cd').dropDuplicates().count())

  # the name matching is based only on country and name of the entities. To increase precision,
  # we can filter out the names which are linked to more than one riad codes: we get a set of 
  # 1:1 correspondence between names of entities and RIAD codes
  
groups = name_matches.groupBy('id_name', 'country').agg(sf.countDistinct('entty_riad_cd').alias('n'))\
                     .filter(col('n') == 1).drop('n').dropDuplicates()
  
name_matches = name_matches.join(groups, ['id_name', 'country'], 'inner')

name_matches.cache()
name_matches.count()
print(name_matches.count())
print(name_matches.select('id_name', 'country').dropDuplicates().count())
print(name_matches.select('entty_riad_cd').dropDuplicates().count())


  # Add new RIAD codes
name_matches = name_matches.withColumnRenamed('entty_riad_cd', 'add_riad')

corep = corep.join(name_matches, [corep.sub_name == name_matches.id_name,\
                                   corep.isocode2 == name_matches.country], 'left').drop('id_name').dropDuplicates()\
             .withColumn('entty_riad_cd', sf.when(col('entty_riad_cd').isNull(), col('add_riad')).otherwise(col('entty_riad_cd')))\
             .drop('add_riad').dropDuplicates()
    
    
print(corep.select('sub_name').count()) #163.311 names available
print(corep.filter(col('entty_riad_cd').isNotNull()).count())  #108.872 obs can be translated with RIAD codes


  # Fixing date to be consistent with AnaCredit

corep = corep.withColumn('sub_lei', sf.when(col('sub_lei').isNotNull(), col('sub_lei')))\
               .withColumnRenamed('entty_riad_cd', 'sub_riad')\
               .drop('country')\
               .withColumn('reported_period', sf.concat(sf.substring(col('reported_period'),1,4),sf.substring(col('reported_period'),6,2)))

    
    
    


  # Write table
#corep.write.format("parquet").mode('overwrite')\
#                .option("path", "/data/lab/prj_sds/db/corep_riad")\
#                .saveAsTable("lab_prj_sds.corep_riad")
#
## Invlidate the metadata
#cmd = "impala-shell -i $IMPALA_HOST:21000 -k --ssl -q 'INVALIDATE METADATA {0}.{1}'".format(
#            'lab_prj_sds', 'corep_riad'
#        )
#process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
#output = process.communicate()






  #Looking at one period only
#one_month = corep.filter(col('reported_period') == '202012')
#one_month.cache()
#one_month.count()
#
#print(one_month.select('sub_riad').dropDuplicates().count())
#print(one_month.select('sub_name').dropDuplicates().count())
#
#
#print(one_month.filter(col('sub_riad').isNotNull()).count())
#print(one_month.join(gleif, one_month.sub_lei == gleif.lei ,'inner').count())
#print(one_month.join(riad, one_month.sub_lei == riad.lei ,'inner').count())
#print(one_month.select('sub_name').count())
#
#
#print(one_month.filter(col('riad_code') == 'DE00001').filter(col('sub_riad').isNotNull()).count())
#print(one_month.filter(col('riad_code') == 'DE00001').filter(col('sub_lei').isNotNull()).count())
#print(one_month.filter(col('riad_code') == 'DE00001').count())


spark.stop()
