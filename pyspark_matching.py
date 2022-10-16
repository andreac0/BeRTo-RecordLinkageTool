# Version: 2.0
##################################################
# This script contains all the functions needed to 
# generate a mapping table of ID between two datasets 
# based on names and addresses
#################################################


import numpy as np
import pandas as pd
import subprocess
from pyspark.sql.functions import regexp_replace, trim, col, upper
from pyspark.sql.functions import array, col, explode, lit, struct
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.types import *
from pyspark.sql import Window


def removePunctuation(column):

  """
  Removes punctuation, changes to upper case, and strips leading and 
  trailing spaces.

  Note:
    Only spaces, letters, and numbers should be retained. Other characters 
    should should be eliminated (e.g. it's becomes its). Leading and 
    trailing spaces should be removed after punctuation is removed.

  """
  return trim(sf.upper(regexp_replace(column, '([^\s\w_]|_)+', '')))



def removeSpaces(column):
  
  '''
  Remove whitespaces
  ''' 
  
  return trim(regexp_replace(column, ' ', ''))



def convert_accent(column):
  
  '''
  This function convert and replace all special/accented letters to standard alphabet
  '''
  
         
  return(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace (regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace (regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(\
         column,'ž','z'),'š','s'),'ê','e'),'è','e'),'æ','a'),'č','e'),'ý','y'),'ăă','co'),'ę','e'\
         ),'ő','o'),'ć','ae'),'ń','n'),'ă','a'),'ß','ss'),'ř','oe'),'ë','e'),'ú','u'),'õ','o'),'ó','o'),\
        'ô','o'),'ã','a'),'ç','c'),'ñ','n'),'ü','ue'),'ä','ae'),'ö','oe'),'é','e') ,'æ','a') ,'ø','oe') ,'á','a'),'í','i')\
         ,'å','aa'), 'à', 'a'), 'ì', 'i'),'Ž','Z'),'Š','S'),'Ê','E'),'È','E'),'Æ','A'),'Č','E'),'Ý','Y'),'ĂĂ','CO'),'Ę','E'\
         ),'Ő','O'),'Ć','AE'),'Ń','N'),'Ă','A'),'ß','SS'),'Ř','OE'),'Ë','E'),'Ú','U'),'Õ','O'),'Ó','O'),\
        'Ô','O'),'Ã','A'),'Ç','C'),'Ñ','N'),'Ü','UE'),'Ä','AE'),'Ö','OE'),'É','E') ,'Æ','A') ,'Ø','OE') ,'Á','A'),'Í','I')\
         ,'Å','AA'), 'À', 'A'), 'Ì', 'I'))



def reorder_street(column):
  
  '''
  This function re-order the street in order to get a standardized format for the address: 
  house number + street name
  '''
  
  return(sf.concat(sf.regexp_extract(regexp_replace(sf.upper(column),'[^a-zA-Z0-9\u00E0-\u00FC ]+', ''),\
                                     '[0-9]+', 0), sf.lit(' '), regexp_replace(regexp_replace(sf.upper(column),\
                                      '[^a-zA-Z0-9\u00E0-\u00FC ]+', ''), '[0-9]', '')))



def reorder_postcode(column):
  '''
  This function re-order the postal code, removing whitespaces and using a standardized format: letters + numbers
  '''

  return(sf.concat(regexp_replace(regexp_replace(sf.upper(column), ' ', ''), '[^a-zA-Z\u00E0-\u00FC ]+', ''),\
                  regexp_replace(regexp_replace(sf.upper(column), ' ', ''),'[^0-9\u00E0-\u00FC ]+', '')))
  
  
  
def countryRulesStreet(street):
    '''
    This function provides a list of common rules and patterns about address abbreviations and
    replace these words with the latter.
  
    '''
    
    new = regexp_replace(street,'STRASSE', 'STR')
    new = regexp_replace(new,'PLATZ', 'PL')
    new = regexp_replace(new,'GASSE', 'G')
    new = regexp_replace(new,'HAUSNUMMER', '')
    new = regexp_replace(new,'HAUSNR', '')
    
    new = regexp_replace(new,'STREET', 'ST')
    new = regexp_replace(new,'ROAD', 'RD')
    new = regexp_replace(new,'SQUARE', 'SQ')
    new = regexp_replace(new,'STATION', 'STA')
    new = regexp_replace(new,'TERRACE', 'TERR')
    new = regexp_replace(new,'BRIDGE', 'BRG')
    new = regexp_replace(new,'CAPE', 'CPE')
    new = regexp_replace(new,'CENTER','CTR')
    new = regexp_replace(new,'CENTRE', 'CTR')
    new = regexp_replace(new,'CORNER', 'COR')
    new = regexp_replace(new,'COURT', 'CT')
    new = regexp_replace(new,'CROSSING', 'XING')
    new = regexp_replace(new,'ESTATE', 'EST')
    new = regexp_replace(new,'FIELD', 'FLD')
    new = regexp_replace(new,'HIGHWAY', 'HWY')
    new = regexp_replace(new,'HILL', 'HL')
    new = regexp_replace(new,'ISLAND', 'IS')
    new = regexp_replace(new,'LAKE', 'LK')
    new = regexp_replace(new,'MEADOW', 'MDW')
    new = regexp_replace(new,'MOTORWAY', 'MTWY')
    new = regexp_replace(new,'PARKWAY', 'PKWY')
    new = regexp_replace(new,'PASSAGE', 'PSGE')
    new = regexp_replace(new,'PLAZA', 'PLZ')
    
    
    new = regexp_replace(new,'BOULEVARD', 'BLVD')
    new = regexp_replace(new,'BOULV', 'BLVD')
    new = regexp_replace(new,'AVENUE',	'AVE')
    new = regexp_replace(new,'AVENU',	'AVE')
    new = regexp_replace(new,'PLACE', 'PL')    
    
    new = regexp_replace(new,'PIAZZA', 'PZA')
    new = regexp_replace(new,'CORSO', 'CSO')
    new = regexp_replace(new,'FRATELLI', 'FLLI')
    new = regexp_replace(new,'LARGO', 'LGO')
    new = regexp_replace(new,'VIALE', 'VLE')
    new = regexp_replace(new,'STRADALE', 'STR')
    new = regexp_replace(new,'NUMERO', 'N')
    
    new = regexp_replace(new,'CALLE', 'CL')
    new = regexp_replace(new,'AVENIDA', 'AV')
    new = regexp_replace(new,'AUTOPISTA', 'AU')
    new = regexp_replace(new,'CARRETERA', 'CT')
    new = regexp_replace(new,'BARRIO', 'BR')
    new = regexp_replace(new,'EDIFICIO', 'ED')
    new = regexp_replace(new,'CASA', 'CS')


    return(new)  
  


def data_preparation(dataset, identifier, name_entity, address, city, postal_code, country):
  '''
  This function clean and uniform the attributes that will be used for the matching based
  on the name and address
  '''
  
  dataset_cleaned = dataset.filter(col(identifier).isNotNull())\
                           .filter(col(name_entity).isNotNull())\
                           .filter(col(country).isNotNull())\
                           .withColumn('name',removeSpaces(removePunctuation(convert_accent(col(name_entity)))))\
                           .filter(col('name') != '')\
                           .withColumnRenamed(country, 'country')\
                           .withColumn('country',removeSpaces(removePunctuation(convert_accent(col('country')))))
  
  
  if address != 'NA':
    
    dataset_cleaned = dataset_cleaned.filter(col(address).isNotNull())\
                                     .withColumn('street',countryRulesStreet(removeSpaces(removePunctuation(reorder_street(convert_accent(address))))))
                            
  else:
    
    dataset_cleaned = dataset_cleaned.withColumn('street', sf.lit('NA'))
  
  if city != 'NA':
    
    dataset_cleaned = dataset_cleaned.withColumn('city',removeSpaces(removePunctuation(convert_accent(regexp_replace(city, '[^a-zA-Z\u00E0-\u00FC ]+', '')))))\
                                     .withColumn("street",sf.expr("regexp_replace(street,city,'')"))\
  
  else:
    
    dataset_cleaned = dataset_cleaned.withColumn('city', sf.lit('NA'))
    
  if postal_code != 'NA':
    
    dataset_cleaned = dataset_cleaned.withColumn('postcode',reorder_postcode(col(postal_code)))\
                                     .withColumn("street",sf.expr("regexp_replace(street,postcode,'')"))
  else:
    
    dataset_cleaned = dataset_cleaned.withColumn('postcode', sf.lit('NA'))
                  
  
  dataset_cleaned = dataset_cleaned.select('name','street', 'postcode', 'city', 'country', identifier)\
                                   .dropDuplicates()

  return(dataset_cleaned)




def apply_dictionary(dataset, column_dict, country, dictionary, pattern, replacement, isocode):
  
  dictionary = dictionary.withColumnRenamed(pattern, 'pattern')\
                         .withColumnRenamed(replacement, 'replacement')\
                         .withColumnRenamed(isocode, 'isocode')
    
  dataset = dataset.withColumnRenamed(column_dict, 'column_dict')\
                   .withColumnRenamed(country, 'country')\
  
  cond = [
    dictionary.isocode == dataset.country,
    sf.expr("column_dict like concat('%', pattern, '%')")     
  ]

  
  dataset = dataset.join(dictionary, cond, 'left').drop('isocode')
  
  dataset = dataset.withColumn('column_dict', sf.when(col('pattern').isNotNull(), sf.expr("regexp_replace(column_dict,pattern,replacement)")).otherwise(col('column_dict')))\
                   .drop('replacement', 'pattern')\
                   .withColumnRenamed('column_dict', column_dict)\
                   .withColumnRenamed('country', country)\
                   .dropDuplicates()

  return(dataset)



def convert_isocode(cleaned_dataset, isocode_file, country_attribute):
  
  '''
  Convert Isocode if needed
  '''
  
  if country_attribute == 'country_name':
    
    cleaned_dataset = cleaned_dataset.join(isocode_file, [cleaned_dataset.country == isocode_file.country_name], 'inner')\
                                     .drop('country','country_name', 'isocode3')\
                                     .withColumnRenamed('isocode2', 'country')
  
  if country_attribute == 'isocode3':
    
    cleaned_dataset = cleaned_dataset.join(isocode_file, [cleaned_dataset.country == isocode_file.isocode3], 'inner')\
                                     .drop('country','country_name', 'isocode3')\
                                     .withColumnRenamed('isocode2', 'country')
      
  return(cleaned_dataset)



  
def stringMatching(dataset1, identifier1, dataset2, identifier2, similarity_on_address = True, \
                  first_split = 10, second_split = 20, diff_group1 = 4, diff_group2 = 7, diff_group3 = 9,\
                  city = 'NA', postcode = 'NA'):
  
  '''
  This function inner join the two given datasets based on the name and address matching approach
  in order to build a mapping table between the two identifiers
  '''
  
  # Change name of second dataset for convienence
  dataset2 = dataset2.withColumnRenamed('name', 'name2')\
                     .withColumnRenamed('street', 'street2')\
                     .withColumnRenamed('postcode', 'postcode2')\
                     .withColumnRenamed('city', 'city2')\
                     .withColumnRenamed('country', 'country2')
  
  if similarity_on_address == True:
    
    #### Different splits based on street length, in order to allow more differences in longer addresses

    split1 = dataset1.filter(sf.length(col('street')) < first_split)
    split2 = dataset1.filter(sf.length(col('street')) >= first_split).filter(sf.length(col('street')) < second_split)
    split3 = dataset1.filter(sf.length(col('street')) >= second_split)
    
    if city != 'NA' and postcode != 'NA':
    
      cond1 = (
        (split1.name == dataset2.name2) & (split1.country == dataset2.country2) &\
        (sf.levenshtein(split1.street, dataset2.street2) <= diff_group1) &\
        ((split1.city == dataset2.city2) | (split1.postcode == dataset2.postcode2)))
      
      cond2 = (
        (split2.name == dataset2.name2) & (split2.country == dataset2.country2) &\
        (sf.levenshtein(split2.street, dataset2.street2) <= diff_group2) &\
        ((split2.city == dataset2.city2) | (split2.postcode == dataset2.postcode2)))
      
      cond3 = (
        (split3.name == dataset2.name2) & (split3.country == dataset2.country2) &\
        (sf.levenshtein(split3.street, dataset2.street2) <= diff_group3) &\
        ((split3.city == dataset2.city2) | (split3.postcode == dataset2.postcode2)))
    
    else: # in case one between city or postal we can change the condition and allow equality on both - the equality of the missing value will always be satisfied
      
      cond1 = (
        (split1.name == dataset2.name2) & (split1.country == dataset2.country2) &\
        (sf.levenshtein(split1.street, dataset2.street2) <= diff_group1) &\
        (split1.city == dataset2.city2) &\
        (split1.postcode == dataset2.postcode2))
      
      cond2 = (
        (split2.name == dataset2.name2) & (split2.country == dataset2.country2) &\
        (sf.levenshtein(split2.street, dataset2.street2) <= diff_group2) &\
        (split2.city == dataset2.city2) & \
        (split2.postcode == dataset2.postcode2))
      
      cond3 = (
        (split3.name == dataset2.name2) & (split3.country == dataset2.country2) &\
        (sf.levenshtein(split3.street, dataset2.street2) <= diff_group3) &\
        (split3.city == dataset2.city2) &\
        (split3.postcode == dataset2.postcode2))
      
    
    name_matches1 = split1.join(dataset2, cond1, 'inner')
    
    name_matches2 = split2.join(dataset2, cond2, 'inner')
    
    name_matches3 = split3.join(dataset2, cond3, 'inner')
    
    matches = name_matches1.unionByName(name_matches2)\
                           .unionByName(name_matches3)\
                           .select(identifier1, identifier2, 'country')\
                           .dropDuplicates()
    
    
  if similarity_on_address == False:
    
    if city != 'NA' and postcode != 'NA':

      cond = (
        (dataset1.name == dataset2.name2) & (dataset1.country == dataset2.country2) &\
        (dataset1.street == dataset2.street2) &\
        ((dataset1.city == dataset2.city2) | (dataset1.postcode == dataset2.postcode2))\
      )
    
    else:
      cond = (
        (dataset1.name == dataset2.name2) & (dataset1.country == dataset2.country2) &\
        (dataset1.street == dataset2.street2) &\
        (dataset1.city == dataset2.city2) &\
        (dataset1.postcode == dataset2.postcode2)\
      )
    
    matches = dataset1.join(dataset2, cond, 'inner')\
                            .select(identifier1, identifier2, 'country')\
                            .dropDuplicates()
  
  
  return(matches)
  

def compute_score(mapping_table, dataset1_cleaned, dataset2_cleaned, identifier1, identifier2, street, city, pstl):
  
    '''
    Function to be applied to the mapping table and to the dataset partially cleaned (without fuzzy dictionary)
    and add a column with the score of similiarity
    '''
    
    
    mapping_table = mapping_table.join(dataset1_cleaned.drop('country'), identifier1, 'inner')\
                               .withColumnRenamed('name', 'name_1')\
                               .withColumnRenamed('street', 'street_1')\
                               .withColumnRenamed('postcode' ,'postcode_1')\
                               .withColumnRenamed('city', 'city_1')\
                               .join(dataset2_cleaned.drop('country'), identifier2, 'inner')\
                               .withColumnRenamed('name', 'name_2')\
                               .withColumnRenamed('street' ,'street_2')\
                               .withColumnRenamed('postcode' ,'postcode_2')\
                               .withColumnRenamed('city', 'city_2')

    mapping_table = mapping_table.withColumn('score_name', 1 - (sf.levenshtein(col('name_1'), col('name_2'))/(sf.length(col('name_1')) + sf.length(col('name_2')))))\
                 .withColumn('score_street', 1- (sf.levenshtein(col('street_1'), col('street_2'))/(sf.length(col('street_1')) + sf.length(col('street_2')))))\
                 .withColumn('score_pstl', sf.when(col('postcode_1') == col('postcode_2'), 1).otherwise(0))\
                 .withColumn('score_city', 1 - (sf.levenshtein(col('city_1'), col('city_2'))/(sf.length(col('city_1')) + sf.length(col('city_2')))))\
                 .withColumn('score_name',sf.when(col('score_name').isNull(),0).otherwise(col('score_name')))\
                 .withColumn('score_street',sf.when(col('score_street').isNull(),0).otherwise(col('score_street')))\
                 .withColumn('score_pstl',sf.when(col('score_pstl').isNull(),0).otherwise(col('score_pstl')))\
                 .withColumn('score_city',sf.when(col('score_city').isNull(),0).otherwise(col('score_city')))
                  
    if street != 'NA' and city != 'NA' and pstl != 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_street')+col('score_pstl')+col('score_city'))/4)
    elif street != 'NA' and city != 'NA' and pstl == 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_street')+col('score_city'))/3)
    elif street != 'NA' and city == 'NA' and pstl != 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_street')+col('score_pstl'))/3)   
    elif street == 'NA' and city != 'NA' and pstl != 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_city')+col('score_pstl'))/3)
    elif street == 'NA' and city == 'NA' and pstl != 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_pstl'))/2)
    elif street == 'NA' and city != 'NA' and pstl == 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_city'))/2)
    elif street != 'NA' and city == 'NA' and pstl == 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')+col('score_street'))/2)
    elif street == 'NA' and city == 'NA' and pstl == 'NA':
       mapping_table = mapping_table.withColumn('score', (col('score_name')))
        
    mapping_table = mapping_table.select(identifier1, identifier2, 'country', 'score')

    w = Window.partitionBy(identifier1, identifier2, 'country')

    mapping_table = mapping_table.withColumn('score', sf.max('score').over(w)*100)
    
    return(mapping_table)
  
  
def fuzzyNameMatching(spark,
                      dataset1_original, id_data1, name_data1, country_1, street_data1, city_data1, pstl_data1,country_attribute,
                      dataset2_original, id_data2, name_data2, country_2, street_data2, city_data2, pstl_data2,country_attribute2,
                      fuzzy_wuzzy, fuzzy_levels,
                      address_similarity, first_split, diff_group1, second_split, diff_group2, diff_group3,
                      add_column_score,
                      save_table, path, table_name):
  
  ## Load Dictionaries

  # Official abbreviations

  abbreviations = pd.read_csv('data/dictionary_abbreviations.csv')
  abbreviations = spark.createDataFrame(abbreviations)
  abbreviations = abbreviations.withColumn('abbr', removeSpaces(removePunctuation(convert_accent(col('abbr')))))\
                               .withColumn('descr', removeSpaces(removePunctuation(convert_accent(col('descr')))))

  abbreviations.cache()
  abbreviations.count()


   # FuzzyWuzzy dictionary

  fuzzy_dict = pd.read_csv('data/fuzzywuzzy_dictionary.csv')
  fuzzy_dict[['isocode', 'pattern',	'replacement']] = fuzzy_dict[['isocode', 'pattern',	'replacement']].astype(str)
  fuzzy_dict = spark.createDataFrame(fuzzy_dict)
  fuzzy_dict = fuzzy_dict.withColumn('replacement', sf.when(col('replacement') == 'nan', '').otherwise(col('replacement')))

  fuzzy_dict.cache()
  fuzzy_dict.count()


   # Isocodes converter

  if country_attribute != country_attribute2:

    isocodes = pd.read_csv('data/isocodes.csv')[['name', 'alpha-2', 'alpha-3']]
    isocodes[['name', 'alpha-2', 'alpha-3']] = isocodes[['name', 'alpha-2', 'alpha-3']].astype(str)
    isocodes = spark.createDataFrame(isocodes)

    isocodes = isocodes.withColumn('name', removeSpaces(removePunctuation(convert_accent(col('name')))))\
                       .withColumnRenamed('name', 'country_name')\
                       .withColumnRenamed('alpha-2', 'isocode2')\
                       .withColumnRenamed('alpha-3', 'isocode3')

    isocodes.cache()
    isocodes.count()


  #------------------------------------
  # 1. Preparation of the first dataset
  #------------------------------------
  dataset1_original.cache()
  dataset1_original.count()

  #### Apply attribute cleaning function
  dataset1_name_match = data_preparation(dataset1_original,  identifier = id_data1, \
                                     name_entity = name_data1, address = street_data1, \
                                     city = city_data1, postal_code = pstl_data1, country = country_1)

  # Convert country into isocode if needed:

  if country_attribute != country_attribute2:

    dataset1_name_match = convert_isocode(dataset1_name_match, isocodes, country_attribute)


  # Apply dictionary of official abbreviations

  dataset1_name_match = apply_dictionary(dataset1_name_match, country ='country', column_dict = 'name', \
                                     dictionary = abbreviations, pattern = 'descr', \
                                     replacement = 'abbr',isocode = 'isocode')

  # Apply FuzzyWuzzy dictionary

  if fuzzy_wuzzy:
    dataset1 = apply_dictionary(dataset1_name_match, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')

    if fuzzy_levels == 2:
        dataset1 = apply_dictionary(dataset1, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')
    if fuzzy_levels > 2:
        dataset1 = apply_dictionary(dataset1, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')


  else: dataset1 = dataset1_name_match


  dataset1.cache()
  dataset1.count()



  #-------------------------------------
  # 2. Preparation of the second dataset
  #-------------------------------------
  dataset2_original.cache()
  dataset2_original.count()


  #### Apply attribute cleaning function
  dataset2_name_match = data_preparation(dataset2_original,  identifier = id_data2, \
                                     name_entity = name_data2, address = street_data2, \
                                     city = city_data2, postal_code = pstl_data2, country = country_2)

  # Convert country into isocode if needed:

  if country_attribute != country_attribute2:

    dataset2_name_match = convert_isocode(dataset2_name_match, isocodes, country_attribute2)


  # Apply dictionary of official abbreviations

  dataset2_name_match = apply_dictionary(dataset2_name_match, country ='country', column_dict = 'name', \
                                     dictionary = abbreviations, pattern = 'descr', \
                                     replacement = 'abbr',isocode = 'isocode')

  # Apply FuzzyWuzzy dictionary

  if fuzzy_wuzzy:
    dataset2 = apply_dictionary(dataset2_name_match, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')

    if fuzzy_levels >= 2:
        dataset2 = apply_dictionary(dataset2, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')
    if fuzzy_levels >= 3:
        dataset2 = apply_dictionary(dataset2, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')

  else: dataset2 = dataset2_name_match

  dataset2.cache()
  dataset2.count()



  #-------------------------------
  # Obtain Matching Table
  #-------------------------------
  if street_data1 == 'NA':
    address_similarity = False

  name_matches = stringMatching(dataset1, id_data1, dataset2, id_data2, \
                                address_similarity, first_split, second_split, diff_group1, \
                                diff_group2, diff_group3, city_data1, pstl_data2)


  name_matches.cache()
  name_matches.count()

  dataset1.unpersist()
  dataset2.unpersist()

  #------------------------------------
  # Compute score
  #-------------------------------------

  if add_column_score == True:
      name_matches = compute_score(name_matches, dataset1_name_match, \
                                   dataset2_name_match, id_data1, id_data2,\
                                   street_data1, city_data1, pstl_data1).dropDuplicates()

      name_matches.cache()
      name_matches.count()
      
  #-----------------------------------------                    
  # Write final table
  #-----------------------------------------

  if save_table:

    name_matches.write.format("csv").mode('overwrite')\
                    .option("path", path + table_name)

    print('Table successfully saved in: ' + path+"/"+table_name)

      
  return(name_matches)
