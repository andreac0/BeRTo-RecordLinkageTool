import pandas as pd
from string_matching import *

dataset1_original = pd.read_csv("data/mfi.csv", sep=';')
dataset2_original = pd.read_csv("data/gleif_it.csv", sep=',')
dataset1_original.columns
dataset2_original.columns

id_data1 = 'RIAD_CODE' 
name_data1 = 'NAME'
country_1 = 'COUNTRY_OF_REGISTRATION'
street_data1 = 'ADDRESS'
city_data1 = 'CITY'
pstl_data1 = 'POSTAL'
country_attribute = 'isocode2'

id_data2 = 'LEI'
name_data2 = 'Name'
country_2 = 'Country'
street_data2 = 'Address'
city_data2 = 'City'
pstl_data2 = 'Postcode'
country_attribute2 = 'isocode2'

  # Official abbreviations

abbreviations = pd.read_csv('data/dictionary_abbreviations.csv')
abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'abbr'), 'abbr'), 'abbr')
abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'descr'), 'descr'), 'descr')

  # FuzzyWuzzy dictionary

fuzzy_dict = pd.read_csv('data/fuzzywuzzy_dictionary.csv')
fuzzy_dict[['isocode', 'pattern',	'replacement']] = fuzzy_dict[['isocode', 'pattern',	'replacement']].astype(str)
fuzzy_dict['replacement'] = fuzzy_dict['replacement'].apply(lambda x: re.sub('nan','',str(x)))

  # Isocodes

isocodes = pd.read_csv('data/isocodes.csv')[['name', 'alpha-2', 'alpha-3']]
isocodes[['name', 'alpha-2', 'alpha-3']] = isocodes[['name', 'alpha-2', 'alpha-3']].astype(str)
isocodes.rename(columns={'name': 'Country Name','alpha-2':'Isocode2','alpha-3':'Isocode3'}, inplace=True)



dataset1_name_match = data_preparation(dataset1_original,  identifier = id_data1, \
                                     name_entity = name_data1, address = street_data1, \
                                     city = city_data1, postal_code = pstl_data1, country = country_1, \
                                     AIparser=False)

dataset2_name_match = data_preparation(dataset2_original,  identifier = id_data2, \
                                     name_entity = name_data2, address = street_data2, \
                                     city = city_data2, postal_code = pstl_data2, country = country_2,\
                                     AIparser=False)



dataset1_name_match = apply_dictionary(dataset1_name_match, country ='country', column_dict = 'name', \
                                     dictionary = abbreviations, pattern = 'descr', \
                                     replacement = 'abbr',isocode = 'isocode')

dataset1 = apply_dictionary(dataset1_name_match, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')


dataset2 = apply_dictionary(dataset2_name_match, country ='country', column_dict = 'name', \
                                     dictionary = abbreviations, pattern = 'descr', \
                                     replacement = 'abbr',isocode = 'isocode')

dataset2 = apply_dictionary(dataset2_name_match, country ='country', column_dict = 'name', \
                              dictionary = fuzzy_dict, pattern = 'pattern', \
                              replacement = 'replacement',isocode = 'isocode')


name_matches = stringMatching(dataset1, id_data1, dataset2, id_data2, False, ratio_similarity = 0.4, city = city_data1, postcode= pstl_data2)

joinOriginalData(name_matches, dataset1_original, 'RIAD_CODE', dataset2_original, 'LEI', name_data1, name_data2, street_data1, street_data2)

