import numbers
import numpy as np
import re
import pandas as pd
from deepparse.parser import AddressParser
from pandasql import sqldf
import Levenshtein as lv

pysqldf = lambda q: sqldf(q, globals())

def removePunctuation(data, column):

    """
    Removes punctuation, changes to upper case, and strips leading and 
    trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained. Other characters 
        should should be eliminated (e.g. it's becomes its). Leading and 
        trailing spaces should be removed after punctuation is removed.

    """
    data[column] = data[column].apply(lambda x: re.sub(r'[^\w\s]', '', str(x)))
    data[column] = data[column].apply(lambda x: x.upper())
    data[column] = data[column].apply(lambda x: x.strip())

    return data

def removeSpaces(data, column):

    '''
    Remove whitespaces
    ''' 
    data[column] = data[column].apply(lambda x:re.sub(r' ', '', str(x)))
    data[column] = data[column].apply(lambda x: x.strip())

    return data


def convert_accent(data, column):
  
  '''
  This function convert and replace all special/accented letters to standard alphabet
  '''
  data[column] =data[column].apply(lambda x: str(x))
  data[column] = data[column].apply(lambda x: \
    re.sub('ž','z', re.sub('š','s', re.sub('ê','e', re.sub('è','e', re.sub('æ','a', re.sub('č','e', re.sub('ý','y', re.sub('ăă','co', re.sub('ę','e', \
    re.sub('ő','o', re.sub('ć','ae', re.sub('ń','n', re.sub('ă','a', re.sub('ß','ss', re.sub('ř','oe', re.sub('ë','e', re.sub('ú','u', re.sub('õ','o', \
    re.sub('ó','o', re.sub('ô','o', re.sub('ã','a', re.sub('ç','c', re.sub('ñ','n', re.sub('ü','ue', re.sub('ä','ae', re.sub('ö','oe', re.sub('é','e', \
    re.sub('æ','a', re.sub('ø','oe', re.sub('á','a', re.sub('í','i', re.sub('å','aa', re.sub( 'à', 'a', re.sub('ì', 'i', re.sub('Ž','Z', re.sub('Š','S', \
    re.sub('Ê','E', re.sub('È','E', re.sub('Æ','A', re.sub('Č','E', re.sub('Ý','Y', re.sub('ĂĂ','CO', re.sub('Ę','E', re.sub('Ő','O', re.sub('Ć','AE', \
    re.sub('Ń','N', re.sub('Ă','A', re.sub('ß','SS', re.sub('Ř','OE', re.sub('Ë','E', re.sub('Ú','U', re.sub('Õ','O', re.sub('Ó','O', re.sub('Ô','O', \
    re.sub('Ã','A', re.sub('Ç','C', re.sub('Ñ','N', re.sub ('Ü','UE', re.sub('Ä','AE', re.sub('Ö','OE', re.sub('É','E',re.sub('Æ','A',re.sub('Ø','OE',\
    re.sub('Á','A', re.sub('Í','I', re.sub('Å','AA', re.sub('À', 'A', re.sub('Ì', 'I', x)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))


  return data


def reorder_street(data, column, useParser = True):
  
  '''
  This function re-order the street in order to get a standardized format for the address: 
  house number + street name
  '''
  if useParser:
    address_upper = data[column].apply(lambda x: str(x).upper())
    address_parser = AddressParser(model_type="bpemb", device=0)
    parsed_addresses = address_parser(address_upper)
    fields = ['StreetNumber', 'StreetName', 'Municipality', 'Province', 'PostalCode']
    parsed_address_data_frame = pd.DataFrame([parsed_address.to_dict(fields=fields) for parsed_address in parsed_addresses],
                                columns=fields)

    data[column] = parsed_address_data_frame['StreetNumber']+' '+parsed_address_data_frame['StreetName']
  
  else:
    address_upper = data[column].apply(lambda x: str(x).upper())
    numbers = address_upper.apply(lambda x: re.findall(r'\d+', x))
    numbers = numbers.apply(lambda x: ''.join([n for n in x if n.isdigit()]))
    letters = address_upper.apply(lambda x: re.sub(r'[^a-zA-Z]', '', x))

    data[column] = numbers + ' ' + letters

  return data

def reorder_postcode(data, column):

  '''
  This function re-order the postal code, removing whitespaces and using a standardized format: letters + numbers
  '''
  data = removeSpaces(data, column)

  numbers = data[column].apply(lambda x: re.findall(r'\d+', str(x)))
  numbers = numbers.apply(lambda x: ''.join([n for n in x if n.isdigit()]))

  letters = data[column].apply(lambda x: re.sub(r'[^a-zA-Z]', '', str(x)))
  data[column] = numbers + letters

  return data


def countryRulesStreet(data, column):
    '''
    This function provides a list of common rules and patterns about address abbreviations and
    replace these words with the latter.
    '''
    data[column] = data[column].apply(lambda x: str(x))
    data[column] = data[column].apply(lambda x: re.sub('STRASSE', 'STR', x))
    data[column] = data[column].apply(lambda x: re.sub('PLATZ', 'PL', x))
    data[column] = data[column].apply(lambda x: re.sub('GASSE', 'G', x))
    data[column] = data[column].apply(lambda x: re.sub('HAUSNUMMER', '', x))
    data[column] = data[column].apply(lambda x: re.sub('HAUSNR', '', x))
    
    data[column] = data[column].apply(lambda x: re.sub('STREET', 'ST', x))
    data[column] = data[column].apply(lambda x: re.sub('ROAD', 'RD', x))
    data[column] = data[column].apply(lambda x: re.sub('SQUARE', 'SQ', x))
    data[column] = data[column].apply(lambda x: re.sub('STATION', 'STA', x))
    data[column] = data[column].apply(lambda x: re.sub('TERRACE', 'TERR', x))
    data[column] = data[column].apply(lambda x: re.sub('BRIDGE', 'BRG', x))
    data[column] = data[column].apply(lambda x: re.sub('CAPE', 'CPE', x))
    data[column] = data[column].apply(lambda x: re.sub('CENTER','CTR', x))
    data[column] = data[column].apply(lambda x: re.sub('CENTRE', 'CTR', x))
    data[column] = data[column].apply(lambda x: re.sub('CORNER', 'COR', x))
    data[column] = data[column].apply(lambda x: re.sub('COURT', 'CT', x))
    data[column] = data[column].apply(lambda x: re.sub('CROSSING', 'XING', x))
    data[column] = data[column].apply(lambda x: re.sub('ESTATE', 'EST', x))
    data[column] = data[column].apply(lambda x: re.sub('FIELD', 'FLD', x))
    data[column] = data[column].apply(lambda x: re.sub('HIGHWAY', 'HWY', x))
    data[column] = data[column].apply(lambda x: re.sub('HILL', 'HL', x))
    data[column] = data[column].apply(lambda x: re.sub('ISLAND', 'IS', x))
    data[column] = data[column].apply(lambda x: re.sub('LAKE', 'LK', x))
    data[column] = data[column].apply(lambda x: re.sub('MEADOW', 'MDW', x))
    data[column] = data[column].apply(lambda x: re.sub('MOTORWAY', 'MTWY', x))
    data[column] = data[column].apply(lambda x: re.sub('PARKWAY', 'PKWY', x))
    data[column] = data[column].apply(lambda x: re.sub('PASSAGE', 'PSGE', x))
    data[column] = data[column].apply(lambda x: re.sub('PLAZA', 'PLZ', x))
    
    data[column] = data[column].apply(lambda x: re.sub('BOULEVARD', 'BLVD', x))
    data[column] = data[column].apply(lambda x: re.sub('BOULV', 'BLVD', x))
    data[column] = data[column].apply(lambda x: re.sub('AVENUE',	'AVE', x))
    data[column] = data[column].apply(lambda x: re.sub('AVENU',	'AVE', x))
    data[column] = data[column].apply(lambda x: re.sub('PLACE', 'PL', x))    
    
    data[column] = data[column].apply(lambda x: re.sub('PIAZZA', 'PZA', x))
    data[column] = data[column].apply(lambda x: re.sub('CORSO', 'CSO', x))
    data[column] = data[column].apply(lambda x: re.sub('FRATELLI', 'FLLI', x))
    data[column] = data[column].apply(lambda x: re.sub('LARGO', 'LGO', x))
    data[column] = data[column].apply(lambda x: re.sub('VIALE', 'VLE', x))
    data[column] = data[column].apply(lambda x: re.sub('STRADALE', 'STR', x))
    data[column] = data[column].apply(lambda x: re.sub('NUMERO', 'N', x))
    
    data[column] = data[column].apply(lambda x: re.sub('CALLE', 'CL', x))
    data[column] = data[column].apply(lambda x: re.sub('AVENIDA', 'AV', x))
    data[column] = data[column].apply(lambda x: re.sub('AUTOPISTA', 'AU', x))
    data[column] = data[column].apply(lambda x: re.sub('CARRETERA', 'CT', x))
    data[column] = data[column].apply(lambda x: re.sub('BARRIO', 'BR', x))
    data[column] = data[column].apply(lambda x: re.sub('EDIFICIO', 'ED', x))
    data[column] = data[column].apply(lambda x: re.sub('CASA', 'CS', x))

    return data 


def data_preparation(dataset, identifier, name_entity, address, city, postal_code, country, AIparser = True):
  '''
  This function clean and uniform the attributes that will be used for the matching based
  on the name and address
  '''
  dataset = dataset[~dataset[identifier].isnull()]
  dataset = dataset[dataset[identifier] != '']
  dataset = dataset[~dataset[name_entity].isnull()]
  dataset = dataset[~dataset[country].isnull()]
  dataset = dataset[dataset[country] != '']
  
  dataset = removeSpaces(removePunctuation(convert_accent(dataset, name_entity), name_entity), name_entity)
  dataset = dataset[dataset[name_entity] != '']
  dataset.rename(columns={name_entity:'name'}, inplace=True)
  
  dataset = removeSpaces(removePunctuation(convert_accent(dataset, country), country), country)
  dataset.rename(columns={country: 'country'}, inplace=True)

  if address != 'NA':
    dataset = countryRulesStreet(removePunctuation(reorder_street(convert_accent(dataset, address), address, useParser=AIparser), address), address)
    dataset.rename(columns={address: 'street'}, inplace=True)
  else:
    no_addresses = pd.DataFrame(columns = {'street'})
    dataset = pd.concat([dataset,no_addresses.assign(street=np.repeat('NA', len(dataset), axis=0).tolist())], axis=1)


  if city != 'NA':
    dataset[city] = dataset[city].apply(lambda x: re.sub(r'[^a-zA-Z]', '', str(x)))
    dataset = removeSpaces(removePunctuation(convert_accent(dataset, city), city), city)
    dataset.rename(columns={city: 'city'}, inplace=True)
  else:
    no_cities = pd.DataFrame(columns = {'city'})
    dataset = pd.concat([dataset,no_cities.assign(city=np.repeat('NA', len(dataset), axis=0).tolist())], axis=1)
  
    
  if postal_code != 'NA':
    dataset = reorder_postcode(dataset, postal_code)
    dataset.rename(columns={postal_code: 'postcode'}, inplace=True)
  else:
    no_pcode = pd.DataFrame(columns = {'postcode'})
    dataset = pd.concat([dataset,no_pcode.assign(postcode=np.repeat('NA', len(dataset), axis=0).tolist())], axis=1)
                  
  
  dataset_cleaned = dataset[['name','street', 'postcode', 'city', 'country', identifier]].drop_duplicates()

  return dataset_cleaned

def replace_pattern(data, column = 'column_dict', pattern='pattern', replacement = 'replacement'):
  names_to_fix = data[column]
  patterns = data[pattern]
  replacements = data[replacement]

  for i in names_to_fix.index:
    if patterns[i] == None:
      data[column].loc[i] = names_to_fix[i]
    else:
      if replacements[i] == None:
        data[column].loc[i] = re.sub(patterns[i], '', names_to_fix[i])
      else:
        data[column].loc[i] = re.sub(patterns[i], replacements[i], names_to_fix[i])

  return data

def apply_dictionary(dataset, column_dict, country, dictionary, pattern, replacement, isocode):
  
  dictionary.rename(columns={pattern: 'pattern'}, inplace=True)
  dictionary.rename(columns={replacement: 'replacement'}, inplace=True)
  dictionary.rename(columns={isocode: 'isocode'}, inplace=True)

  dataset.rename(columns={column_dict: 'column_dict'}, inplace=True)
  dataset.rename(columns={country: 'country'}, inplace=True)

  dataset = sqldf("SELECT * \
                  FROM dataset \
                  LEFT JOIN dictionary\
                  ON dataset.country = dictionary.isocode\
                  AND dataset.column_dict LIKE ('%' || pattern || '%')", locals())

  dataset = dataset.drop('isocode',axis=1)

  dataset[~dataset['pattern'].isnull()] = replace_pattern(dataset[~dataset['pattern'].isnull()])

  dataset = dataset.drop(['replacement', 'pattern'],axis=1).drop_duplicates()
  dataset.rename(columns={'column_dict': column_dict}, inplace=True)
  dataset.rename(columns={'country': country}, inplace=True)

  return dataset


def convert_isocode(cleaned_dataset, isocode_file, country_attribute):
  
  '''
  Convert Isocode if needed
  '''
  
  if country_attribute == 'Country Name':

    cleaned_dataset = pd.merge(cleaned_dataset, isocode_file, left_on=['country'], right_on=['Country Name'], how='inner')
    
    cleaned_dataset = cleaned_dataset.drop(['country','Country Name', 'Isocode3'])
    cleaned_dataset.rename(columns={'Isocode2': 'country'}, inplace=True)
  
  if country_attribute == 'Isocode3':
    
    cleaned_dataset = pd.merge(cleaned_dataset, isocode_file, left_on=['country'], right_on=['Isocode3'], how='inner')
    
    cleaned_dataset = cleaned_dataset.drop(['country','Country Name', 'Isocode3'])
    cleaned_dataset.rename(columns={'Isocode2': 'country'}, inplace=True)

  return(cleaned_dataset)
   
def levenstheinColumns(data, column1, column2):
  data[column1] = data[column1].apply(lambda x: str(x))
  data[column2] = data[column2].apply(lambda x: str(x))
  n = len(data)
  ratios = list()
  for i in range (0,n):
    ratios.append(lv.ratio(data[column1][i], data[column2][i]))
  data.loc[i]
  return ratios


def stringMatching(dataset1, identifier1, dataset2, identifier2, perfect_match = False, \
                  type_similarity = 'Levensthein', ratio_similarity = 0.7, city = 'NA', postcode = 'NA'):
  
  '''
  This function inner join the two given datasets based on the name and address matching approach
  in order to build a mapping table between the two identifiers
  '''
  
  # Change name of second dataset for convienence
  dataset2.rename(columns={'name' : 'name2','street':'street2', 'postcode':'postcode2',\
                            'city':'city2', 'country':'country2'}, inplace=True)

  if perfect_match == False:
    
    if city != 'NA' and postcode != 'NA':

      name_matches1 = pd.merge(dataset1, dataset2, left_on=['name','country','postcode'],  
                                right_on=['name2','country2','postcode2'], how='inner')

      name_matches2 = pd.merge(dataset1, dataset2, left_on=['name','country','city'],  
                                right_on=['name2','country2','city2'], how='inner')

      # name_matches3 = pd.merge(dataset1, dataset2, left_on=['street','country','city'],  
      #                           right_on=['street2','country2','city2'], how='inner')

      # if type_similarity == 'Levensthein':
      #     name_matches3['score_name'] = levenstheinColumns(name_matches3, 'name', 'name2')
      #     name_matches3 = name_matches3[name_matches3['score_name'] >= ratio_similarity].drop(columns='score_name').reset_index(drop = True)

      name_matches = pd.concat([name_matches1, name_matches2],axis=0).drop_duplicates().reset_index(drop=True)

    else: # in case one between city or postal we can change the condition and allow equality on both -
      # the equality of the missing value will always be satisfied
      
      name_matches1 = pd.merge(dataset1, dataset2, left_on=['name','country','city','postcode'],  
                                right_on=['name2','country2','city2','postcode2'], how='inner')

      # name_matches2 = pd.merge(dataset1, dataset2, left_on=['street','country','city','postcode'],  
      #                           right_on=['street2','country2','city2','postcode2'], how='inner')

      # if type_similarity == 'Levensthein':
      #     name_matches2['score_name'] = levenstheinColumns(name_matches2, 'name', 'name2')
      #     name_matches2 = name_matches2[name_matches2['score_name'] >= ratio_similarity].drop(columns='score_name').reset_index(drop = True)

      name_matches = pd.concat([name_matches1],axis=0).drop_duplicates().reset_index(drop=True)

    if type_similarity == 'Levensthein':
      name_matches['score_address'] = levenstheinColumns(name_matches, 'street', 'street2')
      name_matches = name_matches[name_matches['score_address'] >= ratio_similarity].reset_index(drop = True)
      matches = name_matches[[identifier1, identifier2, 'country','score_address']]
    else:
      matches = name_matches[[identifier1, identifier2, 'country']]

    matches = matches.drop_duplicates()

  if perfect_match == True:
    
    if city != 'NA' and postcode != 'NA':
      name_matches1 = pd.merge(dataset1, dataset2, left_on=['name','country','postcode'],  
                                right_on=['name2','country2','postcode2'], how='inner')

      name_matches2 = pd.merge(dataset1, dataset2, left_on=['name','country','city'],  
                                right_on=['name2','country2','city2'], how='inner')                     
      name_matches = pd.concat([name_matches1, name_matches2],axis=0)

    else:
      name_matches = pd.merge(dataset1, dataset2, left_on=['name','country','city','postcode','street'],  
                                right_on=['name2','country2','city2','postcode2','street2'], how='inner')
    
    matches = name_matches[[identifier1, identifier2, 'country']].drop_duplicates()

  
  return matches.reset_index(drop=True)


def joinOriginalData(matches, dataset1, identifier1, dataset2, identifier2, name1, name2, stree1, street2):

  matches = pd.merge(matches, dataset1, how= 'left', on=identifier1).drop_duplicates()
  matches = pd.merge(matches, dataset2, how= 'left', on=identifier2).drop_duplicates()
  matches['score_name'] = levenstheinColumns(matches, name1, name2)
  matches['score_address'] = levenstheinColumns(matches, stree1, street2)


  return matches