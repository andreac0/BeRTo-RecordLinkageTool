# BeRTo - Business Registry Record Linkage Tool

 PySpark-based tool to link legal entity business registries when no common ID is available, using name and address information


## Data Sources for Business Registries
We list some data sources for business registries that can be used for BeRTo experiments:
- ECB Data: https://www.ecb.europa.eu/stats/financial_corporations/list_of_financial_institutions/html/index.en.html
- GLEIF Data: https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy?cachepath=it%2Flei-data%2Fgleif-golden-copy%2Fdownload-the-golden-copy#/
- National Business Registries: https://2015.index.okfn.org/dataset/companies/

## BeRTo's configurations

Suggested configuration for BeRTo-R (recall version):
```
config = {
    # Indicate attributes of first data source
  "identifier_1" : "code",     # mandatory
  "name_1" : "name",           # mandatory
  "country_1" : "country",     # mandatory
  "type_country" : "isocode2", # mandatory
  "street_1" : "NA",      # set "NA" to disable
  "city_1" : "NA",           # set "NA" to disable
  "post_1" : "NA",           # set "NA" to disable
    
    # Indicate attributes of second data source
  "identifier_2" : "LEI",       # mandatory
  "name_2" : "NAME",            # mandatory
  "country_2" : "CNTY",         # mandatory
  "type_country2" : "isocode2", # mandatory
  "street_2" : "NA",          # set "NA" to disable
  "city_2" : "NA",            # set "NA" to disable
  "post_2" : "NA",              # set "NA" to disable

    # Fuzzy Name Settings 
  "use_fuzzy_dictionary": True,
  "fuzzy_level": 3, #possible values = 1,2,3 --> the higher the more attention to recall

    # Address processing settings
  "address_similarity" : False,
  "similarity_level" : 1, #possible values = 1,2,3 --> the higher the more attention to recall
    
    # Add score of matching
  "add_column_score" : True,

}
```

Suggested configuration for BeRTo-BR version:
```
config2 = {
    # Indicate attributes of first data source
  "identifier_1" : "code",     # mandatory
  "name_1" : "name",           # mandatory
  "country_1" : "country",     # mandatory
  "type_country" : "isocode2", # mandatory
  "street_1" : "address",      # set "NA" to disable
  "city_1" : "city",           # set "NA" to disable
  "post_1" : "post",           # set "NA" to disable
    
    # Indicate attributes of second data source
  "identifier_2" : "LEI",       # mandatory
  "name_2" : "NAME",            # mandatory
  "country_2" : "CNTY",         # mandatory
  "type_country2" : "isocode2", # mandatory
  "street_2" : "ADDR",          # set "NA" to disable
  "city_2" : "CITY",            # set "NA" to disable
  "post_2" : "PC",              # set "NA" to disable

    # Fuzzy Name Settings 
  "use_fuzzy_dictionary": True,
  "fuzzy_level": 3, #possible values = 1,2,3 --> the higher the more attention to recall

    # Address processing settings
  "address_similarity" : True,
  "similarity_level" : 3, #possible values = 1,2,3 --> the higher the more attention to recall
    
    # Add score of matching
  "add_column_score" : True,
}
```

Suggested configuration for BeRTo-BP version:
```
config3 = {
    # Indicate attributes of first data source
  "identifier_1" : "code",     # mandatory
  "name_1" : "name",           # mandatory
  "country_1" : "country",     # mandatory
  "type_country" : "isocode2", # mandatory
  "street_1" : "address",      # set "NA" to disable
  "city_1" : "city",           # set "NA" to disable
  "post_1" : "post",           # set "NA" to disable
    
    # Indicate attributes of second data source
  "identifier_2" : "LEI",       # mandatory
  "name_2" : "NAME",            # mandatory
  "country_2" : "CNTY",         # mandatory
  "type_country2" : "isocode2", # mandatory
  "street_2" : "ADDR",          # set "NA" to disable
  "city_2" : "CITY",            # set "NA" to disable
  "post_2" : "PC",              # set "NA" to disable

    # Fuzzy Name Settings 
  "use_fuzzy_dictionary": True,
  "fuzzy_level": 2, #possible values = 1,2,3 --> the higher the more attention to recall

    # Address processing settings
  "address_similarity" : True,
  "similarity_level" : 1, #possible values = 1,2,3 --> the higher the more attention to recall
    
    # Add score of matching
  "add_column_score" : True,
}
```

Suggested configuration for BeRTo-P version:
```
config3 = {
    # Indicate attributes of first data source
  "identifier_1" : "code",     # mandatory
  "name_1" : "name",           # mandatory
  "country_1" : "country",     # mandatory
  "type_country" : "isocode2", # mandatory
  "street_1" : "address",      # set "NA" to disable
  "city_1" : "city",           # set "NA" to disable
  "post_1" : "post",           # set "NA" to disable
    
    # Indicate attributes of second data source
  "identifier_2" : "LEI",       # mandatory
  "name_2" : "NAME",            # mandatory
  "country_2" : "CNTY",         # mandatory
  "type_country2" : "isocode2", # mandatory
  "street_2" : "ADDR",          # set "NA" to disable
  "city_2" : "CITY",            # set "NA" to disable
  "post_2" : "PC",              # set "NA" to disable

    # Fuzzy Name Settings 
  "use_fuzzy_dictionary": True,
  "fuzzy_level": 2, #possible values = 1,2,3 --> the higher the more attention to recall

    # Address processing settings
  "address_similarity" : True,
  "similarity_level" : 1, #possible values = 1,2,3 --> the higher the more attention to recall
    
    # Add score of matching
  "add_column_score" : True,
}
```

