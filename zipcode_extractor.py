import pandas as pd
import numpy as np

from collections import defaultdict
import urllib.request, json

import logging

logging.basicConfig(filename='fifi_zipcode_processing.log',
                      filemode='w',
                      level=logging.DEBUG,
                      format='%(levelname)s:%(asctime)s:%(message)s',
                      datefmt='%m/%d/%Y %I:%M:%S %p')

logger = logging.getLogger(__name__)


# Read each sheet in the excel file as a separate dataframe and load it to dictionary.
# The key of dictionary is the sheet name and value is the respective dataframe.

def extract_with_lat_lon():

  file_name = '../data/fifi_data.xlsx'
  required_columns = ['Service Request Number', 'Created Date', 'Location', 'Location Details', 'Description']

  xls = pd.ExcelFile(file_name)
  fifi_dict = defaultdict.fromkeys(xls.sheet_names)

  # sheet_to_df_map = pd.read_excel(file_name, sheet_name=xls.sheet_names, usecols=required_columns, parse_dates=True)

  for name in fifi_dict.keys():
    fifi_dict[name] = pd.read_excel(xls, name, usecols=required_columns)
  # Assign the sheet name/key as category for each dataframe, so that the dataframes can be distinguished by category post merge.

  for name, value in fifi_dict.items():
    fifi_dict[name]['Category'] = name

  df = pd.concat(fifi_dict, ignore_index=True)

  df['location_latitude'] = df['Location Details'].str.extract("LatLng: (.*),.*$", expand=True)
  df['location_longitude'] = df['Location Details'].str.extract("LatLng: .*, (.*)$", expand=True)

  df['location_X'] = df['Location Details'].str.extract("XY: (.*),.*LatLng:.*$", expand=True)
  df['location_Y'] = df['Location Details'].str.extract("XY: .*,(.*); LatLng:.*$", expand=True)

  # df = pd.read_csv('fifi_cleaned.csv', parse_dates=True)

  df['zipcode'] = df.apply(lambda x: get_zipcode(str(x.location_latitude).strip(), str(x.location_longitude).strip()),
                           axis=1)

  df.to_csv('../data/fifi_cleaned.csv')

def get_zipcode(lat, lon, recursion=0):
  result = np.nan
  try:
    url = 'https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=' + lat + '&longitude=' + lon + '&localityLanguage=en'
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())

    if data['postcode']:
      result = data['postcode']
      logger.info('Done')

  except:
    logger.error(f'Error lat:{lat}, lon:{lon}')

  return result

def fill_missing_zipcode_using_address():
  df = pd.read_csv('fifi_cleaned.csv', parse_dates=True)

  df.loc[df['zipcode'].isnull(), 'zipcode'] = 0
  print(len(df.loc[df['zipcode'].isnull()]))

fill_missing_zipcode_using_address()


