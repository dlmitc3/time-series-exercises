import os
import pandas as pd
import numpy as np
import requests

def items_data():

    # establish a filename for the local csv
    filename = 'items.csv'
    # check to see if a local copy already exists. 
    if os.path.exists(filename):
        print('Reading from local CSV...')
        # if so, return the local csv
        return pd.read_csv(filename)
    # otherwise, pull the data from the api:
    
    # establish domain and endpoint for usl
    domain = 'https://api.data.codeup.com'
    endpoint = '/api/v1/items'
    print('No local csv available. \nReading data from ', domain + endpoint, '...')
    
    # establish empty list for storing data
    items = []
    
    # establish counter for printing progress report
    page_number = 0
    
    # check whether there is another page of data to be read
    while endpoint != None:
        # add one to the counter
        page_number += 1
        # re-establish url as the next page
        url = domain + endpoint
        # make the request
        response = requests.get(url)
        # read the response as json
        data = response.json()
        # add items to the list
        items.extend(data['payload']['items'])
        # print progress report
        print('page # ', page_number, ' data acquired. ', len(items), ' items in list.')
        # establish url for the next page
        endpoint = data['payload']['next_page']
    # print results
    print(page_number, 'page(s) acquired. No more pages available.')

    # create a dataframe
    df = pd.DataFrame(items)
    
    # store a copy as a local csv
    print('Writing to local file ', filename, '...')
    df.to_csv('items.csv', index=False)
    
    # return the resulting dataframe
    return df

def stores_data():

    # establish a filename for the local csv
    filename = 'stores.csv'
    # check to see if a local copy already exists. 
    if os.path.exists(filename):
        print('Reading from local CSV...')
        # if so, return the local csv
        return pd.read_csv(filename)
    # otherwise, pull the data from the api:
    
    # establish domain and endpoint for usl
    domain = 'https://api.data.codeup.com'
    endpoint = '/api/v1/stores'
    print('No local csv available. \nReading data from ', domain + endpoint, '...')
    
    # establish empty list for storing data
    stores = []
    
    # establish counter for printing progress report
    page_number = 0
    
    # check whether there is another page of data to be read
    while endpoint != None:
        # add one to the counter
        page_number += 1
        # re-establish url as the next page
        url = domain + endpoint
        # make the request
        response = requests.get(url)
        # read the response as json
        data = response.json()
        # add items to the list
        stores.extend(data['payload']['stores'])
        # print progress report
        print('page # ', page_number, ' data acquired. ', len(stores), ' stores in list.')
        # establish url for the next page
        endpoint = data['payload']['next_page']
    # print results
    print(page_number, 'page(s) acquired. No more pages available.')

    # create a dataframe
    df = pd.DataFrame(stores)
    
    # store a copy as a local csv
    print('Writing to local file ', filename, '...')
    df.to_csv('stores.csv', index=False)
    
    # return the resulting dataframe
    return df

def sales_data():

    # establish a filename for the local csv
    filename = 'sales.csv'
    # check to see if a local copy already exists. 
    if os.path.exists(filename):
        print('Reading from local CSV...')
        # if so, return the local csv
        return pd.read_csv(filename)
    # otherwise, pull the data from the api:
    
    # establish domain and endpoint for usl
    domain = 'https://api.data.codeup.com'
    endpoint = '/api/v1/sales'
    print('No local csv available. \nReading data from ', domain + endpoint, '...')
    
    # establish empty list for storing data
    sales = []
    
    # establish counter for printing progress report
    page_number = 0
    
    # check whether there is another page of data to be read
    while endpoint != None:
        # add one to the counter
        page_number += 1
        # re-establish url as the next page
        url = domain + endpoint
        # make the request
        response = requests.get(url)
        # read the response as json
        data = response.json()
        # add items to the list
        sales.extend(data['payload']['sales'])
        # print progress report
        print('page # ', page_number, ' data acquired. ', len(sales), ' sales in list.')
        # establish url for the next page
        endpoint = data['payload']['next_page']
    # print results
    print(page_number, 'page(s) acquired. No more pages available.')

    # create a dataframe
    df = pd.DataFrame(sales)
    
    # store a copy as a local csv
    print('Writing to local file ', filename, '...')
    df.to_csv(filename, index=False)
    
    # return the resulting dataframe
    return df

def combine_heb_data(sales, items, stores):
    '''
    Combines sales, items, stores df's (in that order)
    Returns combined df.
    '''
    sales = sales.rename(columns={'item': 'item_id', 'store': 'store_id'})
    sales_items = items.merge(sales, on='item_id', how='left')
    stores_sales_items = sales_items.merge(stores, on='store_id', how='left')
    return stores_sales_items

def opsd_germany_data():
    
    # establish a filename for the local csv
    filename = 'opsd_germany.csv'
    # check to see if a local copy already exists. 
    if os.path.exists(filename):
        print('Reading from local CSV...')
        # if so, return the local csv
        return pd.read_csv(filename)
    # otherwise, pull the data from the url:
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    print('No local copy exists. Reading data from: ', url, '...')
    df = pd.read_csv(url)
    # write to local csv for cache
    print('Writing to local csv...')
    df.to_csv(filename, index=False)
    
    return df

def get_combined_store_data():
    sales = sales_data()
    items = items_data()
    stores = stores_data()
    df = combine_heb_data(sales, items, stores)
    return df