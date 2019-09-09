import read_transaction_data as rtd

import pandas as pd
import numpy as np
import string
import random

def get_transactions(filename):
    return rtd.read(filename)

def calc_cashback(dataframe):
    client_categories = rtd.read('client-categories.csv')

    dataframe.cashback = 0

    for row_index, row in dataframe.iterrows():
        
        clientid = row.clientid
        category = row.mccgrp
        # print('\nthis category = ', category)
        # category = category.replace("[", '')
        # category = category.replace("]", '')
        # category = category.replace("'", '')
        one_client_categories = client_categories[client_categories.clientid == clientid]
        # print('one client categories:\n', one_client_categories)

        # print('first category = ', one_client_categories.at[0,'category'])
        # print('second category = ', one_client_categories.category.values[1])

        if category in one_client_categories.category.values:
            # print('THEY ARE EQUAL')
            this_cat_info = one_client_categories[one_client_categories.category == category].head(1)
            discount = int(this_cat_info.discount)
        else:
            discount = 1
        # print('discount = ', discount)
        dataframe.at[row_index, 'cashback'] = row.withdamt * discount / 100
        print(dataframe.at[row_index, 'withdamt'], '*', discount, '/ 100 = ', dataframe.at[row_index, 'cashback'])
    
    rtd.save(dataframe, 'one-client-transactions.csv')
    return dataframe

if __name__== "__main__":
    # print('hello, world!')
    
    clientid = 409000493210

    data = rtd.read('data-selected.csv') 

    client_list = list(data.clientid.unique())
    random.shuffle(client_list)

    clientid = client_list[0]
    print('clientid: ', clientid)

    dataframe = rtd.get_one_client_transactions(data, clientid)
    dataframe = calc_cashback(dataframe)
