import read_transaction_data as rtd

import pandas as pd
import numpy as np
import string
import random

def get_transactions(filename):
    return rtd.read(filename)
# user can choose clientid from list of all clients
def get_client_id(clients):

    print('clients:')
    for i in range(len(clients)):
        print(i, clients[i])
    
    print('\nchoose client index [ 0 :',len(clients) - 1,']')
    id = clients[int(input())]
    print('client', id)

    return id

# calculate and save cashback amount for each transaction 
# depending on a list of client's categories and discounts
def calc_cashback(dataframe):

    client_categories = rtd.read('client-categories.csv')

    dataframe.cashback = 0

    for row_index, row in dataframe.iterrows():
        
        clientid = row.clientid
        category = row.mccgrp

        one_client_categories = client_categories[client_categories.clientid == clientid]

        if category in one_client_categories.category.values:
            this_cat_info = one_client_categories[one_client_categories.category == category].head(1)
            discount = int(this_cat_info.discount)
        else:
            discount = 1

        dataframe.at[row_index, 'cashback'] = row.withdamt * discount / 100
    
    rtd.save(dataframe, 'one-client-transactions.csv')
    return dataframe

if __name__== "__main__":

    #get all transactions
    data = rtd.read('data-selected.csv') 

    client_list = list(data.clientid.unique())
    clientid = get_client_id(client_list)
    
    # add and fill a column 'cashback'
    dataframe = calc_cashback(data)
    rtd.save(dataframe, 'test-whole-data-cashback.csv')