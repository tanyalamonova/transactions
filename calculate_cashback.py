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
# depending on a client's category and discount
def calc_cashback(dataframe):

    client_info = rtd.read('full-client-info.csv')
    dataframe.cashback = 0

    for row_index, row in dataframe.iterrows():
        
        clientid = row.clientid
        trcategory = row.mccgrp

        client_cat_info = client_info[client_info.clientid == row.clientid].values[0]
        client_category = client_cat_info[2]

        if trcategory == client_category:
            discount = client_cat_info[3]
        else:
            discount = 1

        dataframe.at[row_index, 'cashback'] = row.withdamt * discount / 100
    
    rtd.save(dataframe, 'test-whole-data-cashback.csv')
    return dataframe

if __name__== "__main__":

    #get all transactions
    data = rtd.read('data-selected.csv') 

    # client_list = list(data.clientid.unique())
    # clientid = get_client_id(client_list)
    
    # add and fill a column 'cashback'
    dataframe = calc_cashback(data)
    # rtd.save(dataframe, 'test-whole-data-cashback.csv')