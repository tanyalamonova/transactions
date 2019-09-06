import pandas as pd
import numpy as np
import string
import datetime as dt
import random

def read(filename):
    return pd.read_csv(filename)

def save(dataframe, filename):
    dataframe.to_csv(filename, index=False, header=True)

def update_file(dataframe, filename):
    save(dataframe, filename)
    dataframe = read(filename)

def modify_raw_data(dataframe):

    # remove unnecessary columns
    dataframe = dataframe.fillna(0)
    dataframe = dataframe.drop(["TRANSACTION DETAILS","CHQ.NO","VALUE DATE","DEPOSIT AMT","."], axis=1)
    dataframe.columns = ['clientid','month', 'withdamt', 'balance']

    # clean data (remove unnecessary symbols)
    dataframe.clientid = dataframe.clientid.replace({"'": ''}, regex=True)
    dataframe.withdamt = dataframe.withdamt.replace({",": ''}, regex=True)
    dataframe.balance = dataframe.balance.replace({",": ''}, regex=True)

    # change column datatypes
    dataframe.clientid = dataframe.clientid.astype(int)
    dataframe.withdamt = dataframe.withdamt.astype(float)
    dataframe.balance = dataframe.balance.astype(float)
    dataframe.month = pd.to_datetime(dataframe.month).dt.strftime('%Y-%m')
    return dataframe

# remove all transactions that are not purchase
def remove_not_purchase(dataframe):
    return dataframe[dataframe.withdamt > 0]

# get only this month transactions
def get_this_month_transactions(dataframe, month):
    return dataframe[dataframe.month == month]

# create new df with status info:
# name,
# amount of categories suggested to a client, 
# amount of categories they can choose)
def create_status_data(status_names, suggest_amt, choose_amt):
    status_data = pd.DataFrame({'status': np.array(status_names), 'suggest': np.array(suggest_amt), 'choose': np.array(choose_amt)})
    save(status_data, 'status-data.csv')
    return status_data

# cuz raw mcc_data.csv is just disgusting
def get_transformed_mcc_data():
    mcc_data = read('mcc-data.csv')
    mcc_data = mcc_data[['mcccode', 'mccgrp']]
    mcc_data.columns = ['mcc', 'mccgrp']
    save(mcc_data, 'mcc-data-transformed.csv')
    return mcc_data

# generate random status for each client
def get_client_info(dataframe, status_list):
    client_info = pd.DataFrame(columns={'clientid','status'})
    client_info.clientid = dataframe.clientid.unique()
    client_info.status = np.random.choice(status_list, client_info.shape[0])
    save(client_info, 'client-info.csv')  

# nothing to add
def add_column(dataframe, column_name, values):
    dataframe[column_name] = values
    return dataframe

# generate random mcc_code for each transaction
def add_mcc_codes(dataframe, mcc_codes):
    rows = dataframe.shape[0]
    dataframe['mcc'] = np.random.choice(mcc_codes, rows)
    return dataframe

# change initial dataframe to make it easier to understand and work with
def prep_data():

    filename = 'bank-data.csv'
    dataframe = read(filename)
    dataframe = modify_raw_data(dataframe)

    dataframe = remove_not_purchase(dataframe)
    dataframe = add_mcc_codes(dataframe, mcc_codes)

    month = '2019-03'
    dataframe = get_this_month_transactions(dataframe, month)
    save(dataframe, 'data-selected.csv')
    return dataframe

# no need in it, just a place for playing and testing
def main():

    client_info = read('client-info.csv')

#generate a list of cashback categories and discounts for each client depending on their status
def add_client_categories(client_info):

    categories_df = pd.DataFrame(columns=['clientid', 'category', 'discount'])

    # get each client status and then again loop for 'choose' times to add categories and their discounts 
    for row in client_info.iterrows():

        groups = list(mcc_data.mccgrp.unique()) # got mcc groups shuffled
        random.shuffle(groups)

        client_id = row.clientid
        client_status = row.status

        temp = status_data[status_data.status == client_status].head(1)             
        client_cat_amt = temp.choose                                           # got how many categories a client has
        categories = list(groups[:client_cat_amt])                                  # generated n random categories for a client

        for value in categories:                                                    # and adding them into categoies_df immediately
            discount = random.randint(5, 20)
            new_row = pd.Series([client_id, value, discount], index = categories_df.columns)
            categories_df = categories_df.append(new_row, ignore_index=True)
            print('added a new row to categories_df:\n',new_row, '\n')
        
    save(categories_df, 'client-categories.csv')
    return categories_df

# get only one client transactions by filtering the main df
# total df clean by dropping 'balance' column
def get_one_client_transactions(dataframe, clientid):

    one_client_info = dataframe[dataframe.clientid == clientid]
    one_client_info = one_client_info.drop(['balance'], axis=1)
    save(one_client_info, 'one-client-transactions.csv')
    return one_client_info


def add_mcc_groups(dataframe, mcc_data):

    dataframe['mccgrp'] =''
    for row_index, row in dataframe.iterrows():

        mcc_info = mcc_data[mcc_data['mcc'] == row['mcc']].head(1)
        mcc_info = mcc_info.mccgrp.values
        dataframe.at[row_index,'mccgrp'] = mcc_info

    return dataframe



if __name__== "__main__":

    # get preprocessed data

    mcc_data = read('mcc-data-transformed.csv')
    mcc_groups = list(mcc_data['mccgrp'].unique())
    mcc_codes = list(mcc_data['mcc'])

    #no need in prepping data as it is ready
    # prep_data()

    #dataframe is a transaction list
    dataframe = read('data-selected.csv')
    dataframe = add_mcc_groups(dataframe, mcc_data)
    save(dataframe, 'data-selected.csv')

    status_data = read('status-data.csv')
    
    #get a random user
    clientid = 1196428

    client_info = read('client-info.csv')
    client_categories = read('client-categories.csv')

    one_client_transactions = get_one_client_transactions(dataframe, clientid)
