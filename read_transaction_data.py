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

    #remove unnecessary columns
    dataframe = dataframe.fillna(0)
    dataframe = dataframe.drop(["TRANSACTION DETAILS","CHQ.NO","VALUE DATE","DEPOSIT AMT","."], axis=1)
    dataframe.columns = ['clientid','month', 'withdamt', 'balance']

    #clean data (remove unnecessary symbols)
    dataframe.clientid = dataframe.clientid.replace({"'": ''}, regex=True)
    dataframe.withdamt = dataframe.withdamt.replace({",": ''}, regex=True)
    dataframe.balance = dataframe.balance.replace({",": ''}, regex=True)

    #change column datatypes
    dataframe.clientid = dataframe.clientid.astype(int)
    dataframe.withdamt = dataframe.withdamt.astype(float)
    dataframe.balance = dataframe.balance.astype(float)
    dataframe.month = pd.to_datetime(dataframe.month).dt.strftime('%Y-%m')
    return dataframe

def remove_not_purchase(dataframe):
    return dataframe[dataframe.withdamt > 0]

def get_this_month_transactions(dataframe, month):
    return dataframe[dataframe.month == month]

def create_status_data(status_names, suggest_amt, choose_amt):
    status_data = pd.DataFrame({'status': np.array(status_names), 'suggest': np.array(suggest_amt), 'choose': np.array(choose_amt)})
    update_file(status_data, 'status-data.csv')
    return status_data

def get_transformed_mcc_data():
    mcc_data = read('mcc-data.csv')
    mcc_data = mcc_data[['mcccode', 'mccgrp']]
    mcc_data.columns = ['mcc', 'mccgrp']
    save(mcc_data, 'mcc-data-transformed.csv')
    return mcc_data

def get_client_info(dataframe, status_list):
    client_info = pd.DataFrame(columns={'clientid','status'})
    client_info.clientid = dataframe.clientid.unique()
    client_info.status = np.random.choice(status_list, client_info.shape[0])
    update_file(client_info, 'client-info.csv')  

def add_column(dataframe, column_name, values):
    dataframe[column_name] = values
    return dataframe

def add_mcc_codes(dataframe, mcc_codes):
    rows = dataframe.shape[0]
    dataframe['mcc'] = np.random.choice(mcc_codes, rows)
    return dataframe

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


def main():
    dataframe = prep_data()

    # mcc_data = get_transformed_mcc_data()
    

    # status_names = ['basic','silver','gold','platinum']
    # status_suggest_amt = [2,3,5,6]
    # status_choose_amt = [1,1,2,3]

    client_info = read('client-info.csv')
    # client_info = pd.DataFrame(columns={'clientid','status'})
    # client_info.clientid = dataframe.clientid.unique()
    # client_info.status = np.random.choice(status_list, client_info.shape[0])
    # update_file(client_info, 'client-info.csv')

    client_categories = pd.DataFrame(columns={'clientid','category'})
    client_categories.clientid = dataframe.clientid.unique()
    client_categories = pd.merge(client_categories, client_info, on='clientid')

    client_categories_full = pd.merge(client_categories, status_data, on='status')
    update_file(client_categories_full, 'client-categories-full.csv')
    print('cats full:\n', client_categories_full)


def add_client_categories(client_info):

    categories_df = pd.DataFrame(columns=['clientid', 'category', 'discount'])

    # get each client status and then again loop for 'choose' times to add categories and their discounts 
    for row in client_info.iterrows():

        groups = list(mcc_data.mccgrp.unique()) # got mcc groups shuffled
        random.shuffle(groups)

        client_id = row.clientid
        print('client id = ', client_id,)
        client_status = row.status
        print('status = ', client_status,)
        temp = status_data[status_data.status == client_status].head(1)             # got how many categories a client has
        client_cat_amt = int(temp.choose)
        print('client_cat_amt = ', client_cat_amt,)
        categories = list(groups[:client_cat_amt])                                  # generated n random categories for a client
        print('categories = ', categories, '\n')

        for value in categories:                                                    # and adding them into categoies_df immediately
            discount = random.randint(5, 20)
            print(value, ' discount = ', discount)
            new_row = pd.Series([client_id, value, discount], index = categories_df.columns)
            categories_df = categories_df.append(new_row, ignore_index=True)
            print('added a new row to categories_df:\n',new_row, '\n')
        
    save(categories_df, 'client-categories.csv')

                                                                                    


    # for row_index, row in client_categories.iterrows():
    #     groups = list(mcc_data['mccgrp'].unique())
    #     random.shuffle(groups)
    #     cats = list(groups[:row.choose])
    #     discounts = np.random.randint(5, 20, size=row.choose)
    #     client_categories.category
    #     cat_info = pd.DataFrame({'cat': cats, 'discount': discounts})
    #     # print('client [', row.clientid, '] = ', cat_info.values)
    #     client_categories.at[row_index,'category'] = cat_info
    #     # print('client categories:\n', client_categories.at[row_index,'category'])
    return categories_df

def get_one_client_transactions(dataframe, clientid):
    one_client_info = dataframe[dataframe.clientid == clientid]
    one_client_info.mccgrp = ''
    one_client_info = one_client_info.drop(['balance'], axis=1)
    # print('one_client_info: \n', one_client_info)
    save(one_client_info, 'one-client-transactions.csv')
    return one_client_info

# def add_mcc_groups_to_client(one_client_info):
#     for row_index,row in one_client_info.iterrows():
#         gr = mcc_data[mcc_data.mcc == row.mcc].head(1)
#         # print('gr---> ',gr.mcc.values)
#         one_client_info.at[row_index,'mccgrp'] = gr.mccgrp.values

#     save(one_client_info, 'one-client-info-full.csv')
#     return one_client_info


def add_mcc_groups(dataframe, mcc_data):

    dataframe['mccgrp'] =''
    for row_index, row in dataframe.iterrows():
        print(type(mcc_data.at[row_index,'mcc']))
        mcc_info = mcc_data[mcc_data['mcc'] == row['mcc']].head(1)
        # print('row[mcc] type = ', type(row['mcc']))
        mcc_info = mcc_info.mccgrp.values
        print('mcc info = ', mcc_info)
        dataframe.at[row_index,'mccgrp'] = mcc_info
        print('df grp = ', dataframe.at[row_index,'mccgrp'])
        # row['mcc']
        # grp = mcc_info.mccgrp
        # dataframe['mccgrp'] = grp

    return dataframe



if __name__== "__main__":

    mcc_data = read('mcc-data-transformed.csv')
    mcc_groups = list(mcc_data['mccgrp'].unique())
    mcc_codes = list(mcc_data['mcc'])

    prep_data()

    dataframe = read('data-selected.csv')
    dataframe = add_mcc_groups(dataframe, mcc_data)
    save(dataframe, 'data-selected.csv')
    
    status_data = read('status-data.csv')
    

    clientid = 1196428

    client_info = read('client-info.csv')

    # client_categories = add_client_categories(client_info)
    client_categories = read('client-categories.csv')
    # print('client categories:\n', client_categories)

    one_client_transactions = get_one_client_transactions(dataframe, clientid)
    print('client ', clientid, 'transactions count = ', one_client_transactions.shape[0])

    # one_client_info = read('one-client-info-full.csv')
    # # one_client_info = add_mcc_groups(one_client_info)
    # # print('mcc data: ', mcc_data.columns)
    # # print(mcc_data.mcc.values)

    # # one_client_info.cashbacksum = 0
    # # update_file(one_client_info, 'one-client-info-full.csv')

    # one_client_categories_info = client_categories_full[client_categories_full.clientid == clientid]
    # one_client_categories = pd.DataFrame(one_client_categories_info.category)
    # # one_client_categories = one_client_categories[0]
    # # temp = pd.DataFrame(one_client_categories)
    # temp = pd.DataFrame(one_client_categories)
    # print('one_client_categories.values', temp)
    # print('client categories -->:\n',one_client_categories.columns)
    # one_client_cats_no_discs = one_client_categories
    # # one_client_cats_no_discs = np.delete(one_client_cats_no_discs, 1, 1)
    # # print(len(one_client_cats_no_discs))