from pyspark.sql import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import functions as f

import os
import shutil

def read(filename):
    return spark.read.csv(filename,  inferSchema = True, header = True)

# yet this stuff won't work the way I want it to
def save(dataframe, filename):
    dataframe.coalesce(1).write.csv(filename, header=True, sep=',')

    # for file in os.listdir(filename):
    #     print('file:',file)
    #     if file.endswith('.csv'):
    #         new_dir_file = '~/transactions/'
    #         print('this file path: ', os.path.dirname(__file__))
    #         # shutil.move(file, new_dir_file)
    #         break
    # file_with_path = os.path.join(filename, '/*.csv')
    # print('file with path', file_with_path)
    # shutil.move(file_with_path, filename)
    
# get list of all clients
def get_client_list(dataframe):

    client_list = dataframe.select('clientid').distinct().rdd.map(lambda r: r[0]).collect()
    return client_list

# choose a client who you want to calculate cashback for
def get_client_id(client_list):

    print('\nclients:')
    [print(client) for client in client_list]

    clientid = input('choose client id: ')

    return clientid

# get transactions made by selected client
def get_one_client_transactions(dataframe, clientid):

    one_client_transations = dataframe.filter(dataframe.clientid == clientid)
    return one_client_transations

# still nothing in here
def calc_cashback(dataframe):

    client_categories = read('client-categories.csv')

    new_dataframe = dataframe.withColumn('cashback', f.lit(1))
    new_dataframe = new_dataframe.withColumn('cashback', new_dataframe.cashback*new_dataframe.withdamt/100)
    new_dataframe.show(10)
    

def test():
    test_df = read('client-categories.csv')
    # print('test_df:', test_df)

    # new_df = spark.createDataFrame([[0],[1],[2],[3],[4],[5],[6],[7]], ['new_values'])
    # new_df.show()

    test_df = test_df.withColumn('numbers', f.lit(1)*test_df.withdamt/100).show()
    # print('test_df modified:', test_df)


if __name__== "__main__":

    spark = SparkSession.builder.master("local").appName("calculate cashback").config(conf=SparkConf()).getOrCreate()

    # dataset containing preprocessed transaction info
    filename = 'test-whole-data-cashback.csv'
    dataframe = read(filename)

    client_list = get_client_list(dataframe)
    clientid = get_client_id(client_list)

    filtered_data = get_one_client_transactions(dataframe, clientid)
    filtered_data.show()