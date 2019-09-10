from pyspark.sql import *
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import functions as f


import os
import shutil

print(__file__)

# test sparkSession
# s1 = SparkSession.builder.config("k1", "v1").getOrCreate()
# flag = s1.conf.get("k1") == s1.sparkContext.getConf().get("k1") == "v1"
# print(flag)

def read(filename):
    return spark.read.csv(filename,  inferSchema = True, header = True)

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

    os.path.dirname(r'home/developer/transactions')

def add_mcc_groups(dataframe, mcc_data):
    dataframe.withColumn("mccgrp", dataframe.mcc)
    return dataframe

def main():
    df = spark.read.csv('status-data.csv', inferSchema = True, header = True)
    df.show()

if __name__== "__main__":

    spark = SparkSession.builder.master("local").appName("test app").config(conf=SparkConf()).getOrCreate()
    
    filename = 'data-selected.csv'
    dataframe = read(filename)
    print(dataframe.columns)

    mcc_data = read('mcc-data-transformed.csv')

