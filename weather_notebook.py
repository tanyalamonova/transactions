# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
# ms-python.python added
import os
try:
	os.chdir(os.path.join(os.getcwd(), '../Bank'))
	print(os.getcwd())
except:
	pass

#%%
from pyspark.sql import functions as f

data = spark.read.csv("weather.csv", sep=";", header=True, inferSchema=True)
data = data.withColumn('date', data.date.substr(4,7))

result_data = data.groupby('city','date').agg(f.min('temperature').alias('min'),f.max('temperature').alias('max'),f.avg('temperature').alias('avg')).withColumn('avg', f.round('avg',2)).orderBy('city','date').show()

result_data.coalesce(1).write.csv('result.csv', header=True, sep=';')


