from pyspark.sql import *
from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

import spark_transform_data as sparktd

# .csv dataset has to be transformed to a libsvm kind of data to be used my the machine
# I currently have no idea how to do that
def prep_dataset(dataframe):
    print()

# naive bayes algorithm
def run_model(dataframe):

    # steps taken from spark examples

    splits = dataframe.randomSplit([0.6, 0.4])
    train = splits[0]
    test = splits[1]

    nb = NaiveBayes(smoothing=1.0, modelType='multinomial')

    model = nb.fit(train, params=train.clientcat)
    predictions = model.transform(test)
    predictions.show()

    evaluator = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)

    print("Test set accuracy =", str(accuracy))

# gradient boosted tree algorithm
def try_gradient_boosted_tree(dataframe):

    # steps taken from spark examples

    featureIndexer = VectorIndexer(inputCol="clientid", outputCol="indexedFeatures", maxCategories=10).fit(dataframe)
    (trainingData, testData) = dataframe.randomSplit([0.7, 0.3])
    gbt = GBTRegressor(featuresCol="indexedFeatures", maxIter=10)

    pipeline = Pipeline(stages=[featureIndexer, gbt])
    model = pipeline.fit(trainingData)

    predictions = model.transform(testData)
    predictions.select("prediction", "label", "features").show(5)

    evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    gbtModel = model.stages[1]
    print(gbtModel)

if __name__== "__main__":

    spark = SparkSession.builder.master("local").appName("ml").config(conf=SparkConf()).getOrCreate()

    dataframe = sparktd.read('data-transformed.csv', spark)
    dataframe.show()

    # try_gradient_boosted_tree(dataframe)
    # run_model(dataframe)