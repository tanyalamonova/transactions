from pyspark.sql import *
from pyspark import SparkContext
from pyspark import SparkConf

# from pyspark.ml.classification import NaiveBayes
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# from pyspark.ml import Pipeline
# from pyspark.ml.regression import GBTRegressor

from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

from pyspark.ml.feature import VectorIndexer
from pyspark.ml.feature import VectorAssembler

# from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.linalg import Vectors

import spark_transform_data as sparktd
import read_transaction_data as rtd

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import axes3d
import numpy as np

# .csv dataset has to be transformed to a libsvm kind of data to be used my the machine
# I currently have no idea how to do that
def prep_dataset(dataframe):

    print('prepping dataset...')
    assembler = VectorAssembler(inputCols=['clientid', 'withdamt', 'cashback'], outputCol='features')
    output = assembler.transform(dataframe)
    
    return output

# naive bayes algorithm
def run_model(dataframe):

    # steps taken from spark examples

    splits = dataframe.randomSplit([0.6, 0.4])
    train = splits[0]
    test = splits[1]

    nb = NaiveBayes(smoothing=1.0, modelType='multinomial')

    model = nb.fit(train)
    predictions = model.transform(test)
    predictions.show()

    evaluator = MulticlassClassificationEvaluator(labelCol='clientid', predictionCol='prediction', metricName='accuracy')
    accuracy = evaluator.evaluate(predictions)

    print("Test set accuracy =", str(accuracy))

# gradient boosted tree algorithm
def try_gradient_boosted_tree(dataframe):

    # steps taken from spark examples

    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10).fit(dataframe)
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

def kmeans_algorithm(dataframe):


    kmeans = KMeans().setK(5).setSeed(1)
    model = kmeans.fit(dataframe)

    predictions = model.transform(dataframe)

    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print("Silhouette with squared euclidean distance = " + str(silhouette))

    centers = model.clusterCenters()
    print("Cluster Centers: ")

    for center in centers:
        print(center)

# just an example of showing a scatter plot    
def show_3d_plot():
    # Create data
    N = 60
    g1 = (0.6 + 0.6 * np.random.rand(N), np.random.rand(N),0.4+0.1*np.random.rand(N))
    g2 = (0.4+0.3 * np.random.rand(N), 0.5*np.random.rand(N),0.1*np.random.rand(N))
    g3 = (0.3*np.random.rand(N),0.3*np.random.rand(N),0.3*np.random.rand(N))

    print('g1:\n', type(g1))
    print('g2:\n', type(g2))
    print('g3:\n', type(g3))
    data = (g1, g2, g3)
    colors = ("red", "green", "blue")
    groups = ("coffee", "tea", "water") 

    # Create plot
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    ax = fig.gca(projection='3d')

    for data, color, group in zip(data, colors, groups):
        x, y, z = data
        ax.scatter(x, y, z, alpha=0.8, c=color, edgecolors='none', s=30, label=group)

    plt.title('Matplot 3d scatter plot')
    plt.legend(loc=2)
    plt.show()





if __name__== "__main__":

    spark = SparkSession.builder.master("local").appName("ml").config(conf=SparkConf()).getOrCreate()

    dataframe_path = 'data-transformed.csv'
    test_path = 'client_info.csv'

    dataframe = sparktd.read(dataframe_path, spark)
    dataframe.cache()

    # show_3d_plot()

    dataframe = prep_dataset(dataframe)

    kmeans_algorithm(dataframe)