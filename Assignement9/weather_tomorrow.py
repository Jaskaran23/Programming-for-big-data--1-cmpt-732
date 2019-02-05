import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('tmax model tester').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator





def test_model(model_file):
    # get the data
    data = spark.createDataFrame([('sfustation','2018-11-12',49.2771,-122.9146,330,12.0,316),('sfustation','2018-11-13',49.2771,-122.9146,330,0.0,317)],['station','date','latitude','longitude','elevation','tmax','day'])

    # load the model
    model = PipelineModel.load(model_file)
    
    # use the model to make predictions
    predictions = model.transform(data).collect()
    #predictions.show()
    #print(predictions[0][9])
    prediction = predictions[0][9]
    print('Predicted tmax tomorrow:', prediction)
    

    # If you used a regressor that gives .featureImportances, maybe have a look...
    #print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
