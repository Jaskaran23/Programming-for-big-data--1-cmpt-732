import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.mllib.util import MLUtils
#import org.apache.spark.mllib.util.MLUtils
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs,output):
    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
    ])

    data = spark.read.csv(inputs, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    sqlTrans = SQLTransformer(statement = 'select *,dayofyear(date) as day FROM __THIS__')
 
    sqlTrans1 = SQLTransformer(statement = 'SELECT today.station,today.date,today.latitude,today.longitude,today.elevation,today.tmax, yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station')
    assemble_features = VectorAssembler(inputCols = ['latitude','longitude','elevation','day','yesterday_tmax'], outputCol = 'features')

    gbt = GBTRegressor(featuresCol = 'features', labelCol='tmax')
    pipeline = Pipeline(stages=[sqlTrans1,sqlTrans,assemble_features,gbt])
    weather_model = pipeline.fit(train)

    predictions = weather_model.transform(validation)
    #predictions.show()
    evaluator = RegressionEvaluator(labelCol = 'tmax', predictionCol = 'prediction', metricName = 'rmse')
    score = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % score)

    weather_model.write().overwrite().save(output)
    
   
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
