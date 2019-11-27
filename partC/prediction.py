import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.ml.linalg import Vectors

def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    return udf(to_array_, ArrayType(DoubleType()))(col)


# System Setting ==============================================================
isLocal = True
if isLocal:
    dir = "/Users/maoweng17/Documents/QMUL/BigDataProcessing/Lab/coursework/partC"
    pre_path = dir + "/input/out_preprocessing.csv"

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# Load Data ------------------------------------------------------------
sc= SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv') \
                    .options(header='false', inferschema='true') \
                    .load(pre_path) \
                    .toDF('time','price','total_trade_btc','n_trans')

# Transform into Vector for MlLib ---------------------------------------
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler(inputCols = ['total_trade_btc','n_trans'], outputCol = 'features')
df = vectorAssembler.transform(df) \
                    .select(['features', 'price'])

# Split into Train and Test dataset -------------------------------------
splits = df.randomSplit([0.7, 0.3])
train_df = splits[0]
test_df = splits[1]

# Build LinearRegression Model--------------------------------------------
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol = 'features', labelCol='price')
lr_model = lr.fit(train_df)
trainingSummary = lr_model.summary

# Check Result and Test Accuracy------------------------------------------
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

test_result = lr_model.evaluate(test_df)
print("RMSE on test data = %g" % test_result.rootMeanSquaredError)

# Get Prediction value of whole dataset -----------------------------------
lr_predictions = lr_model.transform(df) \
                         .select('prediction','price','features')
