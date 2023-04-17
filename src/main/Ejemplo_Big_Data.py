import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains

spark = SparkSession.builder.appName('MyFirstSession').getOrCreate()


df2 = spark.read.option("header",True) \
     .csv("/content/sample_data/california_housing_test.csv")
df2.printSchema()

df3 = spark.read.options(header='True', delimiter='|') \
  .csv("/content/sample_data/california_housing_test.csv")
df3.printSchema()


df3.show(truncate=False)