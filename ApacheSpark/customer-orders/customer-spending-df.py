from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('customer-spending').master('local').getOrCreate()

schema = StructType([ \
    StructField('customer_id', StringType(), True), \
    StructField('item_id', IntegerType(), True), \
    StructField('amount', FloatType(), True) \
    ])

# Read the file as dataframe
df = spark.read.schema(schema).csv('customer-orders.csv')
df.printSchema()

totalByCustomer = df.groupBy('customer_id').agg(func.round(func.sum('amount'), 2).alias('total_spent'))

totalByCustomerSorted = totalByCustomer.sort('total_spent')

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()



