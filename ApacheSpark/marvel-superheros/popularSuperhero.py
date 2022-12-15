from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, LongType, ShortType

spark = SparkSession.builder.appName('popularSuperheros').getOrCreate()

schema = StructType([\
    StructField('id', IntegerType(), True), \
    StructField('name', StringType(), True)
])

names = spark.read.option('sep', '\t').schema(schema).csv('Marvel+Names')
lines = spark.read.text('Marvel+Graph')

connections = lines.withColumn('id', func.split(func.col('value'), " ")[0]) \
    .withColumn('connections', func.size(func.split(func.col('value'), " ")) - 1) \
    .groupBy('id').agg(func.sum('connections').alias('connections'))

mostPopular = connections.sort(func.col('connections').desc()).first()

mostPopularNames = names.filter(func.col('id') == mostPopular[0]).select('name').first()

print(mostPopularNames[0] + ' is the most popular superhero with ' + str(mostPopular[1]) + ' co-appearances.')

