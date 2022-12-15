from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

schema = StructType([ \
    StructField('user_id', IntegerType(), True), \
    StructField('movie_id', IntegerType(), True), \
    StructField('rating', IntegerType(), True), \
    StructField('timestamp', LongType(), True)
])

df = spark.read.option('sep', '\t').schema(schema).csv('ml-100k/u.data')

topMoviesDf = df.groupBy('movie_id').count().orderBy(func.desc('count'))

topMoviesDf.show(10)

spark.stop()