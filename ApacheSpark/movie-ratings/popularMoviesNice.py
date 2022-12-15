from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open('ml-100k/u.item', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([\
    StructField('user_id', IntegerType(), True), \
    StructField('movie_id', IntegerType(), True), \
    StructField('rating', IntegerType(), True), \
    StructField('timestamp', LongType(), True)
])

df = spark.read.option('sep', '\t').schema(schema).csv('ml-100k/u.data')

movieCount = df.groupBy('movie_id').count()

def lookupName(movie_id):
    return nameDict.value[movie_id]

lookupNameUDF = func.udf(lookupName)

moviesWithName = movieCount.withColumn('movieTitle', lookupNameUDF(func.col("movie_id")))

sortedMoviesWithName = moviesWithName.orderBy(func.desc('count'))

sortedMoviesWithName.show(10, False)

spark.stop()

