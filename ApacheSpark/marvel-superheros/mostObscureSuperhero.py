from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, LongType, ShortType

spark = SparkSession.builder.appName('popularSuperheros').getOrCreate()

schema = StructType([\
    StructField('id', IntegerType(), True), \
    StructField('name', StringType(), True)
])

names = spark.read.schema(schema).option('sep', ' ').csv('Marvel+Names')
lines = spark.read.text('Marvel+Graph')

connections = lines.withColumn('id', func.split(func.col('value'), " ")[0]) \
    .withColumn('connections', func.size(func.split(func.col('value'), " ")) - 1) \
    .groupBy('id').agg(func.sum('connections').alias('connections'))

##leastPopular = connections.sort(func.col('connections').asc()).first()
minConnectionsCount = connections.agg(func.min('connections')).first()[0]
minConnections = connections.filter(func.col('connections') == minConnectionsCount)

minConnectionsWithName = minConnections.join(names, 'id')

print('The following Marvel superhero only has ' + str(minConnectionsCount) + ' connections.')

minConnectionsWithName.select('name').show()

