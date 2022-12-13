from pyspark.sql import SparkSession

## Hints

spark = SparkSession.builder.appName('ff-exercise').getOrCreate()

data = spark.read.option('header', 'true').option('inferSchema', "true")\
    .csv("fakefriends-header.csv")

print('Here is our inferschema')
data.printSchema()

#Select age and friends column
print('Here is your dataframe')
friendsByAge = data.select(data[2], data[3]).show()
print(friendsByAge)

# From friendsByAge group "age" and then compute average number of friends for each age
print('Here is the average age')
friendsByAge.groupBy('age').avg('friends').show()

# Sort
friendsByAge.groupBy('age').avg('friends').sort("age").show()

# Format decimal places
friendsByAge.groupBy('age').agg(function.round(function.avg('friends'), 2)).sort('age').show()

# With custom column name
friendsByAge.groupBy('age').agg(function.round(function.avg('friends'), 2).alias("friends_avg")).sort("age").show()