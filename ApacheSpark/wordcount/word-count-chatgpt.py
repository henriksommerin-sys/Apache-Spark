from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder \
    .appName("WordCount") \
    .getOrCreate()

# Read the text file into a DataFrame
df = spark.read.text("Book")

# Split the lines into words using a regular expression that extracts words
words = df.select(split(col("value"), "\s+").alias("words"))

# Remove any empty strings
words = words.select(array_remove(col("words"), "").alias("words"))

# Normalize the words to lowercase
lowercaseWords = words.select(f.lower(col("words")).alias("words"))

# Count the occurrences of each word
wordCounts = lowercaseWords.select(explode(col("words")).alias("word")) \
    .groupBy("word") \
    .count()

# Sort the words by count
sortedWordCounts = wordCounts.orderBy(desc("count"))

# Print the results
sortedWordCounts.show()