from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName('df-word-count').getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text('Book')

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

#Count up the occurences of each word
wordCounts= lowercaseWords.groupBy("word").count()

#Sort by counts
wordCountsSorted = wordCounts.sort("count")

#Show the results
wordCountsSorted.show(wordCountsSorted.count())


