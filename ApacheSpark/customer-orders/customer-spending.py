from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('Total-spending')
sc = SparkContext(conf = conf)

def ParseLines(line):
    fields = line.split(',')
    id = int(fields[0])
    amount = float(fields[2])
    return (id, amount)

lines = sc.textFile('customer-orders.csv')
rdd = lines.map(ParseLines)
total_amount = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
total_amount_sorted = total_amount.map(lambda x: (x[1], x[0])).sortByKey()

results = total_amount_sorted.collect()

for result in results:
    print(result)


