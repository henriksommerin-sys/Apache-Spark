from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('test')
sc = SparkContext(conf = conf)

def ParseLines(lines):
    fields = lines.split(',')
    id = int(fields[0])
    amount = float(fields[2])
    return (id, amount)

lines = sc.textFile('customer-orders.py')
rdd = lines.map(ParseLines)
totalAmount = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
results = totalAmount.collect()

for result in results:
    print(result)