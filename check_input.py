'''
  Check if the input data is skewed
  spark-submit check_input.py
'''
from pyspark import SparkContext
sc = SparkContext.getOrCreate()

customersDat = sc.textFile('hdfs://hadoop:9000/datain/customers.dat') # 123#AAA Inc#1 First Ave	Mountain View CA#94040
states = customersDat.map(lambda line: line.split('#'))\
    .filter(lambda x: len(x) == 4)\
    .map(lambda x: x[-2][-2:])\
    .map(lambda state: (state, 1))
print('customers.dat', states.values().stats())
for s in states.take(10):
    print(s)
salesDat = sc.textFile('hdfs://hadoop:9000/datain/sales.dat') # 1454313600#123#123456
customers = salesDat.map(lambda line: line.split('#'))\
    .filter(lambda x: len(x) == 3)\
    .map(lambda x: x[1])\
    .map(lambda customer: (customer, 1))
print('sales.dat', customers.values().stats())
for c in customers.take(10):
    print(c)
sc.stop()
