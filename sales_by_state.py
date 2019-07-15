'''
  spark-submit sales_by_state.py
'''
from pyspark import SparkContext
from collections import namedtuple
from datetime import datetime
from datetime import timezone

sc = SparkContext.getOrCreate()

def parseCustomers(line):
    # 123#AAA Inc#1 First Ave	Mountain View CA#94040
    parts = line.split('#')
    if len(parts) is not 4:
        return(-1, '') # ignore empty lines and bad data
    cid = int(parts[0])
    state = parts[-2][-2:]
    return (cid, state)

customersRdd = sc.textFile('hdfs://hadoop:9000/datain/customers.dat').map(parseCustomers) \
    .filter(lambda x: x[0] is not -1)

# TODO broadcast only if number of customers is finite and fits in memory
# customersDat = customersRdd.partitionBy(1).cache()
# broadcastCustomers = sc.broadcast(customersDat)

def parseSales(line):
    # 1454313600#123#123456
    parts = line.split('#')
    if len(parts) is not 3:
        return(-1, (0, 0)) # ignore empty lines and bad data
    d1 = datetime.fromtimestamp(int(parts[0]))
    # round date to the hour tz=timezone.utc
    import datetime as dtmod
    dttm = d1 + dtmod.timedelta(hours = 1, minutes = -d1.minute, seconds = -d1.second, microseconds = -d1.microsecond)
    cid = int(parts[1])
    amount = int(parts[2])
    return (cid, (dttm, amount))

salesRdd = sc.textFile('hdfs://hadoop:9000/datain/sales.dat').map(parseSales) \
    .filter(lambda x: x[0] is not -1)

# TODO consider partitioning before the join
hourRdd = salesRdd.join(customersRdd) \
    .map(lambda x: ((x[1][1], x[1][0][0]), x[1][0][1])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0], x[0][1].year, x[0][1].month, x[0][1].day, x[0][1].hour, x[1]))
    # ('AK', 2016, 8, 1, 11, 123458)

# TODO consider caching the hourRdd as it is reused

stateRdd = hourRdd.map(lambda x: (x[0], x[5])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0], 0, 0, 0, 0, x[1]))
    # ('AK', 0, 0, 0, 0, 123458)

dayRdd = hourRdd.map(lambda x: ((x[0], x[1], x[2], x[3], 0), x[5])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[1]))
    # ('AK', 2016, 8, 1, 0, 123458)

monthRdd = dayRdd.map(lambda x: ((x[0], x[1], x[2], 0, 0), x[5])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[1]))
    # ('AL', 2016, 8, 0, 0, 123459)

yearRdd = dayRdd.map(lambda x: ((x[0], x[1], 0, 0, 0), x[5])) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[0][4], x[1]))
    # ('AL', 2016, 0, 0, 0, 123459)

combinedRdd = hourRdd.union(dayRdd) \
    .union(monthRdd) \
    .union(yearRdd) \
    .union(stateRdd) \
    .sortBy(lambda x: (x[0], -x[1], -x[2], -x[3], -x[4]), True)

def formatOutput(row):
    s = '{} {} {} {} {} {}'.format(*row)
    # return s # uncomment for a cleaner output
    return s.replace(' 0', '#').replace(' ', '#')

formattedRdd = combinedRdd.map(formatOutput)
formattedRdd.coalesce(1).saveAsTextFile('dataout/sales_by_state.txt') # save the output locally
formattedRdd.saveAsTextFile('hdfs://hadoop:9000/dataout/output') # also save the output in hadoop

for row in formattedRdd.take(100):
    print(row)

sc.stop()
