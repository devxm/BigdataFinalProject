Using Python version 3.6.7 (default, Oct 22 2018 11:32:17)
SparkSession available as 'spark'



##################################### Number of Parking Violation By NY state and Others #######################################################

import sys
from pyspark import SparkConf, SparkContext
from csv import reader
line1 = sc.textFile("/mnt/c/Users/akhil/Desktop/nyc-parking-tickets/FullData.csv")
line1 = line1.mapPartitions(lambda x: reader(x))
state = line1.map(lambda x: (("NY" if str(x[2]) =="NY" else "Other" ),1)).reduceByKey(lambda x, y: x + y)
state.take(10)



##################################### Number of Parking Violation By vehicle Make #############################################################



import sys
from pyspark import SparkConf, SparkContext
from csv import reader
id3 = line1.map(lambda x: ((x[7]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
VehicleMake = sc.parallelize(id3.take(10)).map(lambda x: (x[0], x[1]))
VehicleMake.take(10)


##################################### Number of Parking Violation By Registration State #######################################################

import sys
from pyspark import SparkConf, SparkContext
from csv import reader
id4 = line1.map(lambda x: ((x[2]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
violationcodesByState = sc.parallelize(id4.take(10)).map(lambda x: (x[0], x[1]))
violationcodesByState.take(10)


##################################### Number of Parking Violation By Plate ID and Number of Parking Violation count ############################

import sys
from pyspark import SparkConf, SparkContext
from csv import reader
id1 = line1.map(lambda x: ((x[1]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
topviolator1 = sc.parallelize(id1.take(5)).map(lambda x: (x[0], x[1]))
topviolator1.take(5)



##################################### Number of Parking Violation By Street and Number of Parking Violation count ###############################


import sys
from pyspark import SparkConf, SparkContext
from csv import reader
id2 = line1.map(lambda x: ((x[24]),1)).reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], False)
 topviolatorByStreet = sc.parallelize(id2.take(10)).map(lambda x: (x[0], x[1]))
topviolatorByStreet.take(10)




