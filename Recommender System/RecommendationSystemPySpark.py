#####################################################################################################
# 							CSP554 - BigData Technologies Final Project								#
#####################################################################################################
# Team					: 	BigData Developers														#
# Team Members			:	Akhil Padgilwar (A20427219), Ganga Gudi (A20428842)						#
#							Ridhima Bhalerao (A20422550), Sagar Ippili (A20417999)					#
# Project				: 	Predictive & Descriptive Analytics on NYC Parking Violations			#
#							(2016, 2017, 2018, 2019)												#
#####################################################################################################
# Recommender Systems	:	Predicting the violation codes that one can get ticketed when parked	#
#							on a particular street using PySpark, MlLib, ALS Recommender			#
#####################################################################################################

#Importing all the necessary modules for DataRead, DataParse, DataPreprocess, ColumnIndexing, ColumnScaling/Normalizing, countDistinct, Aggregations, SQLContext,
#Machine Learning capabilities(MlLib), Recommender Systems (ALS), ModelEvaluation (RegressionEvaluator)
import numpy as np
import pandas as pd
from pyspark.ml.feature import StringIndexer
from pyspark.ml.recommendation import ALS,ALSModel
from pyspark.mllib.evaluation import RegressionMetrics,RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import IndexToString
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import countDistinct
from pyspark.sql import functions as F
from pyspark.sql import SQLContext

#Import path to data files
path = '/mnt/c/Users/sagar/Desktop/Temp/nyc-parking-tickets/'

#Read Violation Code description's CSV file into PySpark Dataframe
violationCodeCSV = pd.read_csv(path+"ViolationCodes.csv",usecols=['CODE','DEFINITION'])
violationCodeDictionary = violationCodeCSV.set_index('CODE')['DEFINITION'].to_dict()

#Read 3 years of NYC Parking Violations' CSV file intp PySpark Dataframe
df2016 = spark.read.csv(path+"Yearly/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", inferSchema=True, header=True) 
df2017 = spark.read.csv(path+"Yearly/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", inferSchema=True, header=True) 
df2018 = spark.read.csv(path+"Yearly/Parking_Violations_Issued_-_Fiscal_Year_2018.csv", inferSchema=True, header=True)  

#Take 3 years of data(2016, 2017 & 2018) for training and 2019's data for testing
dfTrain = df2016.union(df2017)  
dfTrain = dfTrain.union(df2018) 
dfTest = spark.read.csv(path+"Yearly/Parking_Violations_Issued_-_Fiscal_Year_2019.csv", inferSchema=True, header=True) 

#Selecting only the 'Street Name' and 'Violation Code' columns as User, Item parameters and count of the violations as Rating for IBCF Recommnder System
dfTrain = dfTrain.groupby('Street Name','Violation Code').count() 
dfTest = dfTest.groupby('Street Name','Violation Code').count()

#Removing the space in the column names by renaming them to 'StreetName' & 'ViolationCode'
dfTrain = dfTrain.withColumnRenamed('Street Name','StreetName').withColumnRenamed('Violation Code','ViolationCode')
dfTest = dfTest.withColumnRenamed('Street Name','StreetName').withColumnRenamed('Violation Code','ViolationCode')

#Indexing the 'Street Name' [String type] to 'StreetCode' [Neumeric type] for CF model
indexer = StringIndexer(inputCol='StreetName', outputCol="StreetCode")
indexModel = indexer.setHandleInvalid("skip").fit(dfTrain)

#Scaling/Normalizing the 'count' on a 1-5 scale for 'Rating' parameter for train and test data
#Training Data
dfTrainAssembler = VectorAssembler(inputCols=['count'],outputCol="countVector")
dfTrainAssembled = dfTrainAssembler.transform(dfTrain)
dfTrainScaler = MinMaxScaler(inputCol="countVector",outputCol="countScaled",min=1,max=5)
dfTrainScalarModel = dfTrainScaler.fit(dfTrainAssembled)
dfTrain = dfTrainScalarModel.transform(dfTrainAssembled) 

#Testing Data
dfTestAssembler = VectorAssembler(inputCols=['count'],outputCol="countVector")
dfTestAssembled = dfTestAssembler.transform(dfTest)
dfTestScaler = MinMaxScaler(inputCol="countVector",outputCol="countScaled",min=1,max=5)
dfTestScalarModel = dfTestScaler.fit(dfTestAssembled)
dfTest = dfTestScalarModel.transform(dfTestAssembled) 

#Converting the vector-scaled output back to a list to include in the final matrix required for building recommnder model
def extract(row):
	return (row.StreetName,row.ViolationCode, )+tuple(row.countScaled.toArray().tolist())

#Final Training and Testing matrices for model building
dfTrain = dfTrain.select("StreetName","ViolationCode","countScaled").rdd.map(extract).toDF(['StreetName','ViolationCode','count']) 
dfTest = dfTest.select("StreetName","ViolationCode","countScaled").rdd.map(extract).toDF(['StreetName','ViolationCode','count'])

#
indexModel = indexer.setHandleInvalid("skip").fit(dfTrain)
dfTrain = indexModel.transform(dfTrain)
dfTest = indexModel.transform(dfTest)

#Building the recommendation model using ALS on the training data
als = ALS(rank=200,maxIter=2,regParam=0.01,userCol="StreetCode",itemCol="ViolationCode",ratingCol="count",coldStartStrategy="drop",implicitPrefs=True)
recommModel = als.fit(dfTrain)

#Evaluate the model by computing the RMSE on the test data
predictions = recommModel.transform(dfTest)
evaluator = RegressionEvaluator(metricName = "rmse", labelCol="count", predictionCol="prediction")
RMSE = evaluator.evaluate(predictions)
print("RMSE of Model:",RMSE)	#Observed value: 0.44057324770617523

#Another way of making sure that the model is good, we can evaluate the model by computing the RMSE on train data
#If RMSE_Train ~> RMSE_Test => Underfitting. If RMSE_Train ~< RMSE_Test => Overfitting. If RMSE_Train ~ RMSE_Test => Good Model
predictionsTrain = recommModel.transform(dfTrain)
evaluatorTrain = RegressionEvaluator(metricName = "rmse", labelCol="count", predictionCol="prediction")
RMSETrain = evaluatorTrain.evaluate(predictionsTrain)
RMSETrain

#Let's now predict the possible Violation codes for 10 users using the model built
streetWithPossibleViolations = recommModel.recommendForAllUsers(10)

#From the analysis on the data, we can see that the below are top 10 streets that has the highest number of tickets for violating parking restrictions
top10StreetCode = dfTrain.filter(dfTrain['StreetName'].isin(['Broadway','1st Ave','2nd Ave','3rd Ave','5th Ave','7th Ave','8th Ave','Madison Ave','Lexington Ave','Queens Blvd']))

#Map the possible violations from above to the associated 'StreetCode'
top10StreetsMap = top10StreetCode.groupBy('StreetCode','StreetName').count()
top10StreetsMap = top10StreetsMap.drop("count")

#Function to retrieve the violations when a 'StreetName' is given
def violationsForStreet(streetName):
	reqStreetCode = top10StreetsMap.filter(top10StreetsMap['StreetName'] == streetName)
	reqStreetCode = reqStreetCode.select('StreetCode').rdd.flatMap(lambda x: x).collect()
	possibleViolationListColumn = streetWithPossibleViolations.filter(streetWithPossibleViolations['StreetCode'].isin(reqStreetCode))
	possibleViolationList = list(possibleViolationListColumn.select('recommendations').toPandas()['recommendations'])
	possibleViolationArray = [int(row.ViolationCode) for row in possibleViolationList[0]]
	output = []
	for violation in possibleViolationArray:
		output.append(violationCodeDictionary[violation])
	return output
		
#Now, let's find the possible violation codes that one can be ticketed for when parked on the top 10 streets
streetName = "Broadway"
print("Possible Violations for "+streetName+" are: "+violationsForStreet(streetName))
#########################################################################################
#Sample output:																			#
#('Possible Violations for ', 'Broadway', ' are: ', ['EXPIRED METER-COMM #METER ZONE',	#
#'BEYOND MARKED SPACE', 'PCKP DSCHRGE IN PRHBTD ZONE','OBSTRUCTING						#
#TRAFFIC/INTERSECT', 'ANGLE PARKING-COMM VEHICLE', 'FAIL TO #DISP. MUNI METER RECPT',	#
#'NO STOPPING-DAY/TIME LIMITS', 'SELLING/OFFERIN # #MCHNDSE-METER', 'IDLING',			#
#'NO PARKING-EXC. AUTH. VEHICLE'])                                                      #											#																			            #
#########################################################################################



