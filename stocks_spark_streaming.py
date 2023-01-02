from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,  StructField, StringType, DoubleType
from pyspark.sql.functions import *


import sys

spark = SparkSession.builder.appName('retaildata').getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema=StructType([
    StructField('symbol', StringType()),
    StructField('timestamp', StringType()),
    StructField('priceData', StringType())])

priceschema= StructType ([
         StructField('close', DoubleType()),
         StructField('high', DoubleType()),
         StructField('low', DoubleType()),
         StructField('open', DoubleType()),
         StructField('volume', DoubleType())])
   
host = sys.argv[1]
port = sys.argv[2]
topic = sys.argv[3]

dfraw = spark.\
    readStream.\
    format("kafka").\
    option("kafka.bootstrap.servers",host + ":"+ port).\
    option("subscribe",topic).\
    option("includeHeaders", "true").\
    load()

get_value=dfraw.select('value')
get_val= get_value.withColumn('values', col('value').cast('string'))
string_val=get_val.withColumn('temp',from_json(col('values'),schema)).select("temp.*")
string_val= string_val.withColumn('prices', from_json(col('priceData'),priceschema)).select(["symbol", "timestamp","prices.*"])
string_val= string_val.withColumn('time', to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss'))

#Calculate the simple moving average on the closing price of all the four stocks 
# for the last 10 minutes and update the results every 5 minutes.  
# Closing prices are used mostly by the traders and investors as it reflects 
# the price at which the market finally settles down. 
# The SMA (Simple Moving Average) is a parameter used to find the average 
# stock price over a certain period based on a set of parameters. 
# The simple moving average is calculated by adding a stock's prices over a 
# certain period and dividing the sum by the total number of periods. 
# The simple moving average can be used to identify buying and selling opportunities


#2. Calculate the profit(average closing price - average opening price) 
#for each stock for the last 10 minutes and update the results every 5 minutes.


#3. Calculate the trading volume(total traded volume) of the four stocks in a for the last 10 minutes 
# and update the results every 5 minutes.  
# Decide which stock to purchase out of the four stocks. 
# Remember to take the absolute value of the volume. 
# Volume plays a very important role in technical analysis as it helps us to confirm trends and patterns. 
# You can think of volumes as a means to gain insights into how other participants perceive the market. 
# Volumes are an indicator of how many stocks are bought and sold over a given period of time. 
# Higher the volume, the more likely the stock will be bought. 

profit=string_val.\
groupby('symbol', window("time", "10 minutes", '5 minutes')).\
    agg(round((avg('close')-avg('open')),2).alias('profit'),\
    round(avg('close'),2).alias('SMA_closure'),\
    round(sum(abs('volume')),2).alias('volume')   
    )


query = string_val.writeStream.\
    format("console").\
    trigger(processingTime="10 minutes").\
    outputMode('append').\
    option('truncate', False).\
        start()


query1 = profit.writeStream.\
    format("console").\
    trigger(processingTime="10 minutes").\
    outputMode('Complete').\
    option('truncate', False).\
        start()



print("data: ")
query.awaitTermination()
print("SMA: ")
query1.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 stockspy.py 52.55.237.11 9092 stockData
