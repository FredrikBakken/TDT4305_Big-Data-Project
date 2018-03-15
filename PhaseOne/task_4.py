'''
TASK 4: Most Active Hours per Country
Description:
 - Find 1-hour time interval with max. number of tweets for each country
 - Use local time for tweets
 - Hint: You can use datetime library in Python. Example:
   - utc_time=1447085101697, timezone_offset= -7200
   - Local time in seconds: 1447085101697 / 1000 - 7200 = 1447077901
   - datetime.fromtimestamp(1447077901) = 2015-11-09 15:05:01
 - Calculate how many tweets are posted at each hour and find the most active hours for the countries
'''

import sys

from pyspark import SparkContext
from pyspark import SparkConf

from datetime import datetime


# 1-hour time interval with max. number of tweets for each country
def countMaxTweetsForEachCountry(data):
    return data.map(lambda x: (x[COUNTRY_NAME], int(x[UTC_TIME]) + int(x[TIMEZONE_OFFSET])))\
                .map(lambda x: ((x[0], datetime.fromtimestamp(x[1]/1000).hour), 1))\
                .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2)))\
                .map(lambda x: (x[0][0], (x[0][1], x[1])))\
                .reduceByKey(lambda x, y: y if y[1] > x[1] else x)\
                .map(lambda x: (x[0],x[1][0],x[1][1])).sortByKey(True,1)


# Header structure
UTC_TIME            = 0
COUNTRY_NAME        = 1
COUNTRY_CODE        = 2
PLACE_TYPE          = 3
PLACE_NAME          = 4
LANGUAGE            = 5
USERNAME            = 6
USER_SCREEN_NAME    = 7
TIMEZONE_OFFSET     = 8
NUMBER_OF_FRIENDS   = 9
TWEET_TEXT          = 10
LATITUDE            = 11
LONGITUDE           = 12

# Task 4 main
def task1_4(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 4')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    result_countMaxTweetsForEachCountry = countMaxTweetsForEachCountry(data)

    # Store results in file
    result_countMaxTweetsForEachCountry.coalesce(1,True).saveAsTextFile(output_file)


if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_4(input_file, output_file)
    except IndexError:
        task1_4('/data/geotweets.tsv', 'data/results/result_4.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')