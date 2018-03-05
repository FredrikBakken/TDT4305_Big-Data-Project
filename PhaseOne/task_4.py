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

from prettytable import PrettyTable
from pyspark import SparkConf, SparkContext


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


# Apache Spark
# TODO


# Task 4 main
def task1_4():
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 4')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    # TODO

    # Store results in file
    # TODO

    # Printing results to console (not part of the requirements)
    # TODO


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

