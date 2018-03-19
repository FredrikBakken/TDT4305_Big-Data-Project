'''
TASK 5: Tweet Counts per City

Description:
 - Find the number of tweets from each city in US (place type = 'city' and country code = 'US'),
   sorted in descending order of tweet counts (for city with equal number of tweets, use alphabetical
   ordering of city names).
'''


import sys

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


# Find the number of tweets from each city in US, sort in descending order of tweet counts, city name
def numberOfTweetsFromEachCityInUS(data):
    return data.filter(lambda x: x[COUNTRY_CODE] == 'US' and x[PLACE_TYPE] == "city") \
        .map(lambda x: (x[PLACE_NAME], 1)) \
        .aggregateByKey(0, (lambda x, y: x + y), (lambda rdd1, rdd2: (rdd1+rdd2))) \
        .sortByKey() \
        .sortBy(lambda x: x[1], False).coalesce(1)

# Task 5 main
def task1_5(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 5')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t')) \

    # Processing data
    result_countOfTweets_sorted = numberOfTweetsFromEachCityInUS(data)

    # Store results in file
    result_countOfTweets_sorted.saveAsTextFile(output_file)

if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_5(input_file, output_file)
    except IndexError:
        task1_5('data/geotweets.tsv', 'data/results/result_5.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
