'''
TASK 1: Load RDD and Explore

Description:
 - Total number of tweets
 - Number of distinct usernames, country_names, place_names, languages
 - Minimum latitude and minimum longitude
 - Maximum latitude and maximum longitude
 - Average number of characters in tweet texts
 - Average number of words in tweet texts
'''

import re
import sys
import string
import calendar

from pyspark import SparkConf, SparkContext
from datetime import datetime


# Data structure
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


# Count the total number of tweets
def totalNumber(tweets):
    return tweets.count()


# Count number of distinct values
def countDistinct(data, r):
    return data.map(lambda row: row[r]).distinct().count()


# Minimum value
# TODO

# Maximum value
# TODO


# Avg. characters
# TODO

# Avg. words
# TODO


def task1_1(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 1')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    total_tweets = totalNumber(data)
    usernames = countDistinct(data, USERNAME)
    country_names = countDistinct(data, COUNTRY_NAME)
    place_names = countDistinct(data, PLACE_NAME)
    languages = countDistinct(data, LANGUAGE)


    # Result print
    print('Total number of tweets: ' + str(total_tweets))
    print('Number of distinct usernames: ' + str(usernames))
    print('Number of distinct country names: ' + str(country_names))
    print('Number of distinct place names: ' + str(place_names))
    print('Number of distinct languages: ' + str(languages))


    # Testing
    #test = data.map(lambda )
    #test = data.map(lambda row : row[TWEET_TEXT]).getAll()
    #r = averageLen(test)
    #print(test)
    #print(r)



if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_1(input_file, output_file)
    except IndexError:
        task1_1('/data/geotweets.tsv', '/data/output_task1')
    except:
        print('Something went wrong during the initialization.')
