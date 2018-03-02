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


# Count the total number of tweets
def totalNumber(data):
    return data.count()


# Count number of distinct values
def countDistinct(data, row):
    return data.map(lambda x: x[row]).distinct().count()


# Minimum value
def minValue(data, row):
    return data.min(key=lambda x : float(x[row]))[row]


# Maximum value
def maxValue(data, row):
    return data.max(key=lambda x : float(x[row]))[row]


# Avg. characters
def avgCharacters(data, row):
    return data.map(lambda x : len(x[row])).mean()


# Avg. words
def avgWords(data, row):
    return data.map(lambda x : len(x[row].split(' '))).mean()


# Task 1 main
def task1_1(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 1')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    total_tweets    = totalNumber(data)
    usernames       = countDistinct(data, USERNAME)
    country_names   = countDistinct(data, COUNTRY_NAME)
    place_names     = countDistinct(data, PLACE_NAME)
    languages       = countDistinct(data, LANGUAGE)
    minLat          = minValue(data, LATITUDE)
    minLng          = minValue(data, LONGITUDE)
    maxLat          = maxValue(data, LATITUDE)
    maxLng          = maxValue(data, LONGITUDE)
    avgChars        = avgCharacters(data, TWEET_TEXT)
    avgWrds         = avgWords(data, TWEET_TEXT)

    # Formatting and printing results
    prettyTable = PrettyTable(['Description', 'Value'])
    prettyTable.add_row(['Total number of tweets', total_tweets])
    prettyTable.add_row(['Number of distinct usernames', usernames])
    prettyTable.add_row(['Number of distinct country names', country_names])
    prettyTable.add_row(['Number of distinct place names', place_names])
    prettyTable.add_row(['Number of distinct languages', languages])
    prettyTable.add_row(['Minimum latitude', minLat])
    prettyTable.add_row(['Minimum longitude', minLng])
    prettyTable.add_row(['Maximum latitude', maxLat])
    prettyTable.add_row(['Maximum longitude', maxLng])
    prettyTable.add_row(['Average number of characters in each tweet', avgChars])
    prettyTable.add_row(['Average number of words in each tweet', avgWrds])
    print(prettyTable)

    # Store results in file
    with open(output_file, 'w') as f:
        f.write(prettyTable.get_string())



if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_1(input_file, output_file)
    except IndexError:
        task1_1('/data/geotweets.tsv', 'PhaseOne/data/results/task_1.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
