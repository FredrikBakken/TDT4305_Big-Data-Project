'''
TASK 2: Tweet Counts per Country

Description:
 - Find the number of tweets posted from each country, sorted in descending order of tweet counts (for countries
   with equal number of tweets, use alphabetic ordering of country names)
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


# Descending sort by number of tweets and country name
def descendingSortByTweetsAndCountry(data, row):
    return sorted(data.map(lambda x : x[row]).countByValue().items(), key=lambda k: (-k[1], k[0]))


# Task 2 main
def task1_2(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 2')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    sorted_result = descendingSortByTweetsAndCountry(data, COUNTRY_NAME)

    # Store results in file
    with open(output_file, 'w') as f:
        for x in range(len(sorted_result)):
            f.write(str(sorted_result[x][0]) + '\t' + str(sorted_result[x][1]) + '\n')
    
    # Printing results to console (not part of the requirements)
    prettyTable = PrettyTable(['Country', 'Number of Tweets'])
    for x in range(len(sorted_result)):
        prettyTable.add_row([sorted_result[x][0], sorted_result[x][1]])
    print(prettyTable)



if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_2(input_file, output_file)
    except IndexError:
        task1_2('/data/geotweets.tsv', 'PhaseOne/data/results/result_2.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
