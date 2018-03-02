'''
TASK 3: Geographical Centroids per Country

Description:
 - Find the geographical centroid (latitude-longitude) for each country having more than 10 tweets (>10).
 - Visualize the results in CartoDB (https://carto.com/)
 - Hint: Centroid is the arithmetic mean of latitude and longitude of tweets. Example:
   - Tweet 1 from US with latitude:  33.884 and longitude: -118.092
   - Tweet 2 from US with latitude:  37.782 and longitude: -122.405
   - Tweet 3 from US with latitude:  40.791 and longitude: - 74.019
   - Calculate the average latitude: 37.485 and longitude: -104.838
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


# Apache Spark functionality
# TODO


def task1_3(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 3')
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
        task1_3(input_file, output_file)
    except IndexError:
        task1_3('/data/geotweets.tsv', 'data/results/task_3.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
