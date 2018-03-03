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

from operator import add
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

    ## Testing
    r1 = data.map(lambda x : (x[COUNTRY_NAME], 1)).reduceByKey(lambda x,y:x+y).collect()
    print(r1)

    # Possible to use RDD actions/transformation for this??
    country_list = []
    for country in r1:
        if country[1] > 10:

            avgLat = data.filter(lambda x : x[COUNTRY_NAME] == country[0]) \
                         .map(lambda x : float(x[LATITUDE])).mean()
            
            avgLng = data.filter(lambda x : x[COUNTRY_NAME] == country[0]) \
                         .map(lambda x : float(x[LONGITUDE])).mean()
            
            a = country[0] + '\t' + str(avgLat) + '\t' + str(avgLng)

            country_list.append(a)
    
    print(country_list)

    # Store results in file
    with open(output_file, 'w') as f:
        for x in range(len(country_list)):
            f.write(country_list[x] + '\n')
    
    #print(country_list)

    #r2 = data.filter(lambda x : x[COUNTRY_NAME] in country_list) \
    #         .map(lambda x : (x[COUNTRY_NAME], [x[LONGITUDE], x[LATITUDE]])).take(5)
    #print(r2)

    #r2 = r1.leftOuterJoin(data).collect()
    #print(r2)
    #r2 = data.filter(lambda x : x[COUNTRY_NAME] and x[COUNTRY_NAME] in r1) \
    #         .map(lambda x : x[COUNTRY_CODE]).countByValue().items()
    #print(r2)

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
        task1_3('/data/geotweets.tsv', 'data/results/result_3.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
