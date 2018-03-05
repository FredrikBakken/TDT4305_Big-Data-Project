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


# Count tweets for each country
def countTweetsCountry(data, row):
    return data.map(lambda x : (x[row], 1)).reduceByKey(lambda x,y:x+y).collect()


# Average (mean)
def averageLatLng(data, country_name, row1, row2):
    return data.filter(lambda x : x[row1] == country_name).map(lambda x : float(x[row2])).mean()


# Task 3 main
def task1_3(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 3')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Processing data
    tweetsCountry = countTweetsCountry(data, COUNTRY_NAME)

    result_list = []
    for country in tweetsCountry:
        country_name    = country[0]
        tweets          = country[1]

        if tweets > 10:
            avgLat = averageLatLng(data, country_name, COUNTRY_NAME, LATITUDE)
            avgLng = averageLatLng(data, country_name, COUNTRY_NAME, LONGITUDE)

            formatted_result = [str(country_name), str(avgLat), str(avgLng)]
            result_list.append(formatted_result)

    # Store results in file
    with open(output_file, 'w') as f:
        for x in range(len(result_list)):
            f.write(result_list[x][0] + '\t' + result_list[x][1] + '\t' + result_list[x][2] + '\n')

    # Printing results to console (not part of the requirements)
    prettyTable = PrettyTable(['Country', 'Average latitude', 'Average longitude'])
    for x in range(len(result_list)):
        prettyTable.add_row([result_list[x][0], result_list[x][1], result_list[x][2]])
    print(prettyTable)
    


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
