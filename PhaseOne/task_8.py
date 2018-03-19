'''
TASK 8: Explore using Spark SQL and Dataset API
Description:
 - Load geotweets.tsv into a Dataframe, name the dataframe and the columns using the column names described before.
 - Calculate the following using suitable Spark SQL commands:
   - Number of tweets
   - Number of distinct usernames/country_names/place_names/languages
   - Minimum latitude and longitude
   - Maximum latitude and longitude
 - Print the results using show() function and include screenshots in your report
'''

import sys

import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row
from pyspark.sql import SQLContext
from prettytable import PrettyTable


# Count the total number of tweets
def totalNumber(sqlContext):
    return sqlContext.sql("select count(*) from geoTweetsTempTable")

# Count number of distinct usernames
def countUsernames(sqlContext):
    return sqlContext.sql("select count(distinct USERNAME) from geoTweetsTempTable")

# Count number of distinct usernames
def countCountryNames(sqlContext):
    return sqlContext.sql("select count(distinct COUNTRY_NAME) from geoTweetsTempTable")

# Count number of distinct usernames
def countPlaceNames(sqlContext):
    return sqlContext.sql("select count(distinct PLACE_NAME) from geoTweetsTempTable")

# Count number of distinct usernames
def countLanguages(sqlContext):
    return sqlContext.sql("select count(distinct LANGUAGE) FROM geoTweetsTempTable")

# Minimum Latitude
def minLatitude(sqlContext):
    return sqlContext.sql("select min(LATITUDE) from geoTweetsTempTable")

# Minimum Longitude
def minLongitude(sqlContext):
    return sqlContext.sql("select min(LONGITUDE) from geoTweetsTempTable")

# Maximum Latitude
def maxLatitude(sqlContext):
    return sqlContext.sql("select max(LATITUDE) from geoTweetsTempTable")

# Maximum Longitude
def maxLongitude(sqlContext):
    return sqlContext.sql("select max(LONGITUDE) from geoTweetsTempTable")


# Task 8 main
def task1_8(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 8')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t')) \

    sqlContext = SQLContext(sc)

    geoTweetsRows = data.map(lambda p: Row(
        UTC_TIME=float(p[0]),
        COUNTRY_NAME=p[1],
        COUNTRY_CODE=p[2],
        PLACE_TYPE=p[3],
        PLACE_NAME=p[4],
        LANGUAGE=p[5],
        USERNAME=p[6],
        USER_SCREEN_NAME=p[7],
        TIMEZONE_OFFSET=float(p[8]),
        NUMBER_OF_FRIENDS=int(p[9]),
        TWEET_TEXT=p[10],
        LATITUDE=float(p[11]),
        LONGITUDE=float(p[12])
    ))

    tempTable = sqlContext.createDataFrame(geoTweetsRows)
    tempTable.registerTempTable("geoTweetsTempTable")

    # Processing data
    total_tweets = totalNumber(sqlContext)
    usernames = countUsernames(sqlContext)
    countryNames = countCountryNames(sqlContext)
    placeNames = countPlaceNames(sqlContext)
    languages = countLanguages(sqlContext)
    minLat = minLatitude(sqlContext)
    minLng = minLongitude(sqlContext)
    maxLat = maxLatitude(sqlContext)
    maxLng = maxLongitude(sqlContext)

    # Printing results to console
    prettyTable = PrettyTable(['Description', 'Value'])
    prettyTable.add_row(['Total number of tweets', total_tweets.first()])
    prettyTable.add_row(['Number of distinct usernames', usernames.first()])
    prettyTable.add_row(['Number of distinct country names', countryNames.first()])
    prettyTable.add_row(['Number of distinct place names', placeNames.first()])
    prettyTable.add_row(['Number of distinct languages', languages.first()])
    prettyTable.add_row(['Minimum latitude', minLat.first()])
    prettyTable.add_row(['Minimum longitude', minLng.first()])
    prettyTable.add_row(['Maximum latitude', maxLat.first()])
    prettyTable.add_row(['Maximum longitude', maxLng.first()])
    print(prettyTable)

if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_8(input_file, output_file)
    except IndexError:
        task1_8('C:\TDT4305_Big-Data-Project/PhaseOne/data/geotweets.tsv', 'C:\TDT4305_Big-Data-Project/PhaseOne/data/result_8.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
