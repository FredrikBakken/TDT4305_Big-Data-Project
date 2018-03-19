'''
TASK 7: Frequent Words per City

Description:
 - Find 5 cities in US with the highest number of tweets, and for each of these cities, find 10 most frequent
   words and their frequencies in tweet_texts.
 - Word-frequency pairs must be sorted in descending order of frequencies (for words with equal frequencies,
   use alphabetical ordering of words).
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

# Extracting the stop words from the stop_words.txt file
def get_stop_words():
    with open('/data/stop_words.txt') as f:
        contents = f.readlines()
    
    contents = [x.strip() for x in contents]
    return contents



def task1_7(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 7')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Empty result list
    result = []

    # Get stop words
    stop_words = get_stop_words()

    # Top 5 cities
    top_5_cities = data.filter(lambda x : x[COUNTRY_CODE] == 'US' and x[PLACE_TYPE] == 'city') \
                       .map(lambda x : (x[PLACE_NAME], 1)) \
                       .reduceByKey(lambda a, b: a + b) \
                       .sortBy(lambda (word, count): count, False) \
                       .take(5)

    # Top 10 words
    for city_count in top_5_cities:
        city = city_count[0]

        top_10_words = data.filter(lambda x : x[COUNTRY_CODE] == 'US' and x[PLACE_NAME] == city) \
               .flatMap(lambda x : x[TWEET_TEXT].lower().split(' ')) \
               .filter(lambda word : len(word) > 2 and word not in stop_words) \
               .map(lambda word : (word, 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda (word, count): count, False) \
               .take(10)
        
        city_words = city

        for word_count in top_10_words:
            word = word_count[0]
            frequency = word_count[1]
            city_words += '\t%s\t%s' % (word, frequency)
        
        result.append(city_words)
    
    # Store results in file
    with open(output_file, 'w') as f:
        for x in range(len(result)):
            f.write(result[x] + '\n')


if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_7(input_file, output_file)
    except IndexError:
        task1_7('/data/geotweets.tsv', 'data/results/result_7.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
