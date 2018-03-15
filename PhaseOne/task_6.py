'''
TASK 6: Frequent Words in a Country

Description:
 - Find 10 most frequent words and their frequencies in tweet_texts posted from the US (country code = 'US').
 - Word-frequency pairs must be sorted in descending order of frequencies (for words with equal frequencies,
   use alphabetical ordering of words).
 - Example use case? See: https://www.trendsmap.com/
 - Use space characters in tweet texts as separator for words
 - Ignore words with length 1 and the words in stop words list
   - Your code must also load the list of stop words from stop_words.txt file
 - Case-insensitive: Remember to convert all texts to lowercase first
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

# Processing by getting top 10 used words in US
# Examples used: https://spark.apache.org/examples.html
def top_10_words_US(data, stop_words):
    return data.filter(lambda x : x[COUNTRY_CODE] == 'US') \
               .flatMap(lambda x : x[TWEET_TEXT].lower().split(' ')) \
               .filter(lambda word : len(word) > 1 and word not in stop_words) \
               .map(lambda word : (word, 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda (word, count): count, False) \
               .take(10)


# Task 6 main
def task1_6(input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 1, Task 6')
    sc = SparkContext(conf = conf)

    rawData = sc.textFile(input_file, use_unicode=False)
    data = rawData.map(lambda x: x.split('\n')[0].split('\t'))\

    # Get stop words
    stop_words = get_stop_words()
    
    # Processing data
    top_10_words = top_10_words_US(data, stop_words)

    # Store results in file
    with open(output_file, 'w') as f:
        for x in range(len(top_10_words)):
            f.write(top_10_words[x][0] + '\t%d\n' % (top_10_words[x][1]))


if __name__ == "__main__":
    arguments = sys.argv

    try:
        input_file = arguments[1]
        output_file = arguments[2]
        task1_6(input_file, output_file)
    except IndexError:
        task1_6('/data/geotweets.tsv', 'data/results/result_6.tsv')
    except:
        print('Something went wrong during the initialization. Please see the command execution examples on Github (www.github.com/FredrikBakken/TDT4305_Big-Data-Project/tree/master/PhaseOne).')
