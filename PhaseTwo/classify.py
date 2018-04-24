'''
 ' Classify.py
'''

import argparse

from pyspark import SparkConf, SparkContext


# Used headings
PLACE_NAME = 4
TWEET_TEXT = 10


# Generate a mapped training set sample (place and tweet)
def generate_training_set(data):
    training_sample = data.sample(False, 0.1, 5) # According to specifications in presentation
    return training_sample.map(lambda pt : (pt[PLACE_NAME], pt[TWEET_TEXT].lower().split(' ')))


# Read input tweet text(s) and return as list of lists
def read_input_tweet(input_file):
    tweets = []
    with open(input_file) as f:
        for line in f:
            tweet = line.lower().strip().split(' ')
            tweets.append(tweet)
    return tweets


# Return total number of tweets
def get_total_number_of_tweets(training_set):
    return training_set.count()


# Count number of tweets with specific word
# occurrences | Counts occurrence of each word in input tweet
# tweet       | Tweet text
# input_tweet | Text from input file
def occurrence_counter(occurrences, tweet, input_tweet):
    for i in range(len(input_tweet)):
        if input_tweet[i] in tweet:
            occurrences[i] += 1
    return occurrences


# Handling of probability functionality
def handle_probability(training_set, total_number_of_tweets, input_tweet):
    word_counter_list = [0] * len(input_tweet)
    return training_set.aggregateByKey( (word_counter_list, 0),
                                        lambda x, tweet : (occurrence_counter(x[0], tweet, input_tweet), x[1] + 1),
                                        lambda rdd1, rdd2: ([rdd1[0][i] + rdd2[0][i] for i in range(len(rdd1[0]))], rdd1[1] + rdd2[1])) \
                                        .filter(lambda x: all(i > 0 for i in x[1][0])) \
                                        .map(lambda x : (x[0], get_location_probability(x[1], total_number_of_tweets)))


# Get probability for each location
def get_location_probability(incidents, total_number_of_tweets):
    probability = (float(incidents[1]) / float(total_number_of_tweets))
    for word_count in incidents[0]:
        probability *= (float(word_count) / float(incidents[1]))
    return probability


# Find the location(s) with the highest probable similarity
def find_highest_probability_location(probability_data):
    if probability_data.count() <= 0:
        return None
    
    highest_probability = probability_data.max(key=lambda x : x[1])
    return probability_data.filter(lambda x : x[1] == highest_probability[1]).collect()


# Store probability results to file
def store_probabilities(probability_data, output_file):
    with open(output_file, 'a') as of:
        if probability_data == None:
            of.write('\n')
        else:
            for location in probability_data:
                of.write(location[0] + '\t')
            of.write(str(location[1]) + '\n')


def classifier(training_file, input_file, output_file):
    conf = SparkConf().setMaster('local[*]').setAppName('TDT4305: Big Data Architecture - Project Phase 2')
    sc = SparkContext(conf = conf)
    raw_data = sc.textFile(training_file, use_unicode=False)    # Set "use_unicode=True" if there is a TypeError
    data = raw_data.map(lambda x: x.split('\n')[0].split('\t'))

    # Training set file
    training_set = generate_training_set(data)

    # Input file
    input_data = read_input_tweet(input_file)

    # Get total number of tweets
    total_number_of_tweets = get_total_number_of_tweets(training_set)

    for input_tweet in input_data:
        # Handle and calculate probabilities
        probability_data = handle_probability(training_set, total_number_of_tweets, input_tweet)

        # Find location(s) with highest probable similarity
        highest_probability = find_highest_probability_location(probability_data)
        
        # Store results to file
        store_probabilities(highest_probability, output_file)


if __name__ == '__main__':
    # Argparse documentation: https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(description='Add paths for training, input, and output files.')
    parser.add_argument('-training', metavar='-t', type=str, help='Full path of the training file.')
    parser.add_argument('-input',    metavar='-i', type=str, help='Full path of the input file.')
    parser.add_argument('-output',   metavar='-o', type=str, help='Full path of the output file.')
    args = parser.parse_args()

    if args.training != None and args.input != None and args.output != None:
        classifier(args.training, args.input, args.output)
    else:
        print('Missing parameters, make sure to add necessary parameters when executing.')
