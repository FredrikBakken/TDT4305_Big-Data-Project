'''
CONFIRMATION TASKS

Description:
The confirmation.py file is used to double check that the Apache Spark results are correct by avoiding using Apache Spark.

Be aware, this is not part of the project solution, only used by the students to double-check that the results from Apache Spark are correct.
'''

import sys


def confirm_task1():
    # Total number of tweets
    with open('/data/geotweets.tsv') as f:
        line_count = 0
        for line in f:
            line_count += 1
        
        print('Total number of tweets: ' + str(line_count))
    

    # Minimum latitude and longitude
    with open('/data/geotweets.tsv') as f:
        lat_tmp = 0.0
        lng_tmp = 0.0
        for line in f:
            line = line.rstrip().split('\t')

            if float(line[11]) < float(lat_tmp):
                lat_tmp = line[11]

            if float(line[12]) < float(lng_tmp):
                lng_tmp = line[12]
        
        print('Minimum latitude: ' + str(lat_tmp))
        print('Minimum longitude: ' + str(lng_tmp))
    

    # Maximum latitude and longitude
    with open('/data/geotweets.tsv') as f:
        lat_tmp = 0.0
        lng_tmp = 0.0
        for line in f:
            line = line.rstrip().split('\t')

            if float(line[11]) > float(lat_tmp):
                lat_tmp = line[11]

            if float(line[12]) > float(lng_tmp):
                lng_tmp = line[12]
        
        print('Maximum latitude: ' + str(lat_tmp))
        print('Maximum longitude: ' + str(lng_tmp))

    
    # Average number of characters in tweets
    with open('/data/geotweets.tsv') as f:
        total_lenght = 0
        for line in f:
            line = line.rstrip().split('\t')

            total_lenght += len(line[10])
        
        avgChar = (total_lenght / line_count)
        print('Average number of characters: ' + str(avgChar))
    

    # Average number of words in tweets
    with open('/data/geotweets.tsv') as f:
        total_words = 0
        for line in f:
            line = line.rstrip().split('\t')
            words = line[10].split(' ')

            total_words += len(words)
        
        avgWords = (total_words / line_count)
        print('Average number of words: ' + str(avgWords))


def confirm_task2():
    with open('/data/geotweets.tsv') as f:
        results = []
        for line in f:
            line = line.rstrip().split('\t')

            country = line[1]

            found = False

            for x in range(len(results)):
                if country == results[x][0]:
                    results[x][1] += 1
                    found = True
                    break
            
            if not found:
                results.append([country, 1])
            
        sorted_results = sorted(results, key=lambda k: (-k[1], k[0]))
        
        for r in sorted_results:
            print(r)



if __name__ == "__main__":
    arguments = sys.argv[1:]

    try:
        if '1' in arguments:
            print('Starting confirmation program for task 1...')
            confirm_task1()
        
        if '2' in arguments:
            print('Starting confirmation program for task 2...')
            confirm_task2()
        
        if '3' in arguments:
            print('Starting confirmation program for task 3...')
        
        if '4' in arguments:
            print('Starting confirmation program for task 4...')
        
        if '5' in arguments:
            print('Starting confirmation program for task 5...')
        
        if '6' in arguments:
            print('Starting confirmation program for task 6...')

        if '7' in arguments:
            print('Starting confirmation program for task 7...')
        
        if '8' in arguments:
            print('Starting confirmation program for task 8...')

    except:
        print('Please specify which tasks to execute...')