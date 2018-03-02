'''
CONFIRMATION TASKS

Description:
The confirmation.py file is used to double check that the Apache Spark results are correct by avoiding using Apache Spark.

Be aware, this is not part of the project solution, only used by the students to double-check that the results from Apache Spark are correct.
'''


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


if __name__ == "__main__":
    confirm_task1()