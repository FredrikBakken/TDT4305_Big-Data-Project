
def run():
    tmp = 0.0
    with open('/data/geotweets.tsv') as f:
        for line in f:
            line = line.rstrip().split('\t')
            #print(line[12] + '   ' + str(type(line[12])))

            if float(line[12]) < float(tmp):
                tmp = line[12]
            #print(l)
        #first_line = f.readline()
        #second_line = f.readline()

    #print(type(first_line))
    #print(second_line)

    #l = first_line.split('\t')
    #print(l)
    #print(type(l))

    print(tmp)

    return True



if __name__ == "__main__":
    run()