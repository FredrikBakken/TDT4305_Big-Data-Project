'''
 ' Classify.py
'''

import argparse

def classifier(training_file, input_file, output_file):
    print('Hello world')
    print(training_file)
    print(input_file)
    print(output_file)


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