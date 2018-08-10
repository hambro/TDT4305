'''
# TDT4305 - Project part 2
# Jørgen Thorsnes, Carl Johan Hambro
## Setup
Create a virtualenvironment with python >= 3.6 . Then run:
```
pip install pyspark
```

usage: 
 `python part2.py [-h] -t training-file -i input-file -o output-file [-s]`

Use naive bayes to predict tweet location

optional arguments:

- -h, --help : show this help message and exit
- -t training-file, --training : training-file : Path to training data file
- -i input-file, --input input-file : Path to input data file
- -o output-file, --output output-file : Path to output data file
- -s, --sample : Enable sampling of training input

Example:

`python part2.py -i input_tweet -t ../geotweets.tsv -o my_output.dat`

The code also comes with a simple integration test that passes the examples given in the handout. This can be run with `python test.py`.

### About
The over all idea is to use the whole data set to train the model and then test some input against this.
We chose to filter the training data based on the input data first, which greatly reduced runtime from 2-3 mins to < 30 secs. 
However, this is unsutable if Spark is queried many times for different tweets as the model is cherry picked to fit a single
input. 

### Description
Count total tweets and tweets per city and store this for later use. 

The training tweets are loaded and all columns except location (city) and tweet-content are discarded.
The tweet content is then unified to lowercase and split into a list of words with a single space
as divider, but each word from a tweet is only stored if it exists in the set of words from the **input tweet**. 


Each tweet is flat mapped to (city - word) tuples by using flatMap on each tweet, along with the
set (!) of words in each tweet. A set is used since 1 tweet may only vote for any word once, not multiple times.
All (city, word) tuples are counted with a reduceByKey on the whole tuple. This gives the basis for
calculating Tc,w_i .
Some shuffeling is done so the result can be "reverse flatmapped". This latter operation uses combineByKey, which we pass a simple
"list add function" (list.__add__). The result are city as keys, and tuples of (word, frequency) as values. We convert this list
of tuples to a dictionary.
City-tweet-count is joined in to calculate Tc,w_i / Tc .

location_prob_intermediate calculates the sum product of all Tc,w_i / Tc by looking up each word in the input tweet
in the dictionary for a given city. 0 is returned as the default value if a word does not exist in the city-word dict.
The probabilities are then calculated over all the words in the input tweet.

The location_prob is found by multiplying in (Tc / |T|) with the previous value. This is done in paralell for all the cities.
Finally, the top two cities are picked out and stored to disk.
'''
import sys, argparse

def multiply_list(l,k):
    total = 1
    for li in l:
        total *= (li / k) 
    return total

def get_or_zero(word_frequency, word):
    try:
        return word_frequency[word]
    except KeyError:
        return 0


def main(training, input_file, output, sample):
    # Import libraries here to avoid cost when parser failes prematurely

    from pyspark import SparkContext
    from pyspark import SparkConf
    from util import load_tweets, sc, load_stopwords


    # Find total tweet count
    tweets = load_tweets(training, sample=sample)
    tweet_count = tweets.count()


    # Read input text
    raw_input_text = sc.textFile(input_file)
    input_text = raw_input_text.map(lambda row: row.lower().split(' '))
    input_text = input_text.collect()[0]
    input_text_set = set(input_text)

    # Load tweets
    tweets = tweets.map(lambda row: (row[4], (row[10].lower().split(' ')) ))


    # Calculate tweets per location
    location_count_int = tweets.map(lambda row: (row[0], 1))
    # ('Framingham, MA', 1)

    location_count = location_count_int.reduceByKey(lambda a,b: (a+b))
    # ('Manhattan, NY', 686809)

   
    # This is a cheeky line which filters all irrelevant words from the tweets which
    # are not present in the input text.
    tweets = tweets.map(lambda row: (row[0], set(row[1]).intersection(input_text_set)))
   
    # Flatmap tweet words to (location, word) tuples
    # Use a set so we don't double count word occurances in a single tweet
    location_word = tweets.flatMap(lambda row: ( ((row[0]), word) for word in row[1]) )
    # ('Framingham, MA', 'can')


    # Count occurances of each word per city
    location_word_int = location_word.map(lambda row: (row, 1))
    # (('East Hartford, CT', '#easthartford'), 1)
    location_word_count_int = location_word_int.reduceByKey(lambda a,b: a+b)
    # (('East Hartford, CT', '#easthartford'), 21)
    location_word_count = location_word_count_int.map(lambda row: ((row[0][0]), [(row[0][1], row[1])]))
    # ('East Hartford, CT', ('#easthartford', 21) )

    # Reverse of flatmap so we can have lists of tuples (word, frequency) for each city
    location_words = location_word_count.combineByKey(list, list.__add__, list.__add__)
    # ('El Oro, Mxico', [('oro', 1), ... ('norte', 4),   ('tiro', 2), ('#puebleando', 1)])

    # Convert list of tuples to a dictionary with words as keys, and frequencies as values
    location_word_freqs = location_words.map(lambda row: (row[0], {word:frequency for word,frequency in row[1]}))
    #('El Oro, Mxico', {'oro': 1, '(': 1, '@': 3, ...  '#puebleando': 1}))
    # Join in Tc (Tweet count per city)
    location_word_freq_tc = location_word_freqs.join(location_count)
    # ('El Oro, Mxico', (frequency_dict, Tc))

    # This gives all the weights of Tcw1/Tc * Tcw2/Tc * ... * Tcwn/Tc
    location_prob_intermediate = location_word_freq_tc.map(lambda row: 
        ((row[0]),
        (multiply_list([get_or_zero(row[1][0],word) for word in input_text], row[1][1])),
        row[1][1])
    )

    # Calculate final probability: (Tc / T) * PI-product[Tc,w_i)/Tc]
    location_prob = location_prob_intermediate.map(lambda row: (row[0], (row[2]/tweet_count) * row[1]))

    # Get top results
    top_results = location_prob.takeOrdered(2, key=lambda row: -row[1])
    
    # Print results that fit the requirements
    if not top_results:
        # There are no matching cities
        top_results = [['', '']]
    elif top_results[0][1] != top_results[1][1]:
        # The top result is not tied with the second place
        # Keep top result
        top_results = [top_results[0]]
    if top_results[0][1] == 0.0:
        # Top result is zero, replace with empty string
        top_results = [['', '']]

    with open(output, 'w+') as out:
        # Print result to file
        for line in top_results:
            out.writelines(f'{line[0]}\t{line[1]}\t')


if __name__ == '__main__':
    # Parser for the input parameters
    parser = argparse.ArgumentParser(description='Use naive bayes to predict tweet location')
    parser.add_argument('-t','--training', metavar='training-file', nargs=1,required=True,
                        help='Path to training data file')
    parser.add_argument('-i','--input', metavar='input-file', nargs=1,required=True,
                        help='Path to input data file')
    parser.add_argument('-o','--output', metavar='output-file', nargs=1,required=True,
                        help='Path to output data file')
    parser.add_argument('-s','--sample', metavar='sample',action='store_const', const=True, default=False,
                        help='Enable sampling of training input')   


    options = parser.parse_args()
    main(options.training[0],
        options.input[0],
        options.output[0],
        options.sample)