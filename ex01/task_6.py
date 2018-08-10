'''
## Task 6
First filter all tweets into rdd whith only tweets with country_code == 'US' to get rdd with only the tweets we need.
Then we use flatmap on the rdd to convert list of values to individual keys and add value 1 to each word. To get words as key, we first convert tweet_text to lowercase and split the tweet_text into the words, and exclude words shorter than 2 characters.
We then use reduceByKey to count the frequency of the words, by adding the values of the rows where the keys (words) are equal.
We load in the stopwords into a new rdd, and map the stopword rdd to (stopword, Add None), because subtractByKey needs the data in the two sets to be of equal format.
We then use subtractByKey on the rdd with the counted words, and use the stopwords rdd as parameter, to get an rdd with the counted words without the stopwords.
Lastly, we sort by frequency by using sortBy and sort by the word count, decreasing, because that looks nice.
'''

from pyspark import SparkContext
from pyspark import SparkConf

from util import load, load_stopwords
tweets = load()



# Filter on country code equal United States
us_tweets = tweets.filter(lambda row: row[2] == 'US')

# Use flatmap to convert list of values to individual keys
# Add column with 1's.
flattened = us_tweets.flatMap(lambda row: ((w.lower(), 1) for w in row[10].split(' ') if len(w) > 2) )

# Count each word by reducing over the unique keys (words), summing the 1-column
counted_words = flattened.reduceByKey(lambda a,b: a+b)

stopwords = load_stopwords()
# ('a', None)

# Subtract the sets
counted_words = counted_words.subtractByKey(stopwords)
# ('aburridooooo', 1)

sorted_by_frequency = counted_words.sortBy(lambda row: -row[1])
# ('#hiring', 294749)

# Store result
sorted_by_frequency.map(lambda row: f'{row[0]}\t{row[1]}').coalesce(1, True).saveAsTextFile('./results/result_6.tsv')
