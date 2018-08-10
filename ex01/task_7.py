'''
## Task 7
First we map all tweets to an rdd with ('place_name', ('country_code', 'place_type', 'tweet_text')) to get only what we need.
We then filter the rdd to a new rdd with only tweets from the US and only where place_type == 'city'.
We then map to a new rdd(City, 1), so that we can count how many tweets there are in one city.
To do this we use reduceByKey on the rdd so that we add the values for each row, where the keys(cities) are equal.
We then use takeOrdered(5, sorted by tweet_count descending) to get a list of the 5 cities with the highest number of tweets.
Since we only have a list we parallelize the list so we get an rdd to work with again.
We then map the rdd with the tweets from US cities, and left outer join this rdd with the rdd that contains the top 5 cities to only get another rdd with only relevant information.
We then flatmap this new rdd to an rdd('word', ('place_name', 'tweet_count')), to get the words we convert the tweet_text to lowercase, split it at whitespace and exclude words shorter than 2 characters.
We then load the stopwords into an rdd('stopword', None) (we add the None to be able to use subtractByKey).
Then we use subtractByKey on the rdd('word', ('place_name', 'tweet_count')) with the stopwords rdd as parameter to get an rdd without the stopwords.

With each word as a key, and (city, city-tweet-count) as value, we map it back to something useful.
This lets us count every word, per city, as the key is a composite of the two.
We wish to use ReduceByKey on cities, in order to get the top N-words. Therefore, the word is 
mapped out of the key, to the value. 
Since the word-count is sorted, we can use a nifty custom add function to get the top-N results.
The add function is run over each city, by starting at the top row, adding (word, word-count) as a list
to the returned value. Once a keys value is a list of more than N results, we stop adding more words. 
Finally, we sort the result on the city-tweet count that we've kept all along and store the result. 
'''

from pyspark import SparkContext
from pyspark import SparkConf

from util import load, sc, load_stopwords
tweets = load()

# Top M cities
M = 5

# Top N words
N = 10


# List all tweets by City(idx-4), Country(idx-2), place_type(idx-3) and tweet-content(idx-10)
country_tweets = tweets.map(lambda row: (row[4], (row[2], row[3], row[10])))
# ('Buenos Aires, Argentina', 'AR', 'lLalalla')

# Filter the country on US
us_tweets = country_tweets.filter(lambda row: (row[1][0] == 'US' and row[1][1] == 'city'))
# ('Garland, TX', ('US', 'Today i used Spark') )


# Map tweets over to (City, 1) form
us_tweets_int = us_tweets.map(lambda row: (row[0], 1))
#('Garland, TX', 1)

# Aggregate the new column
city_tweet_count = us_tweets_int.reduceByKey(lambda a,b: (a+b))
# ('Manhattan, NY', 495)

# Taking the top 5
top_cities = city_tweet_count.takeOrdered(M, key=lambda row: -row[1])
# [('Manhattan, NY', 504), ..., ('Houston, TX', 240)]

# This datatype is a list, not RDD, so we convert it back
top_cities = sc.parallelize(top_cities)
# ('Manhattan, NY', 495)

# ('Garland, TX', ('US', 'Today i used Spark'))
tweets_city = us_tweets.map(lambda row: (row[0], row[1][2]))
# ('Garland, TX', 'Today i used Spark')

# Left outer join cities with tweets, so we only have the relevant tweets left
top_tweets = top_cities.leftOuterJoin(tweets_city)
# (city, (count, text))

# It maps each lowercase word with (city(idx0), city-count(idx1-0)) as key, and 1 as value
# Iff. the length of the word is > 2
# We keep the city-tweet-count in order to sort the results later on
words = top_tweets.flatMap(lambda row: ( ((w.lower()), (row[0],row[1][0])) for w in row[1][1].split(' ') if len(w) > 2) )
# ('#repost', ('Houston, TX', 21499))

# Load stopwords
stopwords = load_stopwords()
# ('a', None)

# Subtract the sets
words_filtered = words.subtractByKey(stopwords)
# ('clerk', ('Houston, TX', 213))

# Map to ("city-word", "city-count", "word") as key, 1 as value
words_intermediate = words_filtered.map(lambda row: ((row[1][0], row[1][1], row[0]),1))
# (('Houston, TX', 21499, 'even'), 1)

# Aggregate over the "city-word"-keys
words_counted = words_intermediate.reduceByKey(lambda a,b: a+b)
# (('Manhattan, NY', 495, 'new'), 158)

# Sort words by frequency
words_sorted = words_counted.sortBy(lambda row: -row[1])
# (('Manhattan, NY', 536, 'new'), 174)

# Convert to: ((city, citycount) , [(word, word_frequency)]
city = words_sorted.map(lambda row: ((row[0][0:2], [(row[0][2]), row[1]])))
# (('Manhattanm NY', 536), [('new', 174)])

# By looking at the data structure above: we want to create a new value, W, a list, by appending V1 to V2.
# This is repeated until W is of length N
# This is achieved by using the add function below
# It assumes the data is already sorted, otherwise we are only concatenating random elements
add = lambda a,b: a if len(a) > N else a+b

top_city_words = city.reduceByKey(add)
# (('Chicago, IL', 22593), ['the', 6460, 'chicago,', 3721, 'for', 2975, 'chicago', 2940, 'and', 2540, '#chicago,', 2411])

# Sort by city-tweet-count, which we have kept all along
top_city_words = top_city_words.sortBy(lambda row: -row[0][1])

def row_formatter(word):
	# Concatenate list of word and count to: "\t w1 \t c1 \w2 \c2"
	word_counts = ''.join([f'{x}\t' for x in word[1]])
	return f'{word[0][0]}\t{word_counts}'

result = top_city_words.map(row_formatter)

result.coalesce(1, True).saveAsTextFile('./results/result_7.tsv')

