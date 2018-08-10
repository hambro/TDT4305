'''
## Task 5
First we filter tweets to only tweets with countrycode 'US' and place_type = 'city', to only work with the tweets we need.
Then we map the rdd of tweets from the US to new rdd with place_name as key and add 1 as value for each row.
We then use reduceByKey and "add" together the values for each row where place_name == place_name, to get wanted result (place_name, tweet_count).
First we sort by place_name, and then by negative tweet_count to sort the rdd descending by tweet_count, and for equal number of tweets, sorted by place_name in alphabetical order.
'''

from pyspark import SparkContext
from pyspark import SparkConf

from util import load
tweets = load()

# Filter tweets from the US
us_tweets = tweets.filter(lambda row: row[2] == 'US' and row[3] == 'city')

# Add 1 to each row
intermediate = us_tweets.map(lambda row: (row[4], 1))

# reduceByKey runs the given function over the sets of values, for each key
tweets_per_city = intermediate.reduceByKey(lambda a,b: a+b)

# Sort alphabetical
alphabetical = tweets_per_city.sortBy(lambda row: row[0])

# Sort by count at last, ties are decided by the alphabetical ordering
sorted_by_count = alphabetical.sortBy(lambda row: -row[1])

# Store result
sorted_by_count.map(lambda row: f'{row[0]}\t{row[1]}').coalesce(1, True).saveAsTextFile('./results/result_5.tsv')
# <place_name>tab<tweet_count>