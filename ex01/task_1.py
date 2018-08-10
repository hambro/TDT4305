'''
# TDT4305 - Project part 1
# Jørgen Thorsnes, Carl Johan Hambro
## Setup
Create a virtualenvironment with python >= 3.6 . Then run:
```
pip install pyspark
```
Simply run `python task_*.py` to run a task. 

To speed up debugging: use kwarg `sample=True` when calling `load()`: `load(sample=True)`.

## About
Usually, 1 coalesce is used in each task to store the final result. 

## Task 1
**a**: Use count() function on the RDD

**b, c, d, e**: Use distinct() to remove all duplicate users, before .count()

**f, g, h, i**: Find min and max of latitud. This is done by passing the function max(a,b) and min(a,b) to the 
reducer function in the RDD object. This runs max and min over the entire dataset, producing only 1 output. 

**j**: Run len() on the tweet content, on all tweets, by passing the function to map() on an RDD object. 
This is afterwards summed up by a reduce() call, and finally, divided by the total amount of tweets found in a.

**k**: Almost the same as j, but instead of len() which gives total characters, we simply count the amount of spaces, 
and add 1 (because there are, for instance, only two spaces in a three word sentence). This gives the total amount of 
words instead. 
'''

from pyspark import SparkContext
from pyspark import SparkConf

from util import load, sc

results = []

# Load tweets
tweets = load()

# 1.a) count number of tweets and print the result
tweet_count = tweets.count()
results.append((f'Number of tweets\t {tweet_count}'))

# 1.b) Count number of distinct usernames and print the result
usernames = tweets.map(lambda row: row[7])
distinct_usernames = usernames.distinct()
user_count = distinct_usernames.count()
results.append(f'Number of distinct usernames\t {user_count}')

# 1.c) Count number of distinct countries
#Map to dataset with only countries
countries = tweets.map(lambda row: row[1])
country_counts = countries.distinct().count()
results.append(f'Number of distinct countries\t {country_counts}')

# 1.d) Count number of distinct place names
# Map to dataset with only place names
locations = tweets.map(lambda row: row[4])
location_counts = locations.distinct().count()
results.append(f'Number of distinct place names\t {location_counts}')

# 1.e) Count number of languages used
languages = tweets.map(lambda row: row[5])
language_count = languages.distinct().count()
results.append(f'Number of distinct languages used\t {language_count}')

# 1.f and h) find min and max latitude
#map to list of latitudes
latitudes = tweets.map(lambda row: float(row[11]))

# map elements to float so we can use max and min function
latitudes_max = latitudes.reduce(max)
latitudes_min = latitudes.reduce(min)
results.append(f'Minimum latitude\t {latitudes_min:.3f}')
results.append(f'Maximum latitude\t {latitudes_max:.3f}')

# 1.g and i) find min and max longitude
longitudes = tweets.map(lambda row: float(row[12]))

longitude_max = longitudes.reduce(max)
longitude_min = longitudes.reduce(min)
results.append(f'Minimum longitude\t {longitude_min:.3f}')
results.append(f'Maximum longitude\t {longitude_max:.3f}')

# 1.j) What is the average length of a tweet text in terms of characters?
#map to tweet text
tweet_lengths = tweets.map(lambda row: len(row[10]))
summed_tweet_lengths = tweet_lengths.reduce(lambda a, b: a+b)
average_tweet_len = summed_tweet_lengths / tweet_count
results.append(f'Average tweet length in characters\t {average_tweet_len:.2f}')

# 1. k) What is the average length of a tweet text in terms of words?
#map a tweet to lists of words to count words, then map to number of words in a tweet, reduce to sum of all words and divide by number of tweets to get the average
tweet_sentences = tweets.map(lambda row: row[10])
tweet_word_count = tweet_sentences.map(lambda sent: (sent.count(' ')+1))

summed_word_counts = tweet_word_count.reduce(lambda first, second: first + second)
average_word_count = summed_word_counts / tweet_count

results.append(f'The average length of a tweet text is\t {average_word_count:.2f}')

# Store the result
sc.parallelize(results).coalesce(1, True).saveAsTextFile('./results/result_1.tsv')

