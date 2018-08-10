'''
## Task 4
Map tweets to country and local time as key, and 1 as value.
We can then reduce these values "down" for each key.
The result is pairs of country-hour with tweet counts as values. 
The local hour has to be moved over from the key to the value, 
in order to reduce all country values to one country with the maximum hour as value.
A simple custom lambda function returns the row with the highest count. 
This is nessecary because the value is (hour, count), while a regular max function doesn't
know how to compare such tuples. 

Map: [https://s.ntnu.no/bigdata_coarto](https://s.ntnu.no/bigdata_coarto) .

'''
from pyspark import SparkContext
from pyspark import SparkConf
from datetime import datetime, timedelta

from util import load
tweets = load()


# The return value is an int of the hour of the local datetime object
# It divides the global time stamp by 1000, to get from millisec to sec
# Then create a timedelta object with the "offset" of the timezone
# Finally, the DateTime and TimeDelta objects are added and the hour is formatted as a string and finally int'ed 
# A bit slow
global_to_local = lambda utc, offset: int((datetime.fromtimestamp(int(utc)//1000) + \
                                       timedelta(seconds=int(offset))).\
                                       strftime("%H"))

# Get country, local time, 1
tweets_time = tweets.map(lambda row: ( (row[1], global_to_local(row[0], row[8])), 1))
# ((country, hour) , (1)

# Result is aggregated by summing this new column over the countries
aggregated_tweet_time = tweets_time.reduceByKey(lambda a, b: a+b)

# Map from : "(country,   hour), (count)" 
# To         "(country), (hour,   count)"
# Change key from country-hour to just country
aggregated_tweet_time = aggregated_tweet_time.map(lambda row: (row[0][0], (row[0][1], row[1])) )
# Returns: ('Costa Rica', (23, 680))


# We can now aggregate over all the countries
# This function returns the object with the highest count
# With country as key, this returns the hour with the highest count
max_tweets_country_time = aggregated_tweet_time.reduceByKey(lambda a, b: a if a[1]>b[1] else b)


# Store result
max_tweets_country_time.map(lambda row: f'{row[0]}\t{row[1][0]}\t{row[1][1]}').coalesce(1, True).saveAsTextFile('./results/result_4.tsv')
# <country_name>tab<begining_hour>tab<tweet_count>
