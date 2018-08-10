'''
## Task 8
First, we load the tweets into an rdd, and map the values to correct types, because we need latitude and longitude as floats to be able to get correct max and min.
We then create a dataframe from the rdd, and add correct names to the columns with the function createDataFrame(), so that we can use sql functions on the dataframe.
To count all tweets we simply us the function count() which returns the count of all rows in the dataframe.
To find distinct values in specific columns, we simply use the function agg() and countDistinct(), which counts the distinct values in the specified columns. Nice.
To find min and max values in specific columns, we also use agg() and simply use {"column_name", "min/max"} as parameter, which finds the min/max value in the specified column.

### a
![](./img/1.png)
### b
![](./img/2.png)
### c
![](./img/3.png) 
### d
![](./img/4.png)
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

from util import load, sc
tweets = load()

#map values to correct type to be able to find max and min
tweets = tweets.map(lambda a: (int(a[0]), a[1], a[2], a[3], a[4], a[5], a[6], a[7], int(a[8]), int(a[9]), a[10], float(a[11]), float(a[12])))

#Set up spark session
spark = SparkSession.builder.master("local").appName("task_8").getOrCreate()
#Create dataframe of tweets and name the columns
tweet_df = spark.createDataFrame(tweets, ['utc_time', 'country_name', 'country_code', 'place_type', 'place_name', 'language', 'user_screen_name', 'username', 'timezone_offset', 'number_of_friends', 'tweet_text', 'latitude', 'longitude'])

#tweet_df.show() #Shows table of all tweets
#(a) Number of tweets. use count() to count all tweets, print result
numberOfTweets = tweet_df.count()
print("(a)\nNumber of tweets: " + str(numberOfTweets) + "\n")

#(b) Number of distinct users (username)
#(c) Number of distinct countries (country name)
#(d) Number of distinct places (place name)
#(e) Number of distinct languages users post tweets
#find distinct values by using countDistinct
print("(b)-(e)")
distinct_values = tweet_df.agg(countDistinct("username"), countDistinct("country_name"), countDistinct("place_name"), countDistinct("language"))
distinct_values.show() #Shows table of distinct values

#(f) Minimum values of latitude and longitude
print("(f)")
min_values = tweet_df.agg({"latitude": "min", "longitude": "min"})
min_values.show() #Shows table of minimum latitude and longitude

#(g) Maximum values of latitude and longitude
print("(g)")
max_values = tweet_df.agg({"latitude": "max", "longitude": "max"})
max_values.show() #Shows table of maximum latitude and longitude
