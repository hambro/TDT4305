
'''
## Task 3
Map all tweets to country names as keys with values: 1, lat and lon. 
These values are row-wise summed, e.g. lat1 + lat2 + ... + latN (same for "1" and lon), per key. 
This gives a cumulated lat and lon value along with total count, for each key.
This is a SQL equivalent of a "GROUP BY" + "SUM". 
Finally, we divide the lat and lon values on their corresponding count values to give an average.
'''

import os

from util import load
tweets = load()


# Add a column to the data with (int "1", lat, long)
country_tweets = tweets.map(lambda row: (row[1], (1, 
                                                  float(row[11]), 
                                                  float(row[12])) ) )
# ('Colombia', (1, '4.74304973', '-74.06204947'))

# Sum the 1, lat and long for each country. Be careful with the indices!
country_tweet_count = country_tweets.reduceByKey(lambda a, b: (a[0]+b[0], 
                                                               a[1]+b[1], 
                                                               a[2]+b[2]))
# ('Paraguay', (258, -6568.194435439999, -14746.30951321))

# Strip away rare countries having a count <= 10
filtered_countries = country_tweet_count.filter(lambda row: row[1][0] > 10)
# ('Paraguay', (258, -6568.194435439999, -14746.30951321))

# Calculate the averages
averages = filtered_countries.map(lambda row: (row[0], row[1][1]/row[1][0], row[1][2]/row[1][0]))
# ('Paraguay', -25.458117966821703, -57.156238423294575)

# Store result
result = averages.map(lambda row: f'{row[0]}\t{row[1]}\t{row[2]}')
result.coalesce(1, True).saveAsTextFile('./results/result_3.tsv')
