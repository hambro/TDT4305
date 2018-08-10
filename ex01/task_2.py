'''
## Task 2
Extract country name from tweet with a simple map. We add 1 to each value.
This lets us count all tweets per country. This is done with reduceByKey(f), 
which iterates over pairs of key-value elements from the RDD instance dataset. 
This is re-run until there is only 1 K-V per distinct key. The value is the aggregated
value of the result by applying **f** over the dataset.
Finally the result is sorted alphabetically, then by count. 
'''

from util import load

tweets = load()

# Add column with int 1 on all tweets
country_intermidiate = tweets.map(lambda row: (row[1], 1))
# (country, 1)

# Reduce the key values by summing the "1" column, per key-value
country_count = country_intermidiate.reduceByKey(lambda a,b: a+b)
# (country, 19)

#Sort the countries alphatbetically
sorted_by_countries = country_count.sortBy(lambda row: row[0])
# (country, 19)

# Then sort by number of tweets
# count-ties are broken with alphabetically ordering
sorted_by_count_countries = sorted_by_countries.sortBy(lambda row: -row[1])
# (country, 900)

# Store result
result = sorted_by_count_countries.map(lambda row: f'{row[0]}\t{row[1]}')
result.coalesce(1, True).saveAsTextFile('./results/result_2.tsv')
