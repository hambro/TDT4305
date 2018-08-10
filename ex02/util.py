from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext()
sc.setLogLevel('OFF')

def load_tweets(file_path, sample=False):
	raw_tweets = sc.textFile(file_path)

	if sample:
		# Sample 1% of the original dataset
		print("SAMPLING")
		raw_tweets = raw_tweets.sample(False, 0.01)

	# Sort every tweet into a list
	tweets = raw_tweets.map(lambda row: row.split('\t'))
	tweets.persist()

	return tweets

def load_stopwords():
	words = sc.textFile("./data/stop_words.txt")
	# Map the words to (word, None) to make it compatible with most operations
	words = words.map(lambda row: (row, None))
	words.persist()
	return words