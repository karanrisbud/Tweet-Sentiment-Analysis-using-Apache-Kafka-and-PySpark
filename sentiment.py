import findspark
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import re
import json
from kafka.producer import KafkaProducer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, StringIndexer, RegexTokenizer, Word2Vec
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline
from pyspark.sql import Row, Column, SparkSession
import pyspark.sql.types as tp
from textblob import TextBlob

findspark.init()
lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words('english'))
producer = KafkaProducer(bootstrap_servers='localhost:9092')

def get_prediction(sentence): 
    sentence = sentence.lower()
    sentence = re.sub("\s+"," ", sentence)
    sentence = re.sub("\W"," ", sentence)
    sentence = re.sub(r"http\S+", "", sentence)
    sentence = ' '.join(word.lower() for word in sentence.split() if word not in stop_words and len(word) > 3)
    sentence = re.sub(r'\b\w{1,3}\b', '', sentence)
    res = TextBlob(sentence)
    senti = res.sentiment.polarity
    if senti > 0:
        senti = ('positive', 1)
    elif senti < 0:
        senti = ('negative', 1)
    elif senti == 0:
        senti = ('neutral', 1)
    return senti

def send_to_topic(rdd):
    tweets = rdd.reduceByKey(lambda x,y: x+y).collect()
    if tweets is not []:
        tweets = json.dumps(tweets).encode('utf-8')
        producer.send('tweets', tweets)

sc = SparkContext(appName = 'Tweets_Processing')
spark = SparkSession.builder.master('local[1]').appName('Tweets_Processing').getOrCreate()
scc = StreamingContext(sc, 3)
text_data = scc.socketTextStream('127.0.0.1', 5555)

stream = text_data.map(lambda x: get_prediction(x))
stream.foreachRDD(send_to_topic)

scc.start()
scc.awaitTermination()