import sys

from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import twitter_samples, stopwords
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from nltk import FreqDist, classify, NaiveBayesClassifier
import json
import hashlib
from kafka import KafkaConsumer
import re, string, random
from elasticsearch import Elasticsearch

def remove_noise(tweet_tokens, stop_words=()):
    cleaned_tokens = []

    for token, tag in pos_tag(tweet_tokens):
        token = re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|' \
                       '(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', token)
        token = re.sub("(@[A-Za-z0-9_]+)", "", token)

        if tag.startswith("NN"):
            pos = 'n'
        elif tag.startswith('VB'):
            pos = 'v'
        else:
            pos = 'a'

        lemmatizer = WordNetLemmatizer()
        token = lemmatizer.lemmatize(token, pos)

        if len(token) > 0 and token not in string.punctuation and token.lower() not in stop_words:
            cleaned_tokens.append(token.lower())
    return cleaned_tokens


def get_all_words(cleaned_tokens_list):
    for tokens in cleaned_tokens_list:
        for token in tokens:
            yield token


def get_tweets_for_model(cleaned_tokens_list):
    for tweet_tokens in cleaned_tokens_list:
        yield dict([token, True] for token in tweet_tokens)


def getSentimentAnalyzer():
    stop_words = stopwords.words('english')
    positive_tweet_tokens = twitter_samples.tokenized('positive_tweets.json')
    negative_tweet_tokens = twitter_samples.tokenized('negative_tweets.json')

    positive_cleaned_tokens_list = []
    negative_cleaned_tokens_list = []

    for tokens in positive_tweet_tokens:
        positive_cleaned_tokens_list.append(remove_noise(tokens, stop_words))

    for tokens in negative_tweet_tokens:
        negative_cleaned_tokens_list.append(remove_noise(tokens, stop_words))

    all_pos_words = get_all_words(positive_cleaned_tokens_list)

    freq_dist_pos = FreqDist(all_pos_words)

    positive_tokens_for_model = get_tweets_for_model(positive_cleaned_tokens_list)
    negative_tokens_for_model = get_tweets_for_model(negative_cleaned_tokens_list)

    positive_dataset = [(tweet_dict, "Positive")
                        for tweet_dict in positive_tokens_for_model]

    negative_dataset = [(tweet_dict, "Negative")
                        for tweet_dict in negative_tokens_for_model]

    dataset = positive_dataset + negative_dataset
    random.shuffle(dataset)
    train_data = dataset
    classifier = NaiveBayesClassifier.train(train_data)
    return classifier


def addId(data):
    j = json.dumps(data).encode('ascii', 'ignore')
    data['doc_id'] = hashlib.sha224(j).hexdigest()
    return (data['doc_id'], json.dumps(data))


def doClassify(record):
    global analyzer
    tweet = record
    tokens = remove_noise(word_tokenize(tweet))
    sentiment = analyzer.classify(dict([token, True] for token in tokens))
    category="cornavirus"
    if "trump" in tweet:
        category="trump"

    esDoc = {
        "tweet": tweet,
        "sentiment": sentiment,
        "category": category
    }
    print(esDoc)
    return esDoc
    # return str(tweet + " " + sentiment)


if __name__ == "__main__":
    argLen = len(sys.argv)
    if (argLen != 2):
        print()
        print(
            "Usage: <script_name> <topic name>")
        print()
        exit(1)
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    topic=str(sys.argv[1])
    consumer.subscribe([topic])
    analyzer = getSentimentAnalyzer()
    count=0
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    for message in consumer:
        data = doClassify(message.value)
        es.index(index='tweet-index', doc_type='default',id=count, body=data)
        count+=1




