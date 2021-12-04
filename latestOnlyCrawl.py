import datetime
import logging
import re
from typing import Set, Any

import praw
import pymongo
import unicodedata
from retrying import retry
from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
CLIENT_ID = '<CLIENT-ID>'
CLIENT_SECRET = '<CLIENT-SECRET>'
REDIRECT_URI = 'http://localhost:65010/auth_redirect'

REGEX_STRING = "[ ](?=[ ])|[^\\/,A-Za-z0-9 ]+"

GLOBAL_SYMBOLS = set()


@retry(wait_fixed=120000)
def crawler(symbols_to_find):
    global GLOBAL_SYMBOLS
    if symbols_to_find is None or len(symbols_to_find) == 0:
        symbols_to_find = GLOBAL_SYMBOLS
    user_agent = "wsb-crawl-app-2 by snarasi"
    r = praw.Reddit(user_agent=user_agent, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

    sub_reddit = r.subreddit('wallstreetbets')
    already_parsed = set()
    parsed_comments = set()
    comments_dict = set()
    train = set()
    counter = 0
    classifier = None

    while True:

        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongodb_database = mongodb_client.get_database("crawler")
        mongodb_collection = mongodb_database.get_collection("comment_sentiment")

        if classifier is None:

            for x in mongodb_collection.find().sort([("createdAt", pymongo.DESCENDING)]).limit(10000):
                if x["category"].upper() in symbols_to_find:
                    # logging.info("Adding to train:: %s", x["commentBody"])
                    train.add((x["commentBody"], x["classification"]))
            logging.info("Building the classifier")
            # Build the classifier
            classifier = NaiveBayesClassifier(train)

        counter += 1
        logging.info("COUNTER:: %s", counter)
        for comment in sub_reddit.comments(limit=1000):
            # logging.info("COMMENT:: %s", comment.body)
            for symbol in symbols_to_find:
                escape_string = re.escape(comment.body)
                pattern = '\\b'+symbol+'\\b'
                if len(symbol) >= 2 and re.search(pattern, escape_string) and comment.id not in already_parsed:
                    parsed_comments.add(comment.body)
                    already_parsed.add(comment.id)
                    comments_dict.add((comment.id, comment.body, symbol))
                    # logging.info("ALL COMMENT FOUND:: %s, %s", comment.body, symbol)

        # logging.info(already_parsed)
        # logging.info(parsed_comments)

        for comment_id, comment_body, symbol in comments_dict:
            testimonial = TextBlob(comment_body)
            query_str = {"commentId": comment_id}
            results = mongodb_collection.count_documents(query_str)
            unicode_string = unicodedata.normalize("NFKD", comment_body).strip()
            escaped_comment_body = re.sub(r"" + REGEX_STRING, "", unicode_string).strip()
            # logging.info("CLASSIFY:: %s", comment_body)
            # logging.info("FEATURE:: %s", classifier.classify(escaped_comment_body))
            if results == 0:
                insert_payload = {"commentId": comment_id, "commentBody": escaped_comment_body,
                                  "polarity": testimonial.polarity,
                                  "classification": "POS" if testimonial.polarity > 0 else "NEG",
                                  "category": symbol, "createdAt": datetime.datetime.utcnow()}
                insert_val = mongodb_collection.insert_one(insert_payload)
                logging.info("Inserted into MONGODB: %s", insert_val)


def fetch_all_tickers():
    # Setup client
    global GLOBAL_SYMBOLS
    mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongodb_database = mongodb_client.get_database("crawler")
    mongodb_collection = mongodb_database.get_collection("symbol_dump")
    symbols = set()
    for x in mongodb_collection.find().limit(10000):
        symbols.add(x["symbol"])
    logging.info("symbols: %s", symbols)
    GLOBAL_SYMBOLS = symbols
    return symbols


if __name__ == '__main__':
    crawler(fetch_all_tickers())
