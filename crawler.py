import datetime
import logging
import re
import time

import praw
import pymongo
import unicodedata
from praw.models import MoreComments
from retrying import retry
from textblob import TextBlob
from textblob.classifiers import NaiveBayesClassifier

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
CLIENT_ID = '<CLIENT-ID>'
CLIENT_SECRET = '<CLIENT-SECRET>'
REDIRECT_URI = 'http://localhost:65010/auth_redirect'

REGEX_STRING = "[ ](?=[ ])|[^\\/,A-Za-z0-9 ]+"


@retry(wait_fixed=120000)
def crawler(symbols_to_find):
    user_agent = "wsb-crawl-app-2 by snarasi"
    r = praw.Reddit(user_agent=user_agent, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)

    sub_reddit = r.subreddit('wallstreetbets')
    already_parsed = set()
    parsed_comments = set()
    comments_dict = set()
    train = set()
    counter = 0

    mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongodb_database = mongodb_client.get_database("crawler")
    mongodb_collection = mongodb_database.get_collection("comment_sentiment")

    for x in mongodb_collection.find().sort([("createdAt", pymongo.DESCENDING)]).limit(10000):
        if x["category"].upper() in symbols_to_find:
            # logging.info("Adding to train:: %s", x["commentBody"])
            train.add((x["commentBody"], x["classification"]))
    logging.info("Building the classifier")
    # Build the classifier
    classifier = NaiveBayesClassifier(train)

    while True:
        counter += 1
        logging.info("COUNTER:: %s", counter)
        for comment in sub_reddit.comments(limit=10):
            # logging.info("COMMENT:: %s", comment.body)
            for symbol in symbols_to_find:
                escape_string = re.escape(comment.body)
                pattern = '\\b'+symbol+'\\b'
                if len(symbol) >= 2 and re.search(pattern, escape_string) and comment.id not in already_parsed:
                    parsed_comments.add(comment.body)
                    already_parsed.add(comment.id)
                    comments_dict.add((comment.id, comment.body, symbol))
                    # logging.info("ALL COMMENT FOUND:: %s", comment.body)

        parse_submission(symbols_to_find, already_parsed, comments_dict, parsed_comments, r, sub_reddit.hot())
        parse_submission(symbols_to_find, already_parsed, comments_dict, parsed_comments, r, sub_reddit.new(limit=10))

        time.sleep(10)

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


def parse_submission(symbols_to_find, already_parsed, comments_dict, parsed_comments, r, sub_reddit_type):
    for submission in sub_reddit_type:
        subtext = r.submission(submission)
        # logging.info("Found POST:: %s", vars(subtext))
        try:
            for top_level_comment in subtext.comments:
                if isinstance(top_level_comment, MoreComments):
                    # logging.info("TOP")
                    continue
                for symbol in symbols_to_find:
                    # logging.info("Symbol: %s, comment: %s", symbol, top_level_comment.body)
                    escape_string = re.escape(top_level_comment.body)
                    pattern = '\\b'+symbol+'\\b'
                    if len(symbol) >= 2 and re.search(pattern,
                                                      escape_string) and top_level_comment.id not in already_parsed:
                        parsed_comments.add(top_level_comment.body)
                        already_parsed.add(top_level_comment.id)
                        comments_dict.add((top_level_comment.id, top_level_comment.body, symbol))
                        # logging.info("FOUND:: %s", top_level_comment.body)
        except:
            logging.error("Comments not found")
            continue


def fetch_all_tickers():
    # Setup client
    mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongodb_database = mongodb_client.get_database("crawler")
    mongodb_collection = mongodb_database.get_collection("symbol_dump")
    symbols = set()
    for x in mongodb_collection.find().limit(10000):
        symbols.add(x["symbol"])
    return symbols


if __name__ == '__main__':
    crawler(fetch_all_tickers())
