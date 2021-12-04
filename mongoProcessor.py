import datetime

from textblob import TextBlob
import pymongo
import re
from retrying import retry
import time
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongodb_database = mongodb_client.get_database("crawler")
mongodb_collection = mongodb_database.get_collection("comment_sentiment")

query_str = {"comment_category": {"$exists": False}}

DIGIT_REGEX = "^(\\d+)([c|p|C|P])"
DATE_REGEX = "^\\d+[\\|/]\\d+$"
OP_REGEX = "\\bcall|\\bput"


def update_mongodb(x, symbols_to_find):
    for symbol in symbols_to_find:
        if symbol in x["comment_body"]:
            set_str = {"$set": {"comment_category": symbol}}
            update_str = {"_id": x["_id"]}
            mongodb_collection.find_one_and_update(update_str, set_str, upsert=True)


def mongodb_extractor(symbols_to_find):
    for x in mongodb_collection.find(query_str):
        # logging.info(x)
        update_mongodb(x, symbols_to_find)


@retry(wait_fixed=120000)
def mongodb_reader_all(symbols_to_find):
    while True:
        for x in mongodb_collection.find().sort([("createdAt", pymongo.DESCENDING)]).limit(10000):
            comment_body = x["commentBody"]
            blob = TextBlob(comment_body)
            ngram_list = blob.ngrams(n=4)
            ops_matched = False
            value_matched = False
            date_matched = False
            value = ""
            date = ""
            operation = ""
            for ngram in ngram_list:
                for symbol in symbols_to_find:
                    if symbol in ngram[0:1]:
                        # logging.info("SYMBOL MATCHED: %s", ngram)
                        for item in ngram[1:3]:
                            matches = re.search(r"" + DIGIT_REGEX, item, re.IGNORECASE)
                            if matches:
                                # logging.info("VALUE Matched", item)
                                value_matched = True
                                value = matches.group(1)
                                operation = "CALL" if matches.group(2).lower() == "c" else "PUT"
                            if re.match(r"" + DATE_REGEX, item):
                                # logging.info("DATE Matched", ngram)
                                date_matched = True
                                date = item
                            if re.match(r"" + OP_REGEX, item, re.IGNORECASE):
                                # logging.info("OP Matched", ngram)
                                ops_matched = True
                                operation = item

            if value_matched or date_matched or ops_matched:
                # logging.info("ID, VALUE, DATE, OP :: %s, %s, %s, %s", x["commentId"], value, date, operation)
                update_tag = {"value": value, "date": date, "operation": operation}
                set_str = {"$set": {"tags": update_tag}}
                update_str = {"_id": x["_id"]}
                mongodb_collection.find_one_and_update(update_str, set_str, upsert=True)
                # logging.info(x)
        logging.info("Sleeping process for 5 minutes")

        # tags_query = "{\"tags\": {$exists: true}, \"tags.value\": {$ne: \"\"}, \"tags.operation\": {$ne: \"\"}, " \
        #              "\"tags.date\": {$ne: \"\"}} "
        true_val = "true"
        empty_val = ""
        tags_query = {"tags": {"$exists": true_val}, "tags.value": {"$ne": empty_val}}
        logging.info("Tags Query: %s", tags_query)
        count_query = mongodb_collection.count_documents(tags_query)
        logging.info("Tags Result: %s", count_query)
        category_dict = dict()
        for x in mongodb_collection.find(tags_query).sort([("createdAt", pymongo.DESCENDING)]).limit(10000):
            tag_object = x["tags"]
            category = x["category"]
            # logging.info("tag_object: %s", tag_object)
            tag_list = []
            if category not in category_dict:
                tag_list.append(tag_object)
                category_dict[category] = tag_list
            else:
                tag_list = category_dict.get(category)
                tag_list.append(tag_object)
                category_dict[category] = tag_list

        for category in category_dict:
            logging.info("Option contract: Symbol: %s, Tags: %s", category, category_dict[category])

        date_start = datetime.datetime.now() - datetime.timedelta(days=1)
        date_end = datetime.date.today() + datetime.timedelta(days=1)
        today_query = {"createdAt": {"$gte": date_start}}
        logging.info("Today Query: %s", today_query)
        count_query = mongodb_collection.count_documents(today_query)
        logging.info("Today Result: %s", count_query)
        category_dict = dict()
        for x in mongodb_collection.find(today_query).sort([("createdAt", pymongo.DESCENDING)]).limit(10000):
            category = x["category"]
            if category not in category_dict:
                category_dict[category] = 1
            else:
                counter = category_dict.get(category)
                category_dict[category] = counter+1
        count_limit = 0
        for category in sorted(category_dict, key=category_dict.get, reverse=True):
            if count_limit > 10:
                break
            else:
                logging.info("Mentions: Symbol %s, Count: %s", category, category_dict[category])
                count_limit += 1

        time.sleep(300)


def mongodb_delete():
    deleted = mongodb_collection.delete_many({})
    logging.info(deleted.deleted_count)


def fetch_all_tickers():
    # Setup client
    m_client = pymongo.MongoClient("mongodb://localhost:27017/")
    m_database = m_client.get_database("crawler")
    m_collection = m_database.get_collection("symbol_dump")
    symbols = set()
    for x in m_collection.find().limit(10000):
        symbols.add(x["symbol"])
    return symbols


if __name__ == '__main__':
    # mongodb_extractor()
    # mongodb_delete()
    mongodb_reader_all(fetch_all_tickers())
