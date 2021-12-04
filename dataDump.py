import time
from json.decoder import JSONDecodeError

import yfinance
import pandas as pd
import pymongo
import datetime
import logging
import finnhub
import requests
from retrying import retry

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')


def download_data(symbol):
    # logging.info("Ticker symbol: %s", symbol)
    response = requests.get('https://finnhub.io/api/v1/stock/social-sentiment?symbol=' + symbol
                            + '&token=btig77748v6ula7e94rg')
    reddit_response = None if response is None or response.json() is None or len(response.json()) == 0 else \
        response.json().get("reddit")

    if reddit_response is not None and len(reddit_response) != 0 and reddit_response[0].get("mention") > 1:
        # logging.info("Symbol added: %s", symbol)
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongodb_database = mongodb_client.get_database("crawler")
        mongodb_collection = mongodb_database.get_collection("symbol_dump")
        push_symbol(mongodb_collection, symbol, response.json())
        ticker_symbol = yfinance.Ticker(symbol)
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        mongodb_database = mongodb_client.get_database("crawler")
        mongodb_collection = mongodb_database.get_collection("options_dump")
        # print(spy.options)
        try:
            if not ticker_symbol.options or len(ticker_symbol.options) == 0:
                logging.info("No options chain!")
                return
            for exp in ticker_symbol.options:
                opt = ticker_symbol.option_chain(exp)
                # [ contractSymbol, lastTradeDate, strike, lastPrice, bid, ask, change, percentChange, volume,
                # openInterest, impliedVolatility, inTheMoney, contractSize, currency ]
                # print(opt.calls)
                # Push OTM Calls
                df = pd.DataFrame(opt.calls)
                calls = df.query('~inTheMoney')
                top_frames = calls.nlargest(5, ["volume", "openInterest", "bid"], keep="first")
                # Push OTM Puts
                push_mongo(mongodb_collection, top_frames, "CALL", symbol)
                df = pd.DataFrame(opt.puts)
                puts = df.query('~inTheMoney')
                top_frames = puts.nlargest(5, ["volume", "openInterest", "bid"], keep="first")
                push_mongo(mongodb_collection, top_frames, "PUT", symbol)
        except IndexError:
            logging.error("Index error encountered: %s", symbol)
        except ConnectionError:
            logging.error("Connection Error encountered: %s", symbol)
        except JSONDecodeError:
            logging.error("Json Decode error: %s", symbol)
        except:
            logging.error("Unknown Error: %s", symbol)


def push_symbol(mongodb_collection, symbol, response):
    query_str = {"symbol": symbol}
    results = mongodb_collection.count_documents(query_str)
    if results == 0:
        insert_payload = {"symbol": symbol, "sentiment": response}
        insert_val = mongodb_collection.insert_one(insert_payload)
        logging.info("Inserted into MONGODB: %s", insert_val)
    else:
        update_val = mongodb_collection.update_one(query_str,
                                                   {"$set": {"sentiment": response}})
        logging.info("Updated into MONGODB: %s", update_val)


def push_mongo(mongodb_collection, top_frames, type_value, symbol):
    for index, row in top_frames.iterrows():
        # print(row["contractSymbol"])
        query_str = {"contractSymbol": row["contractSymbol"], "type": type_value}
        results = mongodb_collection.count_documents(query_str)
        if results == 0:
            insert_payload = {"contractSymbol": row["contractSymbol"],
                              "lastTradeDate": row["lastTradeDate"],
                              "strike": row["strike"],
                              "lastPrice": row["lastPrice"],
                              "bid": row["bid"],
                              "ask": row["ask"],
                              "change": row["change"],
                              "percentChange": row["percentChange"],
                              "volume": row["volume"],
                              "openInterest": row["openInterest"],
                              "impliedVolatility": row["impliedVolatility"],
                              "inTheMoney": row["inTheMoney"],
                              "contractSize": row["contractSize"],
                              "currency": row["currency"], "type": type_value,
                              "category": symbol, "createdAt": datetime.datetime.utcnow(),
                              "historicData": [{"bid": row["bid"],
                                                "ask": row["ask"],
                                                "change": row["change"],
                                                "percentChange": row["percentChange"],
                                                "volume": row["volume"],
                                                "openInterest": row["openInterest"],
                                                "impliedVolatility": row["impliedVolatility"],
                                                "inTheMoney": row["inTheMoney"],
                                                "createdAt": datetime.datetime.utcnow()}]}
            insert_val = mongodb_collection.insert_one(insert_payload)
            logging.info("Inserted into MONGODB: %s", insert_val)
        else:
            if results == 1:
                # logging.info("Existing symbol")
                date_start = datetime.datetime.now() - datetime.timedelta(days=1)
                false_val = "false"
                update_query_str = {"contractSymbol": row["contractSymbol"], "type": type_value,
                                    "$or": [{"lastModified": {"$exists": false_val}},
                                            {"lastModified": {"$gte": date_start}}]}
                # logging.info(update_query_str)
                update_results = mongodb_collection.count_documents(update_query_str)
                # logging.info("Update Results: %s", update_results)
                if update_results == 0:
                    update_val = mongodb_collection.update_one(update_query_str,
                                                               {"$set": {"lastTradeDate": row["lastTradeDate"],
                                                                         "strike": row["strike"],
                                                                         "lastPrice": row["lastPrice"],
                                                                         "bid": row["bid"],
                                                                         "ask": row["ask"],
                                                                         "change": row["change"],
                                                                         "percentChange": row["percentChange"],
                                                                         "volume": row["volume"],
                                                                         "openInterest": row["openInterest"],
                                                                         "impliedVolatility": row["impliedVolatility"],
                                                                         "inTheMoney": row["inTheMoney"],
                                                                         "contractSize": row["contractSize"],
                                                                         "currency": row["currency"],
                                                                         "category": symbol, "type": type_value,
                                                                         "lastModified": datetime.datetime.utcnow()}})
                    push_val = mongodb_collection.update_one(update_query_str,
                                                             {"$push": {"historicData": {
                                                                 "bid": row["bid"],
                                                                 "ask": row["ask"],
                                                                 "change": row["change"],
                                                                 "percentChange": row["percentChange"],
                                                                 "volume": row["volume"],
                                                                 "openInterest": row["openInterest"],
                                                                 "impliedVolatility": row["impliedVolatility"],
                                                                 "inTheMoney": row["inTheMoney"],
                                                                 "createdAt": datetime.datetime.utcnow()}}})
                    logging.info("Updated into MONGODB: %s, pushed into MONGODB: %s", update_val, push_val)
                else:
                    logging.info("Not updated")
            else:
                logging.error("Invalid contractSymbol")


def fetch_all_tickers():
    # Setup client
    finn_hub_client = finnhub.Client(api_key="btig77748v6ula7e94rg")
    return finn_hub_client.stock_symbols(exchange="US")


def fetch_all_tickers_mongo():
    # Setup client
    mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongodb_database = mongodb_client.get_database("crawler")
    mongodb_collection = mongodb_database.get_collection("symbol_dump")
    symbols = set()
    for x in mongodb_collection.find().limit(10000):
        symbols.add(x["symbol"])
    return symbols


@retry(wait_fixed=1200)
def continue_to_try():
    while True:
        # for ticker in fetch_all_tickers():
        #     # logging.info("Ticker symbol: %s", ticker)
        #     if ticker.get('mic') in ['XNYS', 'XNAS']:
        #         download_data(ticker.get('symbol'))
        for symbol in fetch_all_tickers_mongo():
            download_data(symbol)


if __name__ == '__main__':
    continue_to_try()
