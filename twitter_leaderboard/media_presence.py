import logging
from Crawler import twitter_crawler
from common_functions import day_tweets


class MediaPresence:
    def __init__(self, leader='', query_word_list=None, crawl_day=None):
        # currently media presence depends on indexing module, as the keywords are passed as paremeter
        # We should remove this dependence, pass the leader _name only, pick up the words from the database
        logging.info("")
        self.leader = leader
        self.query_word_list = query_word_list
        # self.query_word_list = query_word_list
        self.crawl_day = crawl_day
        logging.info("Initializing  media presence for " + str(leader))

    def get_result(self):
        all_tweets = {}
        for q_word in self.query_word_list:
            crawler_obj = twitter_crawler.TwitterSearch(query=q_word, count=50, end_point='search', authentication="Appouth")
            result = crawler_obj.result()
            result = day_tweets(tweets=result, crawl_day=int(self.crawl_day))
            all_tweets[q_word]=result
            logging.info("Number of tweets captured for the query word is " + str(len(result)))
        return all_tweets




























