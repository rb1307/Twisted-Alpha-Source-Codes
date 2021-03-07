"""
@Romit Bhattacharyya
@module_name : tw_crawler
@info :  flag  : 0 = free
                 1 = in use
                -1 = blocked
  api_endpoints : search
                  followers
                  user_timeline
                  retweeters
superclass = tw_crawler
subclasses : tw_followers
             tw_user_timeline
             tw_retweets
             tw_search

"""
import time
from api_keys import twitter_api_functions
import logging
from Files import i_o
from Crawler import crawler_functions
import common_functions

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


class TwitterCrawler:
    def __init__(self, **kwargs):  # kwargs

        self.values = {
            'end_point': 'search',
            'starter_key_number': '2',
            'query': None,
            'key_words': [],
            'screen_name': None,
            'users': [],
            'id': [],
            'count': 2,
            'loop': 1,
            'wait_on_rate_limit': True,
            'wait_on_rate_limit_notify': True,
            'tweet_mode': 'extended',
            'timeline_start': None,
            'timeline_end': None,
            'followers_count': 0,
            'authentication_type': "Oauth",
            'user_keys': i_o.get_key_status()     # FIRST INSTANCE OF CONNECTING TO THE DATABASE
        }
        self.values.update(kwargs)
        self.values["user_keys"] = crawler_functions.reset_rate_limit_status(end_point=self.values.get("end_point"),
                                                           user_keys=self.values.get("user_keys"))
        self.current_key_number = self.values.get('starter_key_number')


class TwitterUserTimeline(TwitterCrawler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.end_point = 'user_timeline'
        logging.info("Timeline Crawl for " + str(self.values.get("screen_name")))


    def result(self):
        user_keys = self.values.get("user_keys")
        # logging.info("Key Number  picked up  is  " + str(self.current_profile))
        user_keys.get(str(self.current_key_number)).get("api_" + self.values.get('end_point'))['flag'] = 1
        tweepy_api = twitter_api_functions.get_api_authentication(authentication=self.values.get('authentication'),
                                                                  number=self.current_key_number)
        try:

            limit = tweepy_api.rate_limit_status().get("resources").get("statuses").get("/statuses/user_timeline")\
                        .get("remaining"),
            if type(limit) == tuple:
                limit = common_functions.tuple_to_int(data=limit)
        except Exception as e:
            logging.error("Error while retrieving limit from the api due to : " + str(e) + "-- Key limit status reading"
                                                                                           "from file")
            limit = user_keys.get(self.current_key_number).get("api_" + self.values.get("end_point")).get('limit')
            logging.warning("Limit for key no " + str(self.values.get("current_profile")) + "in database is --" +
                            str(limit))
        limit = limit - 1
        if limit <= 0:
            logging.warning("Tweepy Limit reached for the endpoint --user_timeline with key no. :" +
                            str(self.current_key_number))
            tweepy_api, self.current_key_number, user_keys = crawler_functions.rotate_key(api=tweepy_api,
                                                    authentication_type=self.values.get('authentication'),
                                                      profile_no=self.current_key_number,
                                                      end_point=self.end_point, user_keys=self.values.get("user_keys"))
            try:
                limit = tweepy_api.rate_limit_status().get("resources").get("followers").get('/followers/list') \
                                                                                        .get("remaining")
            except Exception:
                limit = user_keys.get(self.current_key_number).get("api_" + self.values.get("end_point")).get('limit')
        all_tweets = crawler_functions.get_timeline_tweets(api=tweepy_api, screen_name=self.values.get("screen_name"),
                                          timeline_start=self.values.get("timeline_start"),
                                        timeline_end=self.values.get("timeline_end"))
        user_keys =crawler_functions.update_blocked_key_credentials(user_keys=user_keys,
                                                            end_point=self.values.get("end_point"),
                                                             current_key_number=self.current_key_number,
                                                             api=tweepy_api)

        # update limit
        user_keys.get(str(self.current_key_number)).get("api_" + self.values.get('end_point'))['limit'] = limit
        # write the key limits back to database
        i_o.output_current_limit(user_keys=user_keys)       # write to database

        return all_tweets


class TwitterSearch(TwitterCrawler):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def result(self):
        count = 0
        user_keys = self.values.get("user_keys")
        user_keys.get(str(self.current_key_number)).get("api_" + self.values.get('end_point'))['flag'] = 1
        tweepy_api = twitter_api_functions.get_api_authentication(authentication=self.values.get('authentication'),
                                                                  number=self.current_key_number)
        try:
            limit = tweepy_api.rate_limit_status().get("resources").get("search").get("/search/tweets").get("remaining")
        except Exception:
            logging.error("Error while retrieving limit from the api")
            limit =user_keys.get(self.current_key_number).get("api_" + self.values.get("end_point")).get('limit')
        logging.info("Profile number = " + str(self.current_key_number) + " with starting limit = " + str(limit))
        last_id_captured = None
        tweets = []
        # self.values.get("count")

        while count < self.values.get("count"):
            limit = limit - 1
            if limit == 0:
                """tweepy_api, self.current_key_number, user_keys = crawler_functions.rotate_key(
                                                                    api=tweepy_api,
                                                                    authentication_type=self.values.get('authentication'),
                                                                    profile_no=self.current_key_number,
                                                                    user_keys= user_keys,
                                                                    end_point=self.values.get("end_point"))

                limit = tweepy_api.rate_limit_status().get("resources").get("search").get("/search/tweets")\
                    .get("remaining")"""
                time.sleep(60 * 5)
            result, last_id_captured = crawler_functions.search_query(word=self.values.get("query"), api=tweepy_api,
                                                              last_id_captured=last_id_captured)

            tweets.extend(result)
            if len(result) == 0:
                break
            last_id_captured = last_id_captured
            count = count + 1
        user_keys = crawler_functions.update_blocked_key_credentials(user_keys=user_keys,
                                                            end_point=self.values.get("end_point"),
                                                             current_key_number=self.current_key_number,
                                                             api=tweepy_api)
        user_keys.get(str(self.current_key_number)).get("api_" + self.values.get('end_point'))['limit'] = limit
        i_o.output_current_limit(user_keys=user_keys)
        return tweets