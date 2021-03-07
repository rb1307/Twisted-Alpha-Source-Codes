import logging
from Files import i_o
from common_functions import get_todays_date, get_current_time, get_date_range, tweet_analysis, get_all_query_tweets,\
    media_space_result, media_space_index, calculate_tweet_reach, calculate_tweet_engagement, total_reach,\
    total_engagement, get_leader_details
from Crawler.twitter_crawler import TwitterUserTimeline
import media_presence
import report_generation
import json
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

current_date = get_todays_date()
current_time = get_current_time()


class LeaderBoard:
    def __init__(self, page_info=None, **kwargs):
        self.param = {
            'starter_key': 0,
        }
        self.param.update(kwargs)
        self.page_details = page_info
        self.key_status = i_o.get_key_status()

    def createleaderboard(self):
        data, media_space_data, media_space_details = self.generatekpivalues()
        profile_data = self.page_details
        for leader_handle, leader_response in data.items():
            follower_count = get_leader_details(page_details=profile_data).get(leader_handle,{})\
                .get("followers_count")
            tweets_data =leader_response.get('tweets', {})
            for t_id, details in tweets_data.items():
                reach = calculate_tweet_reach(details=details, followers=follower_count)
                engagement = calculate_tweet_engagement(details=details, followers=follower_count)
                details['reach'] = reach
                details['engagement'] = engagement
            all_tweet_reach = total_reach(data=leader_response.get("tweets"))
            all_tweet_engagement = total_engagement(data=leader_response.get("tweets"))
            kpi_result= {'total_reach': all_tweet_reach,
                    'total_engagement': all_tweet_engagement,
                    'media_space_score': media_space_data.get(leader_handle),
                    'Party':  get_leader_details(page_details=profile_data).get(leader_handle, {}).get('Party', ''),
                    #'Followers': followers_data.get(leader_handle, {}).get("followers_count", None),
                    'Name':  get_leader_details(page_details=profile_data).get(leader_handle, {}).get("name")}
            leader_response.update(kpi_result)
            leader_response['media_details'] = media_space_details.get(leader_handle, {})
        with open('result.json', 'w') as fp:
            json.dump(data, fp)
        return data

    def generatekpivalues(self):
        # prod_version = self.param.get("Purchased_version")
        media_score = {}
        media_space_details = {}
        output_data = {}
        # media_space_score = 0
        for leader_info in self.page_details:
            screen_name =leader_info.get("Name")
            window_period = get_date_range(day=int(self.param.get("crawl_day")), hour=int(self.param.get("crawl_hour")),
                                           minutes=int(self.param.get("crawl_minutes")))
            raw_tweets = TwitterUserTimeline(timeline_start=window_period.get("timeline_window_start"),
                                            timeline_end=window_period.get("timeline_window_end"),
                                            end_point="user_timeline", screen_name=screen_name,
                                            user_keys=self.key_status, authentication="Oauth").result()
            # all_tweets = user_timeline_object.result()
            tweets = tweet_analysis(tweets=raw_tweets, screen_name=screen_name)
            # NEED TO BE SORTED OUT
            # if self.param.get("run_mediaspace"):
            media_space_score, leader_tweets, tweet_counts = self.get_media_score(leader=screen_name,
                                                                                  query_word_list=[screen_name],
                                                                                  leader_tweets=tweets,
                                                                                  crawl_day=self.param.get("crawl_day"))

            # leader_tweets = self.get_retweet_reach(leader=key, leader_tweets=leader_tweets)

            # call Standard Version Module

            media_score[screen_name] = media_space_score
            media_space_details[screen_name] = tweet_counts
            output_data[screen_name] = {'tweets': tweets}
        return output_data, media_score, media_space_details

    def get_media_score(self, leader=None, query_word_list=None, leader_tweets=None, crawl_day=None):
        mspace_obj = media_presence.MediaPresence(leader=leader, query_word_list=query_word_list, crawl_day=crawl_day)
        all_tweets = get_all_query_tweets(query_t=mspace_obj.get_result())
        leader_tweets, tweet_count = media_space_result(leader=leader, leader_tweets=leader_tweets,
                                                        tweets=all_tweets)
        media_space_score = media_space_index(result=tweet_count)
        # have to connect here to the database component , leader_component
        return media_space_score, leader_tweets, tweet_count

    def generate_report(self):
        leaderboard_data = self.createleaderboard()
        report_configs ={
            'response_data': leaderboard_data,
            'client_name': self.param.get("client"),
            'Purched_version': self.param.get("product_version"),
        }
        report_obj = report_generation.TwitterReport(**report_configs)
        report_obj.x()
