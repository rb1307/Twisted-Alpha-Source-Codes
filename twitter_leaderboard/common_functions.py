import logging
from Errors import UserDetailError
import numpy as np
import random
import tweepy
import datetime
import pandas as pd
import Errors

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')


def get_leader_details(page_details=[]):
    leader_details = {}
    for items in page_details:
        details = {'Party': items.get("Details", {}).get("Party", 1), 'name': items.get("Details", {}).get("name", 1),
                   'followers_count': items.get("Details", {}).get("followers_count", 1)}
        leader_details[items.get("Leader handle")] = details
    return leader_details


def strip_screen_name(data=None, handle_column=None, party_column=None):
    """

    :param data:
    :param handle_column:
    :return:
    """
    # screen_names = data[handle_column].tolist()
    tw_handles = {}
    for index, value in data.iterrows():
        raw_link = value[handle_column]
        link = raw_link.split("twitter.com/")[-1].split("?")[0]
        party = value[party_column]
        tw_handles[link] = party
    """for raw_link in screen_names:
        link = raw_link.split("twitter.com/")[-1].split("?")[0]
        tw_handles[link] = data[data[party_column]==]"""
    return tw_handles


def get_handle_details(api=None, screen_name=None):
    """

    :param api:
    :param screen_name:
    :return:
    """
    try:
        user_response = api.get_user(screen_name=screen_name, wait_on_rate_limit=True,
                                     wait_on_rate_limit_notify=True)
        user_response = user_response._json
    except UserDetailError:
        user_response = {}
    return user_response


def client_info(json=None):
    """

    :param json:
    :return:
    """
    keys = ['id', 'followers_count', 'statuses_count', 'name', 'description', 'verified']
    info_dict = {}
    # info_dict['date'] = datetime.date.today()
    for key in keys:
        info_dict[key] = json.get(key, None)
    return info_dict


def get_todays_date():
    """

    :return:
    """
    today = datetime.date.today()
    return today


def get_current_time():
    """

    :return:
    """
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time


def get_date_range(day=None, hour=None, minutes=None):
    """
    Function creates the timeline window for the tweets
    :return: most recent date , least recent date
    """
    # day=int(day)
    time_line_start = (datetime.datetime.now() - datetime.timedelta(days=day)).replace(hour=hour, minute=minutes). \
        strftime('%Y-%m-%d-%H-%M')

    time_line_end = (datetime.datetime.now()).replace(hour=hour, minute=minutes). \
        strftime('%Y-%m-%d-%H-%M')
    time_line_start = datetime.datetime.strptime(time_line_start, '%Y-%m-%d-%H-%M')
    time_line_end = datetime.datetime.strptime(time_line_end, '%Y-%m-%d-%H-%M')
    return {'timeline_window_start': time_line_start, 'timeline_window_end': time_line_end}


def tuple_to_int(data=()):
    data = data[0]
    return data


def tweet_analysis(tweets=[], screen_name="Batman"):
    """

    :param tweets: tweets in a  list
    :param screen_name: leader/user name
    :return: individual tweets(type->dict) with their details
    """
    tweet_count = {
        'reply': 0,
        'quote': 0,
        'retweet': 0,
    }
    tweet_details = {}
    for tweet in tweets:
        tweet = tweet._json
        hashtag_list = []
        data = {}
        flags = {
            'reply': 0,
            'quote': 0,
            'retweet': 0,
        }
        count, flags = get_tweet_type(tweet, count=tweet_count, flags=flags)
        retweet, appluase_rate = popularity(tweet=tweet)
        data['retweet'] = retweet
        data['applause_rate'] = appluase_rate
        hashtags, user_mentions = entities(tweet=tweet)
        hashtag_list.append(hashtags)
        data['hashtags'] = hashtag_list
        data['BO'] = "CMS"
        data['type'] = 'tweet'
        for key, value in flags.items():
            if value == 1:
                data['type'] = key
        tweet_details[tweet.get("id", '-1')] = data

    return tweet_details


def get_tweet_type(tweet=None, count=None, flags=None):
    if tweet.get("in_reply_to_status_id") is not None:
        flags['reply'] = 1
        count['reply'] = count.get("reply") + 1
    elif tweet.get("is_quote_status"):
        flags['quote'] = 1
        count['quote'] = count.get("quote") + 1
    elif tweet.get("retweeted_status", {}).get("id", '000000000') != '000000000':
        flags['retweet'] = 1
        count['retweet'] = count.get("retweet") + 1
    return count, flags


def popularity(tweet={}):
    applause_rate = tweet.get("favorite_count", 0)
    retweet_count = tweet.get("retweet_count", 0)
    return retweet_count, applause_rate


def entities(tweet={}):
    hashtags = tweet.get("hashtags", [])
    hashtag_data = collect_hashtags(hashtags=hashtags)
    user_mentions = tweet.get("user_mentions", [])
    usermention_data = collect_usermentions(user_mentions=user_mentions)
    return hashtag_data, usermention_data


def collect_hashtags(hashtags=[]):
    htgs = []
    if len(hashtags) != 0:
        for hashtag in hashtags:
            htgs.append(hashtag.get("text", ""))
    return htgs


def collect_usermentions(user_mentions=[]):
    users = []
    for user in user_mentions:
        users.append(user.get("screen_name"))
    return users


def get_all_query_tweets(query_t={}):
    all_tweets = []
    for key, value in query_t.items():
        all_tweets.extend(value)
    return all_tweets


def media_space_result(leader='', leader_tweets={}, tweets=[]):
    htgs_data = []
    us_mens = []
    mentions = 0
    user_details = []
    # pass flags and tweet_count here
    tweet_count = {'reply': 0,
                   'quote': 0,
                   'retweet': 0
                   }
    leader_tweets = get_intial_counts(leader_tweets=leader_tweets)
    for tweet in tweets:
        flags = {
            'reply': 0,
            'quote': 0,
            'retweet': 0,
        }
        tweet = tweet._json
        tweet_count, flags = get_tweet_type(tweet=tweet, count=tweet_count, flags=flags)
        if flags.get("reply") == 1:
            status_id, tweet_type = reply_tweets(leader=leader, tweet=tweet)
            if tweet_type == 'mention':
                mentions = mentions + 1
            elif tweet_type == 'reply':
                leader_tweets = map_reply(leader_tweets=leader_tweets, tweet=tweet, status_id=status_id)
                user_details.append(user_details)
        elif flags.get("quote") == 1:
            status_id, screen_name = quoted_tweets(tweet=tweet)
            leader_tweets = map_quote(leader_tweets=leader_tweets, tweet=tweet, status_id=status_id)
        elif flags.get("retweet") == 1:
            status_id = re_tweet(tweet=tweet)
            # leader_tweets =map_retweet(leader_tweets=leader_tweets , tweet=tweet , status_id=status_id)
        htgs, us_men = entities(tweet.get("entities"))
        htgs_data.append(htgs)
        us_mens.append(us_men)
    tweet_count['reply'] = tweet_count.get("reply") - mentions
    tweet_count['mentions'] = mentions
    tweet_count['tweets'] = len(tweets) - (tweet_count.get("reply") + tweet_count.get("quote") +
                                           tweet_count.get("retweet") + tweet_count.get("mentions"))
    logging.info("Details of the attributes are " + str(tweet_count))
    # files.Output(state_name="arnatak", file_name="media_space.json")
    return leader_tweets, tweet_count


def get_intial_counts(leader_tweets={}):
    for key, value in leader_tweets.items():
        value['reply_count'] = 0
        value['quote_count'] = 0
        value['second_degree_reach'] = 0
    return leader_tweets


def reply_tweets(leader='narendramodi', tweet={}):
    status_id = tweet.get("in_reply_to_status_id", '-1')
    screen_name = tweet.get("in_reply_to_screen_name", None)
    if screen_name == leader:
        tweet_type = 'reply'
    else:
        tweet_type = 'mention'
    return status_id, tweet_type


def map_reply(leader_tweets={}, tweet={}, status_id=-1):
    if status_id in leader_tweets:
        leader_tweets.get(status_id)['reply_count'] = leader_tweets.get(status_id).get('reply_count') + 1
        leader_tweets = get_second_degree(tweet=tweet, leader_tweets=leader_tweets, status_id=status_id)
    return leader_tweets


def quoted_tweets(tweet={}):
    quoted_status = tweet.get("quoted_status", {})
    status_id = quoted_status.get("id", '-1')
    screen_name = quoted_status.get("user", {}).get("screen_name", None)
    return status_id, screen_name


def map_quote(leader_tweets={}, tweet={}, status_id=-1):
    if status_id in leader_tweets:
        leader_tweets.get(status_id)['quote_count'] = leader_tweets.get(status_id).get('quote_count') + 1
        leader_tweets = get_second_degree(tweet=tweet, leader_tweets=leader_tweets, status_id=status_id)
    return leader_tweets


def re_tweet(tweet={}):
    retweeted_status = tweet.get("retweeted_status", {})
    status_id = retweeted_status.get("id")
    return status_id


def get_second_degree(tweet={}, leader_tweets={}, status_id=-1):
    user_details = user_info(json=tweet.get("user"))
    second_degree_reach = user_details.get("followers_count")
    leader_tweets.get(status_id)['second_degree_reach'] = leader_tweets.get(status_id).get('second_degree_reach') + \
                                                          second_degree_reach
    return leader_tweets


def user_info(json=None, result=None):
    """
    :param json: dictionary :: user details
    :param leader_name:
    :param result:
    :return: info_dict ::(type-->dictionary)
    """
    resp = []
    keys = ['id', 'name', 'screen_name', 'location', 'description', 'followers_count', 'created_at',
            'statuses_count', 'geo_enabled', 'verified']
    if json is not None:
        info_dict = {}
        # info_dict['date'] = datetime.date.today()
        for key in keys:
            info_dict[key] = json.get(key, None)
        info_dict['created_at'] = str_to_date(dt_string=json.get('created_at'))
        # info_dict['recent_status_timestamp'] = str(str_to_date(dt_string=json.get("status",{}).get("created_at")))
        return info_dict
    elif result is not None:
        for follower in result:
            info_dict = {}
            follower = follower._json
            for key in keys:
                info_dict[key] = follower.get(key)
            info_dict['created_at'] = str_to_date(dt_string=info_dict.get('created_at'))
            resp.append(info_dict)
        return resp


def media_space_index(result={}):
    logging.info("Media space scoring....")
    score = result.get("tweets") * 0.30 + result.get("quote") * 0.25 + result.get("reply") + 0.25 + \
            result.get("mentions") * 0.15 + result.get("retweet") * 0.10

    return score


def calculate_tweet_reach(details={}, followers=0):
    if details.get("type", "") != 'retweet':
        reach = details.get("second_degree_reach", 0) + details.get("retweet_reach", 0) + followers
    else:
        reach = 0

    return reach


def calculate_tweet_engagement(details={}, followers=0):
    followers_1k = int(followers / 1000)
    if details.get("type", "") != 'retweet':
        engagement = details.get('retweet', 0) / followers_1k + details.get("applause_rate", 0) / followers_1k + \
                     details.get("reply_count", 0) / followers_1k + details.get("quote_count", 0) / followers_1k
    else:
        engagement = 0
    return engagement


def total_reach(data={}):
    t_reach = 0
    for t_id, details in data.items():
        t_reach = t_reach + details.get("reach", 0)
    return t_reach


def total_engagement(data={}):
    t_engagement = 0
    for t_id, details in data.items():
        t_engagement = t_engagement + details.get("engagement")
    return t_engagement


def day_tweets(tweets=[], crawl_day=1):
    result = []
    for item in tweets:
        tweet_data = str_to_date(dt_string=item._json.get("created_at"))
        if tweet_data == datetime.date.today() - datetime.timedelta(crawl_day):
            result.append(item)
    return result


def convert_tweepy_date_todatetime(tweepy_date=None):
    datetime_in_correctformat = datetime.datetime.strftime(
        datetime.datetime.strptime(tweepy_date, '%a %b %d %H:%M:%S +0000 %Y'),
        '%Y-%m-%d %H:%M:%S')
    return datetime_in_correctformat


def str_to_date(dt_string=''):
    """
    :param dt_string:  date time in string
    :return: date (type--> datetime object)
    """
    dt_string = convert_tweepy_date_todatetime(tweepy_date=dt_string)
    # date_format = '%a %b %d %H:%M:%S +0000 %Y'
    date_format = '%Y-%m-%d %H:%M:%S'
    if isinstance(dt_string, str):
        date_time_obj = datetime.datetime.strptime(dt_string, date_format)
        return date_time_obj.date()
    else:
        raise Errors.StringToDateError()
