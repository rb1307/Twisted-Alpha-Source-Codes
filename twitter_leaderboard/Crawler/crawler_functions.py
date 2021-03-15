import time
import logging
from api_keys import twitter_api_functions
import datetime
import dateutil.relativedelta
from Files import db_connect
import tweepy


def get_timeline_tweets(api=None, screen_name='', timeline_start=None, timeline_end=None):
    """

    :param timeline_end:
    :param timeline_start:
    :param api: tweepy authentication
    :param screen_name : twitter_handle
    :return: list of tweets
    """
    all_tweets = []
    try:
        tweets = api.user_timeline(screen_name=screen_name, count=100, result_type='recent', wait_on_rate_limit=True,
                                   wait_on_rate_limit_notify=True, tweet_mode='extended')
    except tweepy.error.TweepError as e:
        tweets = []
        logging.warning("Failed to get timeline tweets for " + screen_name + "due to error --" + str(e))
    for items in tweets:
        utc_datetime = items.created_at
        tweet_local_time = datetime_from_utc_to_local(utc_datetime=utc_datetime)
        if (tweet_local_time >= timeline_start) and (tweet_local_time <= timeline_end):
            all_tweets.append(items)
        else:
            break

    logging.info("Number of timeline tweets for  " + screen_name + " on " + str(datetime.datetime.today()) + " is " +
                 str(len(all_tweets)))

    return all_tweets


def datetime_from_utc_to_local(utc_datetime=None):
    now_timestamp = time.time()
    offset = datetime.datetime.fromtimestamp(now_timestamp) - datetime.datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset


def rotate_key(api=None,
               profile_no=None,
               end_point=None,
               authentication_type='Oauth',
               user_keys=None):
    new_key_data={}
    logging.warning("Key no : " + str(profile_no) + ". End_point :" + end_point +
                    ".limit status has reached.Rotation of keys initiated.\n ****************************")
    # DATABASE READ OF USER - KEYS
    try:
        tweepy_api, current_key_number, user_keys = shuffle_key(
            tweepy_api=api,
            authentication=authentication_type,
            current_key_number=profile_no,
            user_keys=user_keys,
            end_point=end_point)
        # the limit is picked up after a new key is restored.
        limit = get_remaining_hit(end_point=end_point, api=tweepy_api)
        new_key_data['api'] = tweepy_api
        new_key_data['current_key_number'] = current_key_number
        new_key_data['user_keys'] = user_keys
        new_key_data['current_limit'] = limit
    except Exception as e:
        logging.error("Unable to rotate key successfuly.Error found :" + str(e))
    return new_key_data


def get_remaining_hit(end_point=None,api=None):
    """
    :param end_point:
    :param api:
    :return:
    """
    try:
        switcher = {
            "user_timeline": api.rate_limit_status().get("resources", {}).get("statuses",{})
                .get( "/statuses/user_timeline",{}).get("remaining"),
            "search": api.rate_limit_status().get("resources", {}).get("search", {}).get("/search/tweets",{})
                .get("remaining"),
            "followers": api.rate_limit_status().get("resources", {}).get("followers",{}).get('/followers/ids',{})
                .get("remaining"),
            "retweeters": api.rate_limit_status().get("resources", {}).get("statuses",{}).
                get( "/statuses/retweets/:id", {}).get("remaining"),
            "id_user_lookup" :api.rate_limit_status().get("resources",{}).get("users",{}).get("/users/lookup",{})
                .get("remaining")
        }
        return switcher.get(end_point, 15)
    except Exception as e:
        logging.error("Failed to get the remaining limit of the end point " + str(end_point) + " due to error :"
                      + str(e))


def shuffle_key(tweepy_api=None,current_key_number="-1", end_point=None, user_keys=None , authentication="Oauth"):
    """
    :rtype: object
    :param tweepy_api: the current authorized api
    :param current_profile: the key set
    :param end_point: api endpoint
    :param user_keys: the file to store the limit and reset times
    :return: next set_of _keys
    """
    user_keys = update_blocked_key_credentials(api=tweepy_api, user_keys=user_keys,
                                               current_key_number=current_key_number, end_point=end_point)
    user_keys = reset_rate_limit_status(end_point=end_point, user_keys=user_keys)
    # files.output_current_limit(user_keys=user_keys)
    time_to_sleep, current_key_number = next_profile(end_point, user_keys)
    #raise the flag of the end_point of the current key number to 1
    user_keys.get(current_key_number).get("api_" + end_point)['flag'] = 1
    if time_to_sleep != 0:
        logging.warning("The minimum time for the next key to be free is " + str(time_to_sleep) + " seconds")
        time.sleep(time_to_sleep)
    else:
        logging.info("Free Key Available. Key no : " + str(current_key_number))
    tweepy_api = twitter_api_functions.get_api_authentication(authentication= authentication, number =
                                                                                            str(current_key_number))
    # DATABASE UPDATE - 2
    return tweepy_api, current_key_number, user_keys


def reset_rate_limit_status(end_point=None, user_keys=None):
    """
    function to free the blocked keys if the limit window has been crossed
    :param end_point: api_endpoint to be searched
    :param user_keys: the dictionary to keep track of the keys
    :return: dictionary
    """
    """if isinstance(end_point, list):
        for each_endpoint in end_point:
            user_keys = reset_end_point_limits(user_keys=user_keys, end_point=each_endpoint)
    else:
        user_keys = reset_end_point_limits(user_keys=user_keys, end_point = end_point)
    return"""
    user_keys = reset_end_point_limits(user_keys=user_keys, end_point=end_point)
    return user_keys


def reset_end_point_limits(user_keys=None, end_point=None):
    current_time = int(time.time())
    for key, value in user_keys.items():
        # check whether the limit window has been crossed
        if value.get("api_" + end_point).get("timestamp") < current_time:
            value.get("api_" + end_point)['limit'] = get_original_limit(endpoint=end_point)
            value.get("api_" + end_point)['flag'] = 0
            value.get("api_" + end_point)['timestamp'] = 0
    return user_keys



def update_blocked_key_credentials(user_keys={}, current_key_number=None, end_point=None, api=None):
    """
    the function is used to update the flags of the blocked key
    :param user_keys: user_key status                                                                                   #can connect to database once and pass the entire records or just connect to the database for the required key
    :param current_profile: key number
    :param end_point: tweepy_endpoint
    :param reset_time: epoch time for the 15 min window limit for the key
    :return: key limit database
    """
    try:
        reset_time = get_reset_time(end_point=end_point, api=api)
    except Exception as e:
        logging.error("Unable to fetch the reset time due to error :" + str(e))
        reset_time = int(time.time()) + (15 * 60)
    # the flag is dropped to -1 as the key is blocked until reset time
    user_keys.get(current_key_number).get("api_" + end_point,{})['flag'] = -1
    # set request limit to zero
    user_keys.get(current_key_number).get("api_" + end_point,{})['limit'] = 0
    # store the reset_time for the key
    user_keys.get(current_key_number).get("api_" + end_point,{})['timestamp'] = reset_time
    # DATABASE LIMIT STATUS UPDATED  - 1
    db_connect.output_current_limit(user_keys=user_keys)
    return user_keys


def get_original_limit(endpoint):
    switcher = {
        "search": 450,
        "user_timeline": 900,
        "followers": 15,
        "retweeters": 75,
        "id_user_lookup": 900

    }
    return switcher.get(endpoint)


def next_profile(end_point, user_keys):
    """
    :the function picks up the next set of keys based on availibility
    :param end_point: The endpoint for which the next key has to be searched
    :param user_keys: the dictionary to keep the track of user_keys
    :return: time in seconds, profile value for the next set of keys
    """
    minimum_time = 900
    for key, value in user_keys.items():
        flag = value.get("api_" + end_point).get("flag")
        if flag == 0:
            minimum_time = 0
            min_key = key
            break
        elif flag == -1 or flag == 1:
            reset_time = value.get("api_" + end_point).get("timestamp")
            time = wait_time(reset_time)
            if time < minimum_time:
                minimum_time = time
                min_key = key
    return minimum_time, min_key


def get_reset_time(end_point=None,api = None):
    """
    :param end_point: twitter api end point
    :param api: tweepy authentication
    :return: time in seconds for the endpoint of the key to be unblocked
    """
    try:
        switcher = {
            "user_timeline": api.rate_limit_status().get("resources",{}).get("statuses",{})
                .get( "/statuses/user_timeline", {}).get("reset"),
            "search": api.rate_limit_status().get("resources", {}).get("search", {}).get("/search/tweets", {})
                .get("reset"),
            "followers": api.rate_limit_status().get("resources", {}).get("followers", {}).get('/followers/list', {})
                .get("reset"),
            "retweeters": api.rate_limit_status().get("resources", {}).get("statuses", {})
                .get("/statuses/retweets/:id", {}).get("reset"),
            "id_user_lookup": api.rate_limit_status().get("resources", {}).get("users", {}).get("/users/lookup", {})
                .get("reset")

        }
        return switcher.get(end_point, 900)
    except Exception as e :
        logging.error("Failed to retrieve remining time from the api due to error :" + str(e))


def wait_time(reset_time):
    """
    :param reset_time: passes the reset time(in epoch time) of the endpoint
    :return: time to sleep in seconds.
    """
    reset_time = datetime.datetime.fromtimestamp(reset_time)
    current_time = int(time.time())
    current_time = datetime.datetime.fromtimestamp(current_time)
    waiting_time_object = dateutil.relativedelta.relativedelta(current_time, reset_time)
    seconds = abs(waiting_time_object.minutes * 60 + waiting_time_object.seconds) + 1
    return seconds


def search_query(word='', api=None, last_id_captured=None):
    result = []
    try:
        result = api.search(q=word, count=200, wait_on_rate_limit=True, max_id=last_id_captured,
                            tweet_mode="extended", wait_on_rate_limit_notify=True)
    except Exception as e:
        logging.error("Failed to get data sue to : " + str(e))
    try:
        last_id_captured = result[-1]._json.get("id")
    except Exception as e:
        logging.warning("No tweets found for the query : " + str(word))
        last_id_captured = None
    return result, last_id_captured