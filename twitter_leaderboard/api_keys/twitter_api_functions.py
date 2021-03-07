import logging
import json
import tweepy
from Errors import TwitterAPIKeysInputError, ApiAuthenticationError

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

twitter_cred ={}


with open('twitter_credentials.json', 'w') as secret_info:
    json.dump(twitter_cred, secret_info, indent=4, sort_keys=True)


def input_keys():
    """
    function to read stored api keys                                                                                    #store in a database
    :return: json object
    """
    with open("/home/hp/PINGALA ANALYTICS/SM Management Tool/pingala_v2/api_keys/api_keys.json") as f:
        data = json.load(f)
    return data

# HAVE TO DISSOCIATE THE FUNCTION


def get_api_authentication(authentication="Oauth", number=None):
    """
    function to authenticate twitter api with the desired api key
    :param authentication: type of authentication
    :param number:
    :return:
    """
    # input keys should be read directly from the files

    try:
        data = input_keys()         # connect to database directly can remove this function
    except Exception:
        raise TwitterAPIKeysInputError
    number = int(number)

    consumer_key = data[number].get("consumer_key")
    consumer_secret = data[number].get("consumer_secret")
    access_key = data[number].get("access_key")
    access_secret = data[number].get("access_secret")
    # check authentication type
    if authentication == "Oauth":
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_key, access_secret)
        logging.info("Key Number used : "+ str(number) + ". Authentication type : " + str(authentication) +
                     ". Tweepy Authentication successful.")
    elif authentication == "Appouth":
        auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
        logging.info("Key Number used : " + str(number) + ".Authentication type : " + str(authentication) +
                     ". Tweepy Authentication successful.")
    else:
        raise ApiAuthenticationError
    api = tweepy.API(auth)
    return api

