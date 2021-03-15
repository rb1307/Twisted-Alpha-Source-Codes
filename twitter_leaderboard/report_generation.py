import logging
import twitter_leaderboard
import pandas as pd
from common_functions import get_todays_date
from sklearn import preprocessing


class TwitterReport:
    def __init__(self, **kwargs):
        self.params = {
            'date': str(get_todays_date())
        }
        self.params.update(kwargs)

    def generate_final_report(self):
        response_data = self.params.get("response_data")
        if isinstance(response_data, dict):
            createtweetsreport(response_data=response_data)
            leader_board = []
            media_space_board = []
            for leader_name, leader_data in response_data.items():

                total_tweets = calculate_no_of_tweets(data=leader_data.get("tweets"))
                leader_details = {'Leader handle': leader_name, 'Tweets for the day': total_tweets,
                                  'Reach': leader_data.get("total_reach"),
                                  'Engagement Score': leader_data.get('total_engagement'),
                                  'Media Score': leader_data.get('media_space_score', 1),
                                  'Party': leader_data.get("Party"),
                                  'Leader Name': leader_data.get("Name")}
                leader_board.append(leader_details)
                media_space_details = {'Name': leader_data.get("Name")}
                media_space_details.update(leader_data.get("media_details"))
                media_space_board.append(media_space_details)
            final_leaderboard = pd.DataFrame(leader_board)
            media_space_board = pd.DataFrame(media_space_board)
            media_space_board = create_media_space_records(df=media_space_board)
            final_leaderboard = post_procesing_leaderboard(df=final_leaderboard)
            final_leaderboard.to_excel("leaderBoard.xlsx", index=0)
            media_space_board.to_excel('MediaBoard.xlsx')
        return 0


def createtweetsreport(response_data={}):
    all_tweet_data = []
    for leader_name, leader_data in response_data.items():
        tweets_data = leader_data.get("tweets")
        for tweet_id, tweet_details in tweets_data.items():
            details = {'Leader name': leader_data.get('Name', None), 'Retweet Count': tweet_details.get('retweet'),
                       'Applause Rate': tweet_details.get('applause_rate'), 'Tweet Type': tweet_details.get('type'),
                       'Tweet Engagement(per 1K followers)': tweet_details.get("engagement"),
                       'Business Objective': tweet_details.get('BO'),
                       'Party': leader_data.get('Party')}
            tweet_link = generate_tweet_link(twitter_handle=leader_name, tweet_id=tweet_id)
            details['Link'] = tweet_link
            all_tweet_data.append(details)
    # UPlOAD TO DB COLLECTION - insert_one ({'today's date : all_tweet_data})
    # tweet_analysis_data = pd.DataFrame(all_leaders_data)
    # final_data.to("final.xlsx", index=0)
    return 0


def generate_tweet_link(twitter_handle=None, tweet_id=None):
    tweet_link = "https://twitter.com/" + str(twitter_handle) + "/status/" + str(tweet_id)
    return tweet_link


def create_media_space_records(df=None):
    df = df.set_index('Name')
    df['Total'] = df.sum(axis=1)
    res_df = df.div(df.sum(axis=1), axis=0).round(2)
    return res_df


def post_procesing_leaderboard(df=None):
    df['score'] = 0.1 * df['Tweets for the day'] + 0.5 * df['Engagement Score'] \
                  + 0.3 * df['Media Score']
    df = normalize_data_column(df=df, cols=['score'])
    df['Todays Rank'] = df['score'].rank(ascending=False)
    df = df.sort_values(by=['score'], ascending=False)
    df=df.round(2)
    return df


def normalize_data_column(df=None, cols=[]):
    for col in cols:
        x = df[col].values.reshape(1, -1)
        scaler = preprocessing.MinMaxScaler()
        df[col] = scaler.fit_transform(df[[col]])
    return df


def calculate_no_of_tweets(data={}):
    number_of_tweets_posted_that_day = len(list(data.keys()))
    return number_of_tweets_posted_that_day
