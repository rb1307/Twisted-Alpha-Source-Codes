import pandas as pd
import os
from datetime import *
import json
from common_functions import get_todays_date
from Errors import InputDataError
from pymongo import MongoClient
import logging
import csv

api_path ='/home/hp/PINGALA ANALYTICS/SM Management Tool/pingala_v2/api_keys/'

ALL_END_POINTS = ['search', 'followers', 'user_timeline', 'retweeters', 'user_lookup']


def get_key_status():
    """
    function reads from the database/file = the current statuses of the api ends of all the keys
    :return: the current status of each endpoint of each keys
    """
    with open(api_path+"api_limit_flags.json") as f:
        user_keys = json.load(f)
    # user_keys = crawler_functions.reset_rate_limit_status(end_point=ALL_END_POINTS,user_keys=user_keys )
    return user_keys


def output_current_limit (user_keys):
    """
    :param user_keys:
    :return:
    """
    with open('api_limit_flags.json', 'w') as outfile:
        json.dump(user_keys, outfile)


def getmongodbcredentials(storage_path=None, cred_file=None):
    input_obj = Input(storage=storage_path, filename=cred_file)
    credentials = input_obj.input_from_json()
    return credentials


class Input:
    def __init__(self, client_name="Demo", storage=None, filename=None):
        self.client_name=client_name
        self.filename=storage + filename

    def make_current_directory(self):
        original_path = os.getcwd()
        base_directory = (original_path + "/" + str(self.client_name) + "_" + str(date.today()))
        return base_directory

    def input_from_csv(self):
        data_xls = pd.read_csv(self.filename, index_col=False, encoding="utf-8")
        return data_xls

    def input_from_excel(self):
        data_xls = pd.read_excel(self.filename, index_col=False)
        return data_xls

    def input_from_json(self):
        try:
            with open(self.filename) as f:
                data_json = json.load(f)
            return data_json
        except Exception:
            raise InputDataError(file_name=self.filename)


class Output:
    def __init__(self, **kwargs):
        self.params = {
            'date': str(get_todays_date())
        }
        self.params.update(kwargs)

    def clean_data(self):
        pass

    def store_data_in_excel(self):
        pass


class ConnectoMongo:
    def __init__(self, storage_path=None, credential_file=None, client_db_name=None):
        credentials = getmongodbcredentials(storage_path=storage_path, cred_file=credential_file)
        self.cluster_username = credentials.get("username")
        self.cluster_password = credentials.get("password")
        self.db_name = client_db_name
        # self.collection_name = collection_name

    def initialize_cluster(self):
        cluster = MongoClient("mongodb+srv://" + self.cluster_username + ":" + self.cluster_password +
                           "@cluster0.hjuda.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
        return cluster

    def connect_to_collection(self, collection_name=None):
        cluster = self.initialize_cluster()
        collection = cluster[self.db_name][collection_name]
        return collection

    def upload_in_bulk(self, record_list=[]):
        self.connect_to_collection().insert_many(record_list)
        return 0


