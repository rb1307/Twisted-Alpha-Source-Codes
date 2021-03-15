"""
@Romit Bhattacharyya
@ module is to store leader page information  to database : status count , followers_count
Clients.Info is run once/day to store the latest changes to the page.
Input  = Tamil Nadu LeaderBoard Source file
Output = Tamil Nadu Leader Page Details
"""
import logging
from Files import db_connect
from api_keys import twitter_api_functions
from common_functions import strip_screen_name, get_handle_details, client_info

HANDLE_COLUMN='twitter_link'
PARTY_Column = 'Party Name'
COLLECTION_NAME = 'Page Details'


class ClientPageDetails:
    def __init__(self, **kwargs):
        self.parameters = {
            'Collection Name': 'Profile Details'
        }
        self.parameters.update(kwargs)
        # RETRIEVAL OF KEY END POINT STATUS FROM THE DATABASE.
        self.user_keys = db_connect.get_key_status()
        logging.info("Client Name: " + self.parameters.get("client", "") +"\n\tPurchased Version: "
                     + self.parameters.get("product_version", ""))
        self.output_obj = self.parameters.get("db_object")

    def gettwitterhandles(self):
        input_data = self.parameters.get("S3_input_data")
        # handle column has to have the name of the column
        twitter_handles = strip_screen_name(data=input_data, handle_column=HANDLE_COLUMN, party_column=PARTY_Column)
        return twitter_handles

    def get_page_details(self):
        tweepy_api = twitter_api_functions.get_api_authentication(number=self.parameters.get("starter_key_number"))
        page_details = []
        twitter_handles = self.gettwitterhandles()
        for handle, party_name in twitter_handles.items():
            user_response = get_handle_details(api=tweepy_api, screen_name=handle)
            user_info = client_info(json=user_response)
            user_info['Party'] = party_name
            leader_details = {'Leader handle': handle, 'Details': user_info}
            page_details.append(leader_details)
        if self.parameters.get("test"):
            try:
                self.output_obj.upload_in_bulk(collection=COLLECTION_NAME, record_list=page_details)
                # self.output_obj.close_connection()
            except Exception as e:
                # self.output_obj.close_connection()
                print(e)

        else:
            # print to a local file locally
            pass
        return page_details


def getsourceobject(**kwargs):
    params={}
    params.update(kwargs)
    user_obj=ClientPageDetails(**params)
    resp = user_obj.get_page_details()
    return resp