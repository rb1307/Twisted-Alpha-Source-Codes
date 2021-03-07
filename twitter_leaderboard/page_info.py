"""
@Romit Bhattacharyya
@ module is to store leader page information  to database : status count , followers_count
Clients.Info is run once/day to store the latest changes to the page.
Input  = Tamil Nadu LeaderBoard Source file
Output = Tamil Nadu Leader Page Details
"""
import logging
from Files import i_o
from api_keys import twitter_api_functions
from common_functions import strip_screen_name, get_handle_details, client_info

HANDLE_COLUMN='twitter_link'
PARTY_Column = 'Party Name'


class ClientPageDetails:
    def __init__(self, **kwargs):
        self.parameters = {
            'Collection Name': 'Profile Details'
        }
        self.parameters.update(kwargs)
        # RETRIEVAL OF KEY END POINT STATUS FROM THE DATABASE.
        self.user_keys = i_o.get_key_status()
        """self.upload_to_db = i_o.ConnectoMongo(storage_path=self.parameters.get("credentials_path"),
                                              credential_file=self.parameters.get("credentials_file"),
                                              client_db_name=self.parameters.get("client_db_name"),
                                              collection_name=self.parameters.get("Collection Name"))"""
        print ()
        logging.info("Client Name: " + self.parameters.get("client", "") +"\n\tPurchased Version: "
                     + self.parameters.get("product_version", ""))

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
            leader_details = {'Name': handle, 'Details': user_info}
            page_details.append(leader_details)
        # self.upload_to_db.upload_in_bulk(record_list=page_details)
        return page_details


def getsourceobject(**kwargs):
    params={}
    params.update(kwargs)
    user_obj=ClientPageDetails(**params)
    resp = user_obj.get_page_details()
    return resp