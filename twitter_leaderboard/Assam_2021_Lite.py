"""
@Romit Bhattacharyya
@Client = Assam CM
@Party = BJP
@State = Assam

the run module  of Assam Election 2021

"""

import page_info
from Files import i_o
import configargparse
import twitter_leaderboard

MEDIA_SCORE = False


class Assam2021Lite:
    def __init__(self, ):
        parser = configargparse.ArgParser(default_config_files=['/home/hp/PINGALA ANALYTICS/SM Management Tool/'
                                                                'pingala_v2/Config_Files/Assam_2021_Lite_configs.ini'])
        parser.add_argument('--storage_path', required=True, help=' Storage Folder for all Client Data')
        parser.add_argument('--input_data', required=True, help='Input Data(list of Pages/Profiles to be tracked')
        parser.add_argument('--input_file_format', required=True, help='Input File Format')

        parser.add_argument('--credentials_file', required=True, help='credentials for connecting to db')
        parser.add_argument('--credentials_path', required=True, help=' Storage Folder for the Credentials')
        parser.add_argument('--client_db_name', required=True, help='Database Name')

        parser.add_argument('--product_version', required=True, help='The type of product Version')
        parser.add_argument('--test', dest='test', action='store_true', help='run test file.apply ball test conditions')
        parser.add_argument('--no-test', dest='test', action='store_false', help='Apply all test conditions')

        parser.add_argument('--mediaspace', dest='test', action='store_true',
                            help='run test file.apply ball test conditions')
        parser.add_argument('--no-mediaspace', dest='test', action='store_false', help='Apply all test conditions')

        parser.add_argument('--client', required=True, help='The Client name')
        parser.add_argument('--starter_key_number', required=True,
                            help='The key number to be used start hitting the api.')
        parser.add_argument('--crawl_day', required=True, help='Number of days to be sutracted for today to determine '
                                                               'the window period')
        parser.add_argument('--crawl_hour', required=True, help='Hour cut off. e.g 8 pm to 8pm , crawl_hour=20')
        parser.add_argument('--crawl_minutes', required=True, help='Hour cut off. e.g 8 pm to 8pm , crawl_hour=20')
        self.params = parser.parse_args()
        input_obj = i_o.Input(client_name=self.params.client, storage=self.params.storage_path,
                              filename=self.params.input_data)
        self.input_data = input_obj.input_from_excel()

    def base_args(self):
        arguments = vars(self.params)
        arguments.update({"S3_input_data": self.input_data})
        return arguments

    def getpageinformation(self):
        values_passed = self.base_args()
        resp = page_info.getsourceobject(**values_passed)
        return resp

    def getleaderboardresults(self):
        if self.params.test:
            # passed_args.update({'page_info': self.getpageinformation()})
            obju = twitter_leaderboard.LeaderBoard(page_info=self.getpageinformation(), **self.base_args())
            obju.generate_report()
        else:
            # pick up data from local file
            pass
        return 0


obj=Assam2021Lite()
obj.getleaderboardresults()