twitterapikeysinputerror_msg = "ERROR in loading list of keys from file : api_keys.json."
userdetailerror_msg ="Api failed to retrieve any  page information about the user."
apiauthenticationerror = 'API Authentication Error. Type of Authentication is outside scope. '
stringtodateerror = 'String to Date Conversion Error.Check input type'
inputdataerror ='Failed to input data. Check file'


class InputDataError(Exception):
    def __init__(self, message=inputdataerror, file_name=None):
        self.message = message
        self.filename = file_name
        super().__init__(self.message + " : " + self.filename)


class TwitterAPIKeysInputError(Exception):
    def __init__(self, message =twitterapikeysinputerror_msg):
        self.message = message
        super().__init__(self.message)


class UserDetailError(Exception):
    def __init__(self,user=None,message =userdetailerror_msg):
        self.user = user
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        return f'{self.user} ::{ self.message}'


class ApiAuthenticationError(Exception):
    def __init__(self , message =apiauthenticationerror):
        self.message=message
        super().__init__(self.message)


class StringToDateError(Exception):
    def __init__(self , message =stringtodateerror):
        self.message=message
        super().__init__(self.message)