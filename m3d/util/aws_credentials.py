import json


class AWSCredentials(object):
    """
    Object holding AWS credentials.
    """

    class Keys(object):
        ACCESS_KEY_ID = "access_key_id"
        SECRET_ACCESS_KEY = "secret_access_key"

    def __init__(self, access_key_id, secret_access_key):
        """
        Create AWSCredentials object via directly passing access_key_id and secret_access_key.

        :param access_key_id: AWS access key id
        :param secret_access_key: AWS secret access key
        """
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    def __eq__(self, other):
        """ Equality comparison operator """
        if isinstance(other, AWSCredentials):
            return self.access_key_id == other.access_key_id and self.secret_access_key == other.secret_access_key
        return False

    def __ne__(self, other):
        """ Inequality comparison operator """
        return not self == other

    @classmethod
    def from_file(cls, aws_credentials_file_path):
        """
        Create AWSCredentials object via reading credentials from a file.

        :param aws_credentials_file_path: path to file containing AWS credentials
        """
        with open(aws_credentials_file_path) as credentials_file:
            aws_credentials = json.load(credentials_file)

        access_key_id = aws_credentials[AWSCredentials.Keys.ACCESS_KEY_ID]
        secret_access_key = aws_credentials[AWSCredentials.Keys.SECRET_ACCESS_KEY]

        return cls(access_key_id, secret_access_key)
