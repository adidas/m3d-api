import boto3

from m3d.util.aws_credentials import AWSCredentials


class Boto3Util(object):

    @staticmethod
    def create_s3_resource(
            aws_credentials=None
    ):
        """
        Initialize and return boto3 resource for S3.
        :param aws_credentials: AWS credentials. Empty values will be used if it is None.
        :return: initialized boto3 resource object for S3
        """
        if not aws_credentials:
            aws_credentials = AWSCredentials("", "")

        s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            region_name='us-east-1'
        )

        return s3_resource

    @staticmethod
    def create_emr_client(
            aws_region,
            aws_credentials=None
    ):
        """
        Initialize and return boto3 client for EMR.
        :param aws_region: AWS region
        :param aws_credentials: AWS credentials. Empty values will be used if it is None.
        :return: initialized boto3 client object for EMR
        """
        if not aws_credentials:
            aws_credentials = AWSCredentials("", "")

        emr_client = boto3.client(
            'emr',
            region_name=aws_region,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key
        )

        return emr_client
