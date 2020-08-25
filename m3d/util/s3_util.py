import fnmatch
import logging
import os
from gzip import GzipFile
from io import BytesIO
from urllib.parse import urlparse
import boto3
import botocore

from m3d.hadoop.emr.emr_exceptions import M3DAWSAPIException


class S3Util(object):

    def __init__(self, aws_credentials):
        self.s3_resource = boto3.resource(
            "s3",
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key
        )

    def upload_object(self, file_path, s3_path):
        """
        Upload the object file_path to S3 as s3_path
        :param file_path: path to a local file which is going to be uploaded
        :param s3_path: S3 path to upload the local file to, e.g.: s3://bucket/path/to/file
        """
        logging.info("Uploading file to \"{}\" to S3".format(s3_path))
        bucket_name, key = S3Util.get_bucket_and_key(s3_path)
        self.s3_resource.Bucket(bucket_name).upload_file(file_path, key)

    def upload_child_objects(self, local_dir_path, s3_dir_path, recursive=False, fn_pattern=None):
        """
        Upload files from given directory to S3 bucket under given path.

        If recursive is True, then the function will recursively check subdirectories as well.
        If fn_pattern is specified, then only files matching the pattern will be uploaded.

        :param local_dir_path: local directory whose content will be uploaded.
        :param s3_dir_path: root directory in S3 under which objects will be uploaded
        :param recursive:
        :param fn_pattern: Unix shell-style wildcard pattern. If specified, then only files matching
                           the pattern will be uploaded.
        """
        child_objects = [os.path.join(local_dir_path, f) for f in os.listdir(local_dir_path)]
        child_files = [f for f in child_objects if os.path.isfile(f)]
        child_dirs = [f for f in child_objects if os.path.isdir(f)]

        for child_file in child_files:
            if not fn_pattern or fnmatch.fnmatch(child_file, fn_pattern):
                s3_object_path = os.path.join(s3_dir_path, os.path.basename(child_file))
                logging.debug("Uploading \"{}\" to \"{}\"".format(child_file, s3_object_path))
                self.upload_object(child_file, s3_object_path)

        if recursive:
            for child_dir_local in child_dirs:
                child_dir_s3 = os.path.join(s3_dir_path, os.path.basename(child_dir_local))
                self.upload_child_objects(child_dir_local, child_dir_s3, recursive, fn_pattern)

    def download_object(self, s3_path, local_path):
        """
        Download the object file_path to S3 as s3_path
        :param s3_path: S3 path where file is downloaded from
        :param local_path: path to a local path to which it is downloaded
        """
        # creating local directory if necessary
        local_directory = os.path.dirname(local_path)
        if not os.path.exists(local_directory):
            logging.debug("Creating directory \"{}\" in local filesystem".format(local_directory))
            os.makedirs(local_directory)

        # downloading file from S3
        logging.info("Downloading file from S3 \"{}\" to \"{}\"".format(s3_path, local_path))
        bucket_name, key = S3Util.get_bucket_and_key(s3_path)
        self.s3_resource.Bucket(bucket_name).download_file(key, local_path)

    def delete_object(self, s3_path):
        """
        Delete the S3 object denoted by s3_path
        :param s3_path: path to a file on S3 which is going to be deleted, e.g.: s3://bucket/path/to/file
        """
        logging.info("Deleting \"{}\" file from S3".format(s3_path))
        bucket_name, key = S3Util.get_bucket_and_key(s3_path)
        self.s3_resource.ObjectSummary(bucket_name, key).delete()

    def delete_object_if_exists(self, s3_path):
        if self.object_exists(s3_path):
            self.delete_object(s3_path)

    def delete_objects(self, s3_prefix_path):
        """
        Delete all objects in the bucket denoted by s3_path_prefix
        :param s3_prefix_path: S3 path which is used as a prefix for filtering objects
        """
        bucket_name, prefix = S3Util.get_bucket_and_key(s3_prefix_path)
        bucket = self.s3_resource.Bucket(bucket_name)
        for obj in bucket.objects.filter(Prefix=prefix):
            obj.delete()

    def delete_child_objects(self, s3_prefix_path):
        logging.info("Deleting files under \"{}\".".format(s3_prefix_path))
        s3_prefix_path = os.path.join(s3_prefix_path, "")
        self.delete_objects(s3_prefix_path)

    def list_objects(self, s3_prefix_path):
        """
        Lists objects in a S3 bucket which paths start with the specified s3_prefix_path
        :param s3_prefix_path: S3 path which is used as a prefix for filtering objects
        :return: a list of the paths to the objects in the particular bucket
        """
        bucket_name, prefix = S3Util.get_bucket_and_key(s3_prefix_path)
        bucket = self.s3_resource.Bucket(bucket_name)
        return ["s3://" + bucket_name + "/" + key.key for key in bucket.objects.filter(Prefix=prefix)]

    def list_keys(self, s3_prefix_path, delimiter='/'):
        """
        Lists keys in a S3 bucket which paths start with the specified s3_prefix_path
        The listing is not recursive, as a limiter is specified
        :param s3_prefix_path: S3 path which is used as a prefix for filtering objects
        :param delimiter delimiter to avoid recursive listing
        :return: a list of the keys in the particular bucket
        """
        bucket_name, prefix = S3Util.get_bucket_and_key(s3_prefix_path)
        bucket = self.s3_resource.Bucket(bucket_name)
        result = bucket.meta.client.list_objects(Bucket=bucket_name,
                                                 Prefix=prefix,
                                                 Delimiter=delimiter)
        if result.get('CommonPrefixes') is not None:
            return [o.get('Prefix') for o in result.get('CommonPrefixes')]

    def list_child_objects(self, s3_prefix_path):
        s3_prefix_path = os.path.join(s3_prefix_path, "")
        return self.list_objects(s3_prefix_path)

    def list_child_keys(self, s3_prefix_path, delimiter='/'):
        s3_prefix_path = os.path.join(s3_prefix_path, "")
        return self.list_keys(s3_prefix_path, delimiter)

    def move_object(self, src_s3_path, destination_s3_path):
        """
        Moves the S3 object denoted by src_s3_path to/as destination_s3_path
        :param src_s3_path: S3 path to copy object from, e.g.: s3://bucket/source/path/to/file
        :param destination_s3_path: S3 path to copy object to, e.g.: s3://bucket/destination/path/to/file
        """
        src_bucket_name, src_key = S3Util.get_bucket_and_key(src_s3_path)
        destination_bucket_name, destination_key = S3Util.get_bucket_and_key(destination_s3_path)
        obj = self.s3_resource.ObjectSummary(src_bucket_name, src_key)
        destination_bucket = self.s3_resource.Bucket(destination_bucket_name)
        destination_bucket.copy(CopySource=self._object_summary_to_copy_source(obj), Key=destination_key)
        obj.delete()

    def move_objects(self, src_s3_prefix_path, destination_s3_prefix_path):
        """
        Moves the S3 objects in src_s3_prefix_path to destination_s3_prefix_path
        :param src_s3_prefix_path: S3 path which is used as a prefix for filtering source objects
        :param destination_s3_prefix_path: S3 path which is used as a prefix for building destination paths
        """
        src_bucket_name, src_prefix = S3Util.get_bucket_and_key(src_s3_prefix_path)
        destination_bucket_name, destination_prefix = S3Util.get_bucket_and_key(destination_s3_prefix_path)

        src_bucket = self.s3_resource.Bucket(src_bucket_name)
        destination_bucket = self.s3_resource.Bucket(destination_bucket_name)

        for obj in src_bucket.objects.filter(Prefix=src_prefix):
            source_obj = self._object_summary_to_copy_source(obj)

            # replace the prefix
            new_key = obj.key.replace(src_prefix, destination_prefix)
            destination_bucket.copy(CopySource=source_obj, Key=new_key)
            obj.delete()

    def move_child_objects(self, src_s3_prefix_path, destination_s3_prefix_path):
        src_s3_prefix_path = os.path.join(src_s3_prefix_path, "")
        destination_s3_prefix_path = os.path.join(destination_s3_prefix_path, "")
        self.move_objects(src_s3_prefix_path, destination_s3_prefix_path)

    def object_exists(self, s3_path):
        """
        Check if object denoted by s3_path exist
        :param s3_path: path to an object on S3
        """
        s3_objects = self.list_objects(s3_path)
        return s3_path in s3_objects

    def read_gzip_file_content(self, s3_path):
        """
        Read the content of a gzip file
        :param s3_path: path to an object on S3
        :return: content of the file
        """
        s3_object = self._get_s3_object(s3_path)

        bytestream = BytesIO(s3_object.get()['Body'].read())

        gzip_file = GzipFile(mode="rb", fileobj=bytestream)
        output = gzip_file.read().decode("utf-8")

        return output

    @staticmethod
    def get_bucket_and_key(object_key):
        """
        Parse an S3 location of a resource (e.g. s3://adidas-test/test-folder/test-file.json)
        :param object_key: whole path of the resource included the bucket
        :return: bucket_name, object_key
        """
        parsed_object = urlparse(object_key)
        return parsed_object.netloc, parsed_object.path.lstrip('/')

    @staticmethod
    def _object_summary_to_copy_source(s3_object):
        """
        :param s3_object: a resource representing an Amazon Simple Storage Service (S3) Object
        :return: a dictionary containing the bucket_name and the key
        """
        return {
            "Bucket": s3_object.bucket_name,
            "Key": s3_object.key
        }

    def _get_s3_object(self, s3_path):
        """
        Return a boto s3 object from an s3 path
        :param s3_path: location of the object in s3
        :return: s3 object
        """
        bucket_name, key = S3Util.get_bucket_and_key(s3_path)
        return self.s3_resource.Object(bucket_name, key)

    def wait_for_file_availability(self, s3_file_location, polling_interval_seconds, timeout_seconds):
        """
        Continually poll for output or error file availability. Wait for max SLA from AWS.
        In this stage the client has already waited for step completion
        :param s3_file_location: file location in s3
        :param polling_interval_seconds: polling interval time in seconds
        :param timeout_seconds: timeout interval in seconds
        """
        bucket_name, key = S3Util.get_bucket_and_key(s3_file_location)
        s3_obj = self.s3_resource.Object(bucket_name, key)

        max_attempts = 1 + timeout_seconds / polling_interval_seconds

        try:
            s3_obj.wait_until_exists(
                WaiterConfig={
                    'Delay': polling_interval_seconds,
                    'MaxAttempts': max_attempts
                }
            )
        except botocore.exceptions.WaiterError as e:
            if "Max attempts exceeded" in str(e):
                raise M3DAWSAPIException("File {} failed to be available after {} seconds."
                                         .format(s3_file_location, timeout_seconds))
            else:
                raise

    @staticmethod
    def normalize_s3_path(s3_path):
        """
        Normalize S3 path by changing it to 's3://' protocol.
        Should be used for converting from 's3a://' and 's3n://' protocols.

        :param s3_path: full S3 path that should be normalized
        :return: normalized full S3 path with 's3://' protocol
        """
        bucket_name, key_path = S3Util.get_bucket_and_key(s3_path)
        normalized_s3_path = "s3://{}/{}".format(bucket_name, key_path)
        return normalized_s3_path

    @staticmethod
    def is_s3_path(obj):
        """
        Check if passed object is a string which represents a path in S3.

        :param obj: an object that we want to check
        :return: True, if obj is a string representing S3 path. False otherwise.
        """
        if isinstance(obj, str):
            if obj.startswith("s3://") or obj.startswith("s3a://") or obj.startswith("s3n://"):
                return True

        return False
