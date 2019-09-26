import pytest
from moto import mock_s3

from m3d.hadoop.emr.emr_exceptions import M3DAWSAPIException
from m3d.util.s3_util import S3Util
from m3d.util.aws_credentials import AWSCredentials

from test.core.unit_test_base import UnitTestBase
from test.test_util.boto3_util import Boto3Util
from test.test_util.concurrent_executor import ConcurrentExecutor


class TestS3Util(UnitTestBase):

    @pytest.mark.emr
    @mock_s3
    def test_upload_object(self):
        test_bucket_name = "test_bucket"
        test_key = "test_s3_util/tconx-bdp-emr_test-dev-bi_test101.json"
        file_name = "test/resources/test_s3_util/tconx-bdp-emr_test-dev-bi_test101.json"

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_bucket_name)

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.upload_object(file_name, "s3://" + test_bucket_name + "/" + test_key)

        s3_objects = list(s3_resource.Bucket(test_bucket_name).objects.all())
        assert len(s3_objects) == 1
        assert s3_objects[0].key == test_key

    @pytest.mark.emr
    @mock_s3
    def test_delete_object(self):
        test_bucket_name = "test_bucket"
        test_key = "test_dir/test_key"

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_bucket_name)
        s3_resource.Bucket(test_bucket_name).put_object(Key=test_key, Body="")

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.delete_object("s3://" + test_bucket_name + "/" + test_key)

        remaining_objects = list(s3_resource.Bucket(test_bucket_name).objects.all())
        assert len(remaining_objects) == 0

    @pytest.mark.emr
    @mock_s3
    def test_delete_objects(self):
        test_bucket_name = "test_bucket"
        test_prefix = "test_dir"
        test_keys = [
            "test_key1",
            "{}/test_key2".format(test_prefix),
            "{}/test_key3".format(test_prefix),
            "{}/test_key4".format(test_prefix)
        ]

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_bucket_name)
        for key in test_keys:
            s3_resource.Bucket(test_bucket_name).put_object(Key=key, Body="")

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.delete_objects("s3://" + test_bucket_name + "/" + test_prefix)

        remaining_objects = list(s3_resource.Bucket(test_bucket_name).objects.all())
        assert len(remaining_objects) == 1
        assert remaining_objects[0].key == test_keys[0]

    @pytest.mark.emr
    @mock_s3
    def test_list_objects_in_bucket(self):
        test_bucket_name = "test_bucket"
        test_prefix = "test_dir"
        test_keys = [
            "test_key1",
            "{}/test_key2".format(test_prefix),
            "{}/test_key3".format(test_prefix),
            "{}/test_key4".format(test_prefix)
        ]
        test_resources = ["s3://{}/".format(test_bucket_name) + key for key in test_keys]

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_bucket_name)
        for key in test_keys:
            s3_resource.Bucket(test_bucket_name).put_object(Key=key, Body="")

        s3_util = S3Util(AWSCredentials("", ""))
        keys = s3_util.list_objects("s3://" + test_bucket_name + "/" + test_prefix)

        assert keys == test_resources[1:4]

    @pytest.mark.emr
    @mock_s3
    def test_move_object(self):
        test_src_bucket_name = "test_src_bucket"
        test_destination_bucket_name = "test_destination_bucket"
        test_src_key = "test_src_key"
        test_destination_key = "test_destination_key"
        test_content = "aaa1"

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_src_bucket_name)
        s3_resource.create_bucket(Bucket=test_destination_bucket_name)
        s3_resource.Bucket(test_src_bucket_name).put_object(Key=test_src_key, Body=test_content)

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.move_object(
            ("s3://" + test_src_bucket_name + "/" + test_src_key),
            ("s3://" + test_destination_bucket_name + "/" + test_destination_key)
        )

        destination_objects = list(s3_resource.Bucket(test_destination_bucket_name).objects.all())
        assert len(destination_objects) == 1
        assert destination_objects[0].key == test_destination_key

        src_objects = list(s3_resource.Bucket(test_src_bucket_name).objects.all())
        assert len(src_objects) == 0

    @pytest.mark.emr
    @mock_s3
    def test_move_objects(self):
        test_src_bucket_name = "test_src_bucket"
        test_destination_bucket_name = "test_destination_bucket"
        test_src_prefix = "test_src_dir"
        test_destination_prefix = "test_destination_dir"
        test_src_keys = [
            "test_key1",
            "{}/test_key2".format(test_src_prefix),
            "{}/test_key3".format(test_src_prefix),
            "{}/test_key4".format(test_src_prefix)
        ]
        test_destination_keys = [
            "{}/test_key2".format(test_destination_prefix),
            "{}/test_key3".format(test_destination_prefix),
            "{}/test_key4".format(test_destination_prefix)
        ]

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=test_src_bucket_name)
        s3_resource.create_bucket(Bucket=test_destination_bucket_name)

        for key in test_src_keys:
            s3_resource.Bucket(test_src_bucket_name).put_object(Key=key, Body="")

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.move_objects(
            ("s3://" + test_src_bucket_name + "/" + test_src_prefix),
            ("s3://" + test_destination_bucket_name + "/" + test_destination_prefix)
        )

        src_objects = list(s3_resource.Bucket(test_src_bucket_name).objects.all())
        assert len(src_objects) == 1
        assert src_objects[0].key == test_src_keys[0]

        destination_objects = s3_resource.Bucket(test_destination_bucket_name).objects.all()
        assert sorted(map(lambda x: x.key, destination_objects)) == test_destination_keys

    @pytest.mark.emr
    @mock_s3
    def test_wait_for_file_availability(self):
        bucket = "cur_bucket"
        key = "stdout.txt"
        data = "no output"

        s3_full_path = "s3://{}/{}".format(bucket, key)

        s3_resource = Boto3Util.create_s3_resource()
        s3_resource.create_bucket(Bucket=bucket)

        def create_file():
            s3_resource.Bucket(bucket).put_object(Key=key, Body=data)

        s3_util = S3Util(AWSCredentials("", ""))

        polling_interval = 0.02
        timeout = 0.5

        with ConcurrentExecutor(create_file, 0.2):
            s3_util.wait_for_file_availability(
                s3_full_path,
                polling_interval,
                timeout
            )

        s3_util.delete_object(s3_full_path)

        err_msg = "File {} failed to be available after {} seconds.".format(s3_full_path, timeout)

        with pytest.raises(M3DAWSAPIException, match=err_msg):
            s3_util.wait_for_file_availability(
                s3_full_path,
                polling_interval,
                timeout
            )

    @pytest.mark.emr
    @mock_s3
    def test_normalize_s3_path(self):
        assert "s3://buc/s/o/me/path.txt" == S3Util.normalize_s3_path("s3://buc/s/o/me/path.txt")
        assert "s3://buc/s/o/me/path.txt" == S3Util.normalize_s3_path("s3a://buc/s/o/me/path.txt")
        assert "s3://buc/s/o/me/path.txt" == S3Util.normalize_s3_path("s3n://buc/s/o/me/path.txt")

    @pytest.mark.emr
    @mock_s3
    def test_is_s3_path(self):
        assert S3Util.is_s3_path("s3a://buc/s/o/me/path.txt")
        assert S3Util.is_s3_path("s3n://buc/s/o/me/path.txt")
        assert S3Util.is_s3_path("s3://buc/s/o/me/path.txt")
        assert S3Util.is_s3_path("s3://bucket/")

        assert not S3Util.is_s3_path("hdfs://sandbox.com:8020/user/it1/.staging")

        assert not S3Util.is_s3_path("/local/path.txt")
        assert not S3Util.is_s3_path("relative/path.txt")

        assert not S3Util.is_s3_path(["s3://bucket/"])
        assert not S3Util.is_s3_path(3)
        assert not S3Util.is_s3_path({})
