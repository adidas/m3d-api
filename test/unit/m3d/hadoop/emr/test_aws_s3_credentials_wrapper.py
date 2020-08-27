import pytest
import re

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.util.aws_credentials import AWSCredentials
from m3d.util.s3_util import S3Util
from m3d.hadoop.emr.aws_s3_credentials_wrapper import AWSS3CredentialsWrapper


class Invocation(object):

    def __init__(self, aws_credentials, method, args, kwargs):
        self._aws_credentials = aws_credentials
        self._method = method
        self._args = args
        self._kwargs = kwargs

    def __eq__(self, other):
        """ Equality comparison operator """
        if isinstance(other, Invocation):
            return \
                self._aws_credentials == other._aws_credentials and \
                self._method == other._method and \
                self._args == other._args and \
                self._kwargs == other._kwargs

        return False

    def __ne__(self, other):
        """ Inequality comparison operator """
        return not self == other

    def __str__(self):
        call_str = "Invocation object at {}: " \
                   "_aws_credentials=[{},{}], " \
                   "_method={}, " \
                   "_args={}, " \
                   "_kwargs={}".\
            format(
                id(self),
                self._aws_credentials.access_key_id, self._aws_credentials.secret_access_key,
                self._method,
                self._args,
                self._kwargs
            )

        return call_str


class FakeS3Util(object):
    calls = []

    def __init__(self, aws_credentials):
        self._aws_credentials = aws_credentials

    def __getattr__(self, attr):
        if hasattr(S3Util, attr):
            def wrapper(*args, **kwargs):
                FakeS3Util.calls.append(Invocation(self._aws_credentials, attr, args, kwargs))

            return wrapper

        raise AttributeError(attr)

    @staticmethod
    def is_s3_path(*args, **kwargs):
        """
        With this one function we will test invocation of static methods
        """
        FakeS3Util.calls.append(Invocation(AWSCredentials("", ""), "is_s3_path", args, kwargs))
        return S3Util.is_s3_path(*args, **kwargs)


class TestAWSS3CredentialsWrapper(object):

    aws_credentials_api = AWSCredentials("access_key_id-api", "secret_access_key-api")
    aws_credentials_put = AWSCredentials("access_key_id-put", "secret_access_key-put")
    aws_credentials_del = AWSCredentials("access_key_id-del", "secret_access_key-del")

    ERROR_METHOD_NOT_SUPPORTED_TEMPLATE = \
        "The following method is not supported for AWSS3CredentialsWrapper: {attr}()"
    ERROR_UNKNOWN_ATTRIBUTE_TEMPLATE = \
        "{attr} is not an attribute of S3Util"

    ERROR_UNMANAGED_BUCKET_TEMPLATE = \
        "AWSS3CredentialsWrapper.{attr}() has been called with arguments pointing to S3 buckets which are " \
        "not managed by AWSS3CredentialsWrapper: {buckets}"

    ERROR_CROSS_BUCKET_ACCESS_TEMPLATE = \
        "AWSS3CredentialsWrapper.{attr}() has been called with arguments pointing to both" \
        " data and applications S3 buckets, this is not supported."

    @staticmethod
    def _create_aws_factory_s3_util(application_buckets, data_buckets):
        s3_util = AWSS3CredentialsWrapper.__new__(AWSS3CredentialsWrapper)
        s3_util.app_buckets = application_buckets
        s3_util.data_buckets = data_buckets

        s3_util.s3_util_api = FakeS3Util(TestAWSS3CredentialsWrapper.aws_credentials_api)
        s3_util.s3_util_put = FakeS3Util(TestAWSS3CredentialsWrapper.aws_credentials_put)
        s3_util.s3_util_del = FakeS3Util(TestAWSS3CredentialsWrapper.aws_credentials_del)

        return s3_util

    @staticmethod
    def _reset():
        FakeS3Util.calls = []

    def setup_method(self, _):
        TestAWSS3CredentialsWrapper._reset()

    @staticmethod
    def _assert_calls_formatted(call_0, call_1):
        not_equal_msg = "<{}> is not equal to <{}>".format(call_0, call_1)
        assert call_0 == call_1, not_equal_msg

    @pytest.mark.emr
    def test_upload_object(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app"], ["data"])

        # case 1: point to application bucket
        s3_util.upload_object("local/path", "s3://app/s3/path")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "upload_object",
                       ("local/path", "s3://app/s3/path"), {})
        )

        # case 2: point to data bucket
        s3_util.upload_object("local/path", "s3://data/s3/path")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "upload_object",
                       ("local/path", "s3://data/s3/path"), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(attr="upload_object",
                                                                                     buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.upload_object("local/path", "s3://unknown/s3/path")

    @pytest.mark.emr
    def test_upload_child_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.upload_child_objects("local/path", "s3://app/s3/path", recursive=True)
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "upload_child_objects",
                       ("local/path", "s3://app/s3/path"), {"recursive": True})
        )

        # case 2: point to data bucket
        s3_util.upload_child_objects("local/path", "s3://lake/s3/path", fn_pattern="*.py")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "upload_child_objects",
                       ("local/path", "s3://lake/s3/path"), {"fn_pattern": "*.py"})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="upload_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.upload_child_objects("local/path", "s3://unknown/s3/path")

    @pytest.mark.emr
    def test_delete_object(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.delete_object("s3://log/s3/path/some.log")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "delete_object",
                       ("s3://log/s3/path/some.log",), {})
        )

        # case 2: point to data bucket
        s3_util.delete_object("s3://landing/s3/path/some.log")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_del, "delete_object",
                       ("s3://landing/s3/path/some.log",), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="delete_object", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.delete_object("s3://unknown/s3/path/some.log")

    @pytest.mark.emr
    def test_delete_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.delete_objects("s3://log/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "delete_objects", ("s3://log/s3/path/",), {})
        )

        # case 2: point to data bucket
        s3_util.delete_objects("s3://landing/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_del, "delete_objects",
                       ("s3://landing/s3/path/",), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="delete_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.delete_objects("s3://unknown/s3/path/")

    @pytest.mark.emr
    def test_delete_child_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.delete_child_objects("s3://log/s3/path")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "delete_child_objects",
                       ("s3://log/s3/path",), {})
        )

        # case 2: point to data bucket
        s3_util.delete_child_objects("s3://landing/s3/path")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_del, "delete_child_objects",
                       ("s3://landing/s3/path",), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="delete_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.delete_child_objects("s3://unknown/s3/path/")

    @pytest.mark.emr
    def test_list_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.list_objects("s3://log/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "list_objects", ("s3://log/s3/path/",), {})
        )

        # case 2: point to data bucket
        s3_util.list_objects("s3://landing/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "list_objects",
                       ("s3://landing/s3/path/",), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="list_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.list_objects("s3://unknown/s3/path/")

    @pytest.mark.emr
    def test_list_child_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.list_child_objects("s3://app/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "list_child_objects",
                       ("s3://app/s3/path/",), {})
        )

        # case 2: point to data bucket
        s3_util.list_child_objects("s3://landing/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "list_child_objects",
                       ("s3://landing/s3/path/",), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="list_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.list_child_objects("s3://unknown/s3/path/")

    @pytest.mark.emr
    def test_move_object(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application buckets
        s3_util.move_object("s3://app/s3/path/some.obj", destination_s3_path="s3://log/s3/path/some.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "move_object",
                       ("s3://app/s3/path/some.obj",), {"destination_s3_path": "s3://log/s3/path/some.obj"})
        )

        # case 2: point to data buckets
        err_msg = TestAWSS3CredentialsWrapper.ERROR_METHOD_NOT_SUPPORTED_TEMPLATE.format(attr="move_object")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.move_object(
                src_s3_path="s3://landing/s3/path/some.obj",
                destination_s3_path="s3://lake/s3/path/some.obj"
            )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_object", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_object("s3://unknown/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 4: try to copy from app to landing
        err_msg = TestAWSS3CredentialsWrapper.ERROR_CROSS_BUCKET_ACCESS_TEMPLATE.format(attr="move_object")
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_object("s3://app/d/o.obj", "s3://landing/d/o.obj")

        # case 5: try to copy from app to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_object", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_object("s3://app/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 6: try to copy from landing to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_object", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_object("s3://landing/d1/o.obj", "s3://unknown/d2/o.obj")

    @pytest.mark.emr
    def test_move_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application buckets
        s3_util.move_objects("s3://app/s3/path/", destination_s3_prefix_path="s3://log/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "move_objects",
                       ("s3://app/s3/path/",), {"destination_s3_prefix_path": "s3://log/s3/path/"})
        )

        # case 2: point to data buckets
        err_msg = TestAWSS3CredentialsWrapper.ERROR_METHOD_NOT_SUPPORTED_TEMPLATE.format(attr="move_objects")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.move_objects("s3://landing/s3/path/", "s3://lake/s3/path/")

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_objects("s3://unknown/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 4: try to copy from app to landing
        err_msg = TestAWSS3CredentialsWrapper.ERROR_CROSS_BUCKET_ACCESS_TEMPLATE.format(attr="move_objects")
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_objects("s3://app/d/", "s3://landing/d/")

        # case 5: try to copy from app to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_objects("s3://app/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 6: try to copy from landing to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_objects("s3://landing/d1/o.obj", "s3://unknown/d2/o.obj")

    @pytest.mark.emr
    def test_move_child_objects(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application buckets
        s3_util.move_child_objects("s3://app/s3/path/", "s3://log/s3/path/")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "move_child_objects",
                       ("s3://app/s3/path/", "s3://log/s3/path/"), {})
        )

        # case 2: point to data buckets
        err_msg = TestAWSS3CredentialsWrapper.ERROR_METHOD_NOT_SUPPORTED_TEMPLATE.format(attr="move_child_objects")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.move_child_objects(
                src_s3_prefix_path="s3://landing/s3/path/",
                destination_s3_prefix_path="s3://lake/s3/path/"
            )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_child_objects("s3://unknown/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 4: try to copy from app to landing
        err_msg = TestAWSS3CredentialsWrapper.ERROR_CROSS_BUCKET_ACCESS_TEMPLATE.format(attr="move_child_objects")
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_child_objects("s3://app/d/", "s3://landing/d/")

        # case 5: try to copy from app to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_child_objects("s3://app/d1/o.obj", "s3://unknown/d2/o.obj")

        # case 6: try to copy from landing to unmanaged
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="move_child_objects", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.move_child_objects("s3://landing/d1/o.obj", "s3://unknown/d2/o.obj")

    @pytest.mark.emr
    def test_object_exists(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.object_exists("s3://app/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(
                TestAWSS3CredentialsWrapper.aws_credentials_api,
                "object_exists",
                ("s3://app/s3/path/o.obj",),
                {}
            )
        )

        # case 2: point to data bucket
        s3_util.object_exists(s3_path="s3://landing/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "object_exists", (),
                       {"s3_path": "s3://landing/s3/path/o.obj"})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="object_exists", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.object_exists("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_read_gzip_file_content(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.read_gzip_file_content("s3://app/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "read_gzip_file_content",
                       ("s3://app/s3/path/o.obj",), {})
        )

        # case 2: point to data bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_METHOD_NOT_SUPPORTED_TEMPLATE.format(attr="read_gzip_file_content")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.read_gzip_file_content(s3_path="s3://landing/s3/path/o.obj")

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="read_gzip_file_content", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.read_gzip_file_content("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_get_bucket_and_key(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.get_bucket_and_key("s3://app/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "get_bucket_and_key",
                       ("s3://app/s3/path/o.obj",), {})
        )

        # case 2: point to data bucket
        s3_util.get_bucket_and_key(object_key="s3://landing/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "get_bucket_and_key", (),
                       {"object_key": "s3://landing/s3/path/o.obj"})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="get_bucket_and_key", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.get_bucket_and_key("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_wait_for_file_availability(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.wait_for_file_availability(
            s3_file_location="s3://app/s3/path/o.obj",
            polling_interval_seconds=3,
            timeout_seconds=10)
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "wait_for_file_availability", (),
                       {
                           "s3_file_location": "s3://app/s3/path/o.obj",
                           "polling_interval_seconds": 3,
                           "timeout_seconds": 10
                       })
        )

        # case 2: point to data bucket
        s3_util.wait_for_file_availability("s3://landing/s3/path/o.obj", 3000, 10000)
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "wait_for_file_availability",
                       ("s3://landing/s3/path/o.obj", 3000, 10000), {})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="wait_for_file_availability", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.wait_for_file_availability("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_normalize_s3_path(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        s3_util.normalize_s3_path("s3a://app/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_api, "normalize_s3_path",
                       ("s3a://app/s3/path/o.obj",), {})
        )

        # case 2: point to data bucket
        s3_util.normalize_s3_path(s3_path="s3n://landing/s3/path/o.obj")
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(TestAWSS3CredentialsWrapper.aws_credentials_put, "normalize_s3_path", (),
                       {"s3_path": "s3n://landing/s3/path/o.obj"})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="normalize_s3_path", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.normalize_s3_path("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_is_s3_path(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket
        assert s3_util.is_s3_path("s3a://app/s3/path/o.obj") is True
        self._assert_calls_formatted(
            FakeS3Util.calls[0],
            Invocation(AWSCredentials("", ""), "is_s3_path", ("s3a://app/s3/path/o.obj",), {})
        )

        # case 2: point to data bucket
        assert s3_util.is_s3_path(obj="/landing/s3/path/o.obj") is False
        self._assert_calls_formatted(
            FakeS3Util.calls[1],
            Invocation(AWSCredentials("", ""), "is_s3_path", (), {"obj": "/landing/s3/path/o.obj"})
        )

        # case 3: point to un-managed bucket
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET_TEMPLATE.format(
            attr="is_s3_path", buckets=["unknown"])
        err_msg = re.escape(err_msg)

        with pytest.raises(M3DIllegalArgumentException, match=err_msg):
            s3_util.is_s3_path("s3://unknown/s3/path/o.obj")

    @pytest.mark.emr
    def test_unknown_method(self):
        s3_util = TestAWSS3CredentialsWrapper._create_aws_factory_s3_util(["app", "log"], ["landing", "lake"])

        # case 1: point to application bucket. s3_resource is not an attribute of S3Util.
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNKNOWN_ATTRIBUTE_TEMPLATE.format(attr="s3_resource")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.s3_resource("s3://log/s3/path/")

        # case 2: point to data bucket. unknown_method is not an attribute of S3Util.
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNKNOWN_ATTRIBUTE_TEMPLATE.format(attr="unknown_method")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.unknown_method("s3://landing/s3/path/")

        # case 3: point to un-managed bucket. unknown_method is still not an attribute of S3Util.
        err_msg = TestAWSS3CredentialsWrapper.ERROR_UNKNOWN_ATTRIBUTE_TEMPLATE.format(attr="unknown_method")
        err_msg = re.escape(err_msg)

        with pytest.raises(AttributeError, match=err_msg):
            s3_util.unknown_method("s3://unknown/s3/path/")
