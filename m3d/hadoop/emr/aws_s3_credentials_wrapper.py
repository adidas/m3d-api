from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.util.s3_util import S3Util


class AWSS3CredentialsWrapper(object):
    """
    This class will wrap S3Util objects for each set of AWS credentials: API, S3 put and S3 delete.
    It will manage method routing to correct S3Util object based on arguments passed to the method.
    """

    put_methods = [
        "upload_object",
        "upload_child_objects",
        "list_objects",
        "list_child_objects",
        "object_exists",
        "wait_for_file_availability",
        "download_object"
    ]

    delete_methods = [
        "delete_object",
        "delete_objects",
        "delete_child_objects"
    ]

    static_methods = [
        "get_bucket_and_key",
        "normalize_s3_path",
        "is_s3_path"
    ]

    ERROR_METHOD_NOT_SUPPORTED = "The following method is not supported for AWSFactoryS3Wrapper: {attr}()"
    ERROR_UNKNOWN_ATTRIBUTE = "{attr} is not an attribute of S3Util"
    ERROR_CROSS_BUCKET_ACCESS = "AWSFactoryS3Wrapper.{attr}() has been called with arguments pointing to both" \
                                " data and applications S3 buckets, this is not supported."
    ERROR_UNMANAGED_BUCKET = "AWSFactoryS3Wrapper.{attr}() has been called with arguments pointing to S3 buckets " \
                             "which are not managed by AWSFactoryS3Wrapper: {buckets}"

    def __init__(
            self,
            application_buckets,
            data_buckets,
            aws_credentials_api,
            aws_credentials_s3_put,
            aws_credentials_s3_del
    ):
        self.app_buckets = application_buckets
        self.data_buckets = data_buckets

        self.s3_util_api = S3Util(aws_credentials_api)
        self.s3_util_put = S3Util(aws_credentials_s3_put)
        self.s3_util_del = S3Util(aws_credentials_s3_del)

    def __getattr__(self, attr):
        """
        This function will be called by Python automatically whenever we try to call a function on AWSFactoryS3Wrapper.
        It provides mapping from a function name, attr, to a callable object that will be called with function
        arguments.

        It will return a wrapper object which will decide which S3Util object to call based on function
        arguments. It will extract arguments that are S3 paths and will use this for choosing correct S3Util object
        to use.

        Please note that methods which mix paths from both S3 application and data buckets are not allowed. Function
        will also raise if client passes arguments pointing to S3 buckets which are not managed by AWSFactoryS3Wrapper,
        that is they are not in self.app_buckets or self.data_buckets.

        :param attr: name of the method that should be called
        :return: callable object which will redirect call to corresponding S3Util object
        """
        if hasattr(S3Util, attr):
            def s3_util_router(*args, **kwargs):
                s3_buckets = AWSS3CredentialsWrapper._extract_set_of_s3_buckets(*args, **kwargs)

                # check if we need to use S3Util with API credentials
                if s3_buckets.issubset(self.app_buckets):
                    return getattr(self.s3_util_api, attr)(*args, **kwargs)

                # check if we need to use S3Util with data credentials, put or delete
                if s3_buckets.issubset(self.data_buckets):
                    # based on method type determine which S3Util object to call
                    if (
                            attr in AWSS3CredentialsWrapper.static_methods or
                            attr in AWSS3CredentialsWrapper.put_methods
                    ):
                        # we will use self.s3_util_put for move operations as well
                        return getattr(self.s3_util_put, attr)(*args, **kwargs)
                    elif attr in AWSS3CredentialsWrapper.delete_methods:
                        return getattr(self.s3_util_del, attr)(*args, **kwargs)
                    else:
                        raise AttributeError(AWSS3CredentialsWrapper.ERROR_METHOD_NOT_SUPPORTED.format(attr=attr))

                # If we are here, then we are doing operation which has either both data and application S3 paths in
                # arguments, or S3 path which point to a buckets which is not managed by AWSFactoryS3Wrapper. Both
                # cases are not supported.
                self._raise_unsupported_bucket_operation_error(attr, s3_buckets)

            return s3_util_router

        raise AttributeError(AWSS3CredentialsWrapper.ERROR_UNKNOWN_ATTRIBUTE.format(attr=attr))

    @staticmethod
    def _extract_set_of_s3_buckets(*args, **kwargs):
        """
        Process all arguments and extract set of S3 buckets from them that represent S3 paths.
        """
        # extract arguments which are S3 paths from args and kwargs
        s3_paths = []
        s3_paths.extend(filter(lambda el: S3Util.is_s3_path(el), args))
        s3_paths.extend(filter(lambda el: S3Util.is_s3_path(el), kwargs.values()))

        # extract S3 bucket names from s3_paths
        s3_buckets = map(lambda s3_path: S3Util.get_bucket_and_key(s3_path)[0], s3_paths)

        return set(s3_buckets)

    def _raise_unsupported_bucket_operation_error(self, attr, s3_buckets_set):
        """
        Raise error indicating that either

            - attribute has been called with arguments which has both data
              and application S3 paths in arguments

            - attribute has been called with arguments that point to S3
              buckets which are not managed by AWSFactoryS3Wrapper

        Both cases are not supported.

        :param attr: attribute/method name that has been called
        :param s3_buckets_set: set of S3 buckets extracted from function arguments
        """
        if s3_buckets_set.issubset(self.app_buckets + self.data_buckets):
            raise M3DIllegalArgumentException(AWSS3CredentialsWrapper.ERROR_CROSS_BUCKET_ACCESS.format(attr=attr))

        else:
            # s3_buckets_set contains buckets which are not managed by AWSFactoryS3Wrapper.
            managed_s3_buckets = self.app_buckets + self.data_buckets
            unmanaged_s3_buckets = list(filter(
                lambda bucket: bucket not in managed_s3_buckets,
                s3_buckets_set
            ))

            raise M3DIllegalArgumentException(
                AWSS3CredentialsWrapper.ERROR_UNMANAGED_BUCKET.format(attr=attr, buckets=unmanaged_s3_buckets)
            )
