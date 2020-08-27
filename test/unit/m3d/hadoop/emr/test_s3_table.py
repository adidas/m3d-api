import logging

import pytest
from moto import mock_s3

from m3d.config.config_service import ConfigService
from m3d.exceptions.m3d_exceptions import M3DException
from m3d.hadoop.emr.emr_exceptions import M3DEMRStepException
from m3d.hadoop.emr.s3_table import S3Table
from m3d.hadoop.emr.aws_s3_credentials_wrapper import AWSS3CredentialsWrapper
from m3d.util.aws_credentials import AWSCredentials

from test.test_util.boto3_util import Boto3Util


class FakeStorageSystem:

    def __init__(self, hql_validation_function, hql_validation_error=None):
        self.hql_validation_function = hql_validation_function
        self.hql_validation_error = hql_validation_error
        self.bucket_landing = TestS3Table.LANDING_SPEC.bucket
        self.bucket_lake = TestS3Table.LAKE_SPEC.bucket
        self.statements = []

        self.s3_util = AWSS3CredentialsWrapper(
            [],
            [self.bucket_landing, self.bucket_lake],
            AWSCredentials("aws_access_key_api", "aws_secret_key_api"),
            AWSCredentials("aws_access_key_s3_put", "aws_secret_key_s3_put"),
            AWSCredentials("aws_access_key_s3_del", "aws_secret_key_s3_del")
        )

    def execute_hive(self, hql):
        logging.info("Executing statement: {}".format(hql))
        if self.hql_validation_function(hql):
            self.statements.append(hql)
        else:
            if self.hql_validation_error:
                raise self.hql_validation_error
            else:
                raise M3DEMRStepException(
                    emr_cluster_id="",
                    emr_step_id="",
                    msg="Unable to execute HQL"
                )


class Storage:

    def __init__(self, table, bucket, data_dir, files):
        self.table = table
        self.bucket = bucket
        self.data_dir = data_dir
        self.keys = list(map(lambda x: "{}/{}".format(data_dir, x), files))


# noinspection PyMethodMayBeStatic
class TestS3Table(object):

    LANDING_SPEC = Storage(
        "landing.test101_stg1",
        "bucket_landing",
        "test/bi/test101/data/",
        ["file-00000.part", "file-00001.part"]
    )

    LAKE_SPEC = Storage(
        "lake.test101",
        "bucket_lake",
        "test/bi/test101/data/",
        ["file-00000.part", "file-00001.part", "file-00002.part"]
    )

    DEFAULT_CONFIG_PATH = "config/m3d/config.json"

    def _create_s3_table(self, s3_resource, hql_validation_function, hql_validation_error=None):
        s3_resource.create_bucket(Bucket=self.LAKE_SPEC.bucket)
        for f in self.LAKE_SPEC.keys:
            logging.info("Creating object s3://{}/{}".format(self.LAKE_SPEC.bucket, f))
            s3_resource.Bucket(self.LAKE_SPEC.bucket).put_object(Key=f, Body="")

        s3_resource.create_bucket(Bucket=self.LANDING_SPEC.bucket)
        for f in self.LANDING_SPEC.keys:
            logging.info("Creating object s3://{}/{}".format(self.LANDING_SPEC.bucket, f))
            s3_resource.Bucket(self.LANDING_SPEC.bucket).put_object(Key=f, Body="")

        s3_table = S3Table.__new__(S3Table)

        # landing
        s3_table.db_table_landing = self.LANDING_SPEC.table

        dir_landing_data = "s3://{}/{}".format(self.LANDING_SPEC.bucket, self.LANDING_SPEC.data_dir)
        s3_table.dir_landing_data = dir_landing_data
        s3_table.dir_landing_work = dir_landing_data.replace("data", "work")
        s3_table.dir_landing_archive = dir_landing_data.replace("data", "archive")
        s3_table.dir_landing_final = s3_table.dir_landing_data

        # lake
        s3_table.db_table_lake = self.LAKE_SPEC.table

        dir_lake_data = "s3://{}/{}".format(self.LAKE_SPEC.bucket, self.LAKE_SPEC.data_dir)
        s3_table.dir_lake_final = dir_lake_data

        s3_table.emr_system = FakeStorageSystem(hql_validation_function, hql_validation_error)
        s3_table.s3_resource = s3_resource

        test_landing_bucket_name = self.LANDING_SPEC.bucket
        test_lake_bucket_name = self.LAKE_SPEC.bucket

        s3_table.dir_landing_table = "s3://" + test_landing_bucket_name + "/" + self.LANDING_SPEC.data_dir
        s3_table.dir_lake_table = "s3://" + test_lake_bucket_name + "/" + self.LAKE_SPEC.data_dir

        s3_table.config_service = ConfigService(TestS3Table.DEFAULT_CONFIG_PATH)

        s3_table.partitioned_by = "month"
        s3_table.header_lines = 0
        s3_table.delimiter = "|"

        s3_table.columns_lake = [
            ("name1", "varchar(21)"),
            ("name2", "varchar(6)"),
            ("name3", "varchar(4)")
        ]

        return s3_table

    def _list_deleted_keys(self, fs, bucket_name):
        keys = filter(lambda x: x.deleted, fs.s3.buckets[bucket_name].objects.objects)
        return list(map(lambda x: x.value, keys))

    @staticmethod
    def list_objects_in_bucket(s3_bucket):
        s3_resource = Boto3Util.create_s3_resource()
        objects = [obj.key for obj in s3_resource.Bucket(s3_bucket).objects.all()]
        return sorted(objects)

    @pytest.mark.emr
    @mock_s3
    def test_drop_tables_successful_execution(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: True)
        s3_table.drop_tables()

        assert len(s3_table.emr_system.statements) == 2

        # nothing should be deleted by drop_tables() call
        assert self.list_objects_in_bucket(self.LAKE_SPEC.bucket) == self.LAKE_SPEC.keys
        assert self.list_objects_in_bucket(self.LANDING_SPEC.bucket) == self.LANDING_SPEC.keys

    @pytest.mark.emr
    @mock_s3
    def test_drop_tables_both_tables_fail_to_drop(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: False)
        with pytest.raises(Exception, match="^Unable to drop any of the following tables.+"):
            s3_table.drop_tables()

        assert len(s3_table.emr_system.statements) == 0

        # nothing should be deleted by drop_tables() call
        assert self.list_objects_in_bucket(self.LAKE_SPEC.bucket) == self.LAKE_SPEC.keys
        assert self.list_objects_in_bucket(self.LANDING_SPEC.bucket) == self.LANDING_SPEC.keys

    @pytest.mark.emr
    @mock_s3
    def test_drop_tables_one_hive_table_fails_to_drop_1(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: self.LANDING_SPEC.table in x)
        s3_table.drop_tables()

        assert len(s3_table.emr_system.statements) == 1

        # nothing should be deleted by drop_tables() call
        assert self.list_objects_in_bucket(self.LAKE_SPEC.bucket) == self.LAKE_SPEC.keys
        assert self.list_objects_in_bucket(self.LANDING_SPEC.bucket) == self.LANDING_SPEC.keys

    @pytest.mark.emr
    @mock_s3
    def test_drop_tables_one_hive_table_fails_to_drop_2(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: self.LAKE_SPEC.table in x)
        s3_table.drop_tables()

        assert len(s3_table.emr_system.statements) == 1

        # nothing should be deleted by drop_tables() call
        assert self.list_objects_in_bucket(self.LAKE_SPEC.bucket) == self.LAKE_SPEC.keys
        assert self.list_objects_in_bucket(self.LANDING_SPEC.bucket) == self.LANDING_SPEC.keys

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_everything_deleted_successfully(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: True)
        s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 2

        landing_dir = "s3://{}/{}".format(self.LANDING_SPEC.bucket, self.LANDING_SPEC.data_dir)
        lake_dir = "s3://{}/{}".format(self.LAKE_SPEC.bucket, self.LAKE_SPEC.data_dir)

        expected_statements = [
            'ALTER TABLE {} SET LOCATION "{}";'.format(self.LANDING_SPEC.table, landing_dir),
            'DROP TABLE {};\n'.format(self.LAKE_SPEC.table) +
            TestS3Table._get_table_ddl_lake(self.LAKE_SPEC.table, s3_table.columns_lake, lake_dir) + "\n" +
            'MSCK REPAIR TABLE {};'.format(self.LAKE_SPEC.table)
        ]

        assert s3_table.emr_system.statements == expected_statements
        assert not self.list_objects_in_bucket(self.LANDING_SPEC.bucket)
        assert not self.list_objects_in_bucket(self.LAKE_SPEC.bucket)

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_both_repairs_fail_expectedly(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(
            s3_resource,
            lambda x: False,
            M3DEMRStepException("", "", "Table not found")
        )

        s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 0

        assert not self.list_objects_in_bucket(self.LAKE_SPEC.bucket)
        assert not self.list_objects_in_bucket(self.LANDING_SPEC.bucket)

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_both_repairs_fail_unexpectedly(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: False)

        with pytest.raises(M3DException, match="^Failed to truncate any of the following tables: .+"):
            s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 0

        assert not self.list_objects_in_bucket(self.LAKE_SPEC.bucket)
        assert not self.list_objects_in_bucket(self.LANDING_SPEC.bucket)

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_one_repair_fails_unexpectedly_1(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: self.LANDING_SPEC.table in x)
        s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 1

        landing_dir = "s3://{}/{}".format(self.LANDING_SPEC.bucket, self.LANDING_SPEC.data_dir)

        assert s3_table.emr_system.statements == [
            'ALTER TABLE {} SET LOCATION "{}";'.format(self.LANDING_SPEC.table, landing_dir)
        ]

        assert not self.list_objects_in_bucket(self.LAKE_SPEC.bucket)
        assert not self.list_objects_in_bucket(self.LANDING_SPEC.bucket)

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_one_repair_fails_unexpectedly_2(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: self.LAKE_SPEC.table in x)
        s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 1

        lake_dir = "s3://{}/{}".format(self.LAKE_SPEC.bucket, self.LAKE_SPEC.data_dir)

        assert s3_table.emr_system.statements == [
            'DROP TABLE {};\n'.format(self.LAKE_SPEC.table) +
            TestS3Table._get_table_ddl_lake(self.LAKE_SPEC.table, s3_table.columns_lake, lake_dir) + "\n" +
            'MSCK REPAIR TABLE {};'.format(self.LAKE_SPEC.table)
        ]

        assert not self.list_objects_in_bucket(self.LAKE_SPEC.bucket)
        assert not self.list_objects_in_bucket(self.LANDING_SPEC.bucket)

    @pytest.mark.emr
    @mock_s3
    def test_truncate_tables_wrong_files_not_deleted(self):
        s3_resource = Boto3Util.create_s3_resource()
        s3_table = self._create_s3_table(s3_resource, lambda x: True)

        landing_extra_keys = sorted(["test_key1", "test_key2"])
        lake_extra_keys = sorted(["test_key1", "test_dir/test_key2"])

        for k in landing_extra_keys:
            s3_resource.Bucket(self.LANDING_SPEC.bucket).put_object(Key=k, Body="")
        for k in lake_extra_keys:
            s3_resource.Bucket(self.LAKE_SPEC.bucket).put_object(Key=k, Body="")

        s3_table.truncate_tables()

        assert len(s3_table.emr_system.statements) == 2

        assert self.list_objects_in_bucket(self.LAKE_SPEC.bucket) == lake_extra_keys
        assert self.list_objects_in_bucket(self.LANDING_SPEC.bucket) == landing_extra_keys

    @staticmethod
    def _get_table_ddl_lake(db_table, columns, location):
        columns_str = ", ".join(map(lambda x: "{} {}".format(x[0], x[1]), columns))

        return "\n".join([
            "CREATE EXTERNAL TABLE {}({})".format(db_table, columns_str),
            "PARTITIONED BY (year smallint, month smallint)",
            "STORED AS PARQUET",
            "LOCATION \'{}\'".format(location),
            "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"
        ])
