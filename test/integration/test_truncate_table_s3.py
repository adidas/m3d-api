import logging
import os

import mock
import pytest
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from test.core.s3_table_test_base import S3TableTestBase


class TestTruncateTableS3Integration(S3TableTestBase):

    @pytest.mark.emr
    @mock.patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_s3_cleanup(self, add_tags_patch, _):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        source_system = "bi"
        table = "test101"

        m3d_config_file, _, _, m3d_config_dict, scon_emr_dict = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )

        table_config_args = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {"emr_cluster_id": self.emr_cluster_id}

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        bucket_landing = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["landing"]
        bucket_lake = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["lake"]

        test_content = "sample content"

        landing_dir = "{environment}/{source_system}/{table}".format(
            environment=destination_environment,
            source_system=source_system,
            table=table
        )

        landing_data_dir = os.path.join(landing_dir, "data")
        landing_archive_dir = os.path.join(landing_dir, "archive")
        landing_work_dir = os.path.join(landing_dir, "work")

        landing_data_key = os.path.join(landing_data_dir, "new_landing_dump")
        landing_archive_key = os.path.join(landing_archive_dir, "old_dump.gz")
        landing_work_key = os.path.join(landing_work_dir, "temporary_data")

        lake_dir = "{environment}/{source_system}/{table}".format(
            environment=destination_environment,
            source_system=source_system,
            table=table
        )

        lake_data_dir = os.path.join(lake_dir, "data")
        lake_data_key = os.path.join(lake_data_dir, "new_lake_dump")

        # adding data to landing and lake directories
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_data_key, Body=test_content)
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_archive_key, Body=test_content)
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_work_key, Body=test_content)

        self.s3_resource.Bucket(bucket_lake).put_object(Key=lake_data_key, Body=test_content)
        logging.info("Calling  M3D.truncate_table()")
        M3D.truncate_table(*table_config_args, **table_config_kwargs)

        emr_backend = self.mock_emr.backends[self.default_aws_region]
        fake_cluster = emr_backend.clusters[self.emr_cluster_id]

        assert len(fake_cluster.steps) == 2

        # Get actual HQL statements
        actual_hqls = []

        for step in fake_cluster.steps:
            assert ["hive", "--silent", "-f"] == step.args[0:3]

            hql_file = step.args[3]
            hql_content = self.get_object_content_from_s3(hql_file)
            actual_hqls.append(hql_content)

        db_table_landing = "{}.{}{}".format(
            db_landing,
            destination_table,
            m3d_config_dict["tags"]["table_suffix_stage"]
        )
        landing_table_location = os.path.join("s3://", bucket_landing, landing_data_dir, "")

        db_table_lake = "{}.{}".format(db_lake, destination_table)
        lake_table_location = os.path.join("s3://", bucket_lake, lake_data_dir, "")

        landing_hql = "ALTER TABLE {} SET LOCATION \"{}\";".format(db_table_landing, landing_table_location)
        lake_hql = "\n".join([
            "DROP TABLE {};".format(db_table_lake),
            TestTruncateTableS3Integration._get_table_ddl_lake(db_table_lake, lake_table_location),
            "MSCK REPAIR TABLE {};".format(db_table_lake)
        ])

        expected_hqls = [landing_hql, lake_hql]

        assert actual_hqls == expected_hqls

        # checking landing directory
        landing_files = [k.key for k in self.s3_resource.Bucket(bucket_landing).objects.all()]
        assert len(landing_files) == 0

        # checking lake directory
        lake_files = [k.key for k in self.s3_resource.Bucket(bucket_lake).objects.all()]
        assert len(lake_files) == 0

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "truncate_table"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]

    @pytest.mark.emr
    @mock.patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_s3_cleanup_with_timestamped_table_location(self, add_tags_patch, _):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"
        destination_table_location1 = "data_20200101100015123"
        destination_table_location2 = "data_20200111100015456"

        source_system = "bi"
        table = "test101"

        m3d_config_file, _, _, m3d_config_dict, scon_emr_dict = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )

        table_config_args = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {"emr_cluster_id": self.emr_cluster_id}

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        bucket_landing = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["landing"]
        bucket_lake = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["lake"]

        test_content = "sample content"

        landing_dir = "{environment}/{source_system}/{table}".format(
            environment=destination_environment,
            source_system=source_system,
            table=table
        )

        landing_data_dir = os.path.join(landing_dir, "data")
        landing_archive_dir = os.path.join(landing_dir, "archive")
        landing_work_dir = os.path.join(landing_dir, "work")

        landing_data_key = os.path.join(landing_data_dir, "new_landing_dump")
        landing_archive_key = os.path.join(landing_archive_dir, "old_dump.gz")
        landing_work_key = os.path.join(landing_work_dir, "temporary_data")

        lake_dir = "{environment}/{source_system}/{table}".format(
            environment=destination_environment,
            source_system=source_system,
            table=table
        )

        lake_data_dir1 = os.path.join(lake_dir, destination_table_location1)
        lake_data_dir2 = os.path.join(lake_dir, destination_table_location2)
        lake_data_keys = [
            os.path.join(lake_data_dir1, "new_lake_dump1"),
            os.path.join(lake_data_dir1, "new_lake_dump2"),
            os.path.join(lake_data_dir2, "new_lake_dump3"),
            os.path.join(lake_data_dir2, "new_lake_dump4")
        ]

        # adding data to landing and lake directories
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_data_key, Body=test_content)
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_archive_key, Body=test_content)
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_work_key, Body=test_content)

        for key in lake_data_keys:
            self.s3_resource.Bucket(bucket_lake).put_object(Key=key, Body=test_content)

        logging.info("Calling M3D.truncate_table()")
        M3D.truncate_table(*table_config_args, **table_config_kwargs)

        emr_backend = self.mock_emr.backends[self.default_aws_region]
        fake_cluster = emr_backend.clusters[self.emr_cluster_id]

        assert len(fake_cluster.steps) == 2

        # Get actual HQL statements
        actual_hqls = []

        for step in fake_cluster.steps:
            assert ["hive", "--silent", "-f"] == step.args[0:3]

            hql_file = step.args[3]
            hql_content = self.get_object_content_from_s3(hql_file)
            actual_hqls.append(hql_content)

        db_table_landing = "{}.{}{}".format(
            db_landing,
            destination_table,
            m3d_config_dict["tags"]["table_suffix_stage"]
        )
        landing_table_location = os.path.join("s3://", bucket_landing, landing_data_dir, "")

        db_table_lake = "{}.{}".format(db_lake, destination_table)
        lake_table_location = os.path.join("s3://", bucket_lake, lake_data_dir2, "")

        landing_hql = "ALTER TABLE {} SET LOCATION \"{}\";".format(db_table_landing, landing_table_location)
        lake_hql = "\n".join([
            "DROP TABLE {};".format(db_table_lake),
            TestTruncateTableS3Integration._get_table_ddl_lake(db_table_lake, lake_table_location),
            "MSCK REPAIR TABLE {};".format(db_table_lake)
        ])

        expected_hqls = [landing_hql, lake_hql]

        assert actual_hqls == expected_hqls

        # checking landing directory
        landing_files = [k.key for k in self.s3_resource.Bucket(bucket_landing).objects.all()]
        assert len(landing_files) == 0

        # checking lake directory
        lake_files = [k.key for k in self.s3_resource.Bucket(bucket_lake).objects.all()]
        assert len(lake_files) == 0

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "truncate_table"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]

    @staticmethod
    def _get_table_ddl_lake(db_table, location):
        columns = ", ".join([
            "name1 varchar(21)",
            "name2 varchar(6)",
            "name3 varchar(4)"
        ])

        return "\n".join([
            "CREATE EXTERNAL TABLE {}({})".format(db_table, columns),
            "PARTITIONED BY (year smallint, month smallint)",
            "STORED AS PARQUET",
            "LOCATION \'{}\'".format(location),
            "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"
        ])
