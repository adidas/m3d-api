# import responses
from mock import patch
import logging
import pytest

from moto.emr.models import FakeStep

from m3d import M3D
from test.core.s3_table_test_base import S3TableTestBase


class TestDropTableS3Integration(S3TableTestBase):

    default_tconx = "test/resources/test_drop_table_s3/tconx-bdp-emr_prod-dev-bi_test101.json"

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_s3_cleanup(self, add_tags_patch, _):
        # responses.add_passthru(self.default_server_url)

        logging.info("Starting s3 Checkup cleanup")

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, _, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        table_config_args = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        bucket_landing = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["landing"]
        bucket_lake = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["lake"]

        test_content = "sample content"
        test_lake_key_filename = "test_lake_key"
        test_land_key_filename = "test_land_key"

        source_system = "bi"
        table = "test101"

        test_land_key = "{environment}/{source_system}/{table}/data/{obj_name}".format(
            environment=destination_environment,
            source_system=source_system,
            table=table,
            obj_name=test_land_key_filename
        )

        test_lake_key = "{environment}/{source_system}/{table}/data/{obj_name}".format(
            environment=destination_environment,
            db_cd=db_lake,
            source_system=source_system,
            table=table,
            obj_name=test_lake_key_filename
        )

        # adding data to landing and lake directories
        self.s3_resource.Bucket(bucket_landing).put_object(Key=test_land_key, Body=test_content)
        self.s3_resource.Bucket(bucket_lake).put_object(Key=test_lake_key, Body=test_content)

        logging.info("Calling  M3D.create_table()")
        M3D.create_table(*table_config_args, **table_config_kwargs)

        logging.info("Calling  M3D.drop_table()")
        M3D.drop_table(*table_config_args, **table_config_kwargs)

        emr_backend = self.mock_emr.backends[self.default_aws_region]
        fake_cluster = emr_backend.clusters[self.emr_cluster_id]

        assert 3 == len(fake_cluster.steps)

        # Get actual HQL statements
        actual_hqls = []

        for step in fake_cluster.steps:
            assert ["hive", "--silent", "-f"] == step.args[0:3]

            hql_file = step.args[3]
            hql_content = self.get_object_content_from_s3(hql_file)
            actual_hqls.append(hql_content)

        expected_hqls = [
            'DROP TABLE {}.{}{};'.format(db_landing, destination_table, m3d_config_dict["tags"]["table_suffix_stage"]),
            'DROP TABLE {}.{};'.format(db_lake, destination_table)
        ]

        assert expected_hqls == actual_hqls[1:3]

        # checking landing directory
        landing_files = [k.key for k in self.s3_resource.Bucket(bucket_landing).objects.all()]
        assert len(landing_files) == 1
        assert landing_files[0] == test_land_key

        # checking lake directory
        lake_files = [k.key for k in self.s3_resource.Bucket(bucket_lake).objects.all()]
        assert len(lake_files) == 1
        assert lake_files[0] == test_lake_key

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 4
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "create_table"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]
        assert add_tags_patch_call_args_list[2][0][0] == [{
            "Key": "ApiMethod",
            "Value": "drop_table"
        }]
        assert add_tags_patch_call_args_list[3][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]
