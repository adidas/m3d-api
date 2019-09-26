import logging
import pytest
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from test.core.s3_table_test_base import S3TableTestBase


class TestDropOutViewS3Integration(S3TableTestBase):

    default_tconx = "test/resources/test_drop_out_view_s3/tconx-bdp-emr_test-dev-bi_test101.csv"

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_s3_cleanup(self, add_tags_patch, _):
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

        db_lake_out = scon_emr_dict["environments"][destination_environment]["schemas"]["lake_out"]

        lake_out = "bi_test101"

        logging.info("Calling  M3D.drop_lake_out_view()")
        M3D.drop_lake_out_view(*table_config_args, **table_config_kwargs)

        emr_backend = self.mock_emr.backends[self.default_aws_region]
        fake_cluster = emr_backend.clusters[self.emr_cluster_id]

        assert 1 == len(fake_cluster.steps)

        hive_step = fake_cluster.steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])
        expected_hql = "DROP VIEW IF EXISTS {}.{};".format(db_lake_out, lake_out)
        assert expected_hql == actual_hql_content_in_bucket

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "drop_lake_out_view"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetView",
            "Value": "dev_lake_out.bi_test101"
        }]
