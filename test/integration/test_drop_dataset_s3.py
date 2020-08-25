import logging
import os

import mock
import pytest
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase


class TestDropDatasetS3Integration(EMRSystemUnitTestBase):

    @pytest.mark.emr
    @mock.patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_s3_cleanup(self, add_tags_patch, _):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_dataset = "nest_nest_test"

        source_system = "nest"
        short_dataset_name = "nest_test"

        m3d_config_file, _, m3d_config_dict, scon_emr_dict = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment
        )

        dataset_config_args = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_dataset
        ]

        dataset_config_kwargs = {"emr_cluster_id": self.emr_cluster_id}

        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        bucket_landing = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["landing"]
        bucket_lake = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["lake"]

        test_content = "sample content"

        landing_dir = "{environment}/{source_system}/{dataset}".format(
            environment=destination_environment,
            source_system=source_system,
            dataset=short_dataset_name
        )

        landing_data_dir = os.path.join(landing_dir, "data")

        landing_data_key = os.path.join(landing_data_dir, "new_landing_dump")

        lake_dir = "{environment}/{source_system}/{dataset}".format(
            environment=destination_environment,
            source_system=source_system,
            dataset=short_dataset_name
        )

        lake_data_dir = os.path.join(lake_dir, "data")
        lake_data_key = os.path.join(lake_data_dir, "new_lake_dump")

        # adding data to landing and lake directories
        self.s3_resource.Bucket(bucket_landing).put_object(Key=landing_data_key, Body=test_content)
        self.s3_resource.Bucket(bucket_lake).put_object(Key=lake_data_key, Body=test_content)

        # checking if landing and lake directories contain the uploaded files
        landing_files = [k.key for k in self.s3_resource.Bucket(bucket_landing).objects.all()]
        assert len(landing_files) == 1
        lake_files = [k.key for k in self.s3_resource.Bucket(bucket_lake).objects.all()]
        assert len(lake_files) == 1

        logging.info("Calling  M3D.drop_dataset()")
        M3D.drop_dataset(*dataset_config_args, **dataset_config_kwargs)

        # checking if the files were removed
        landing_files = [k.key for k in self.s3_resource.Bucket(bucket_landing).objects.all()]
        assert len(landing_files) == 0
        lake_files = [k.key for k in self.s3_resource.Bucket(bucket_lake).objects.all()]
        assert len(lake_files) == 0

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "drop_dataset"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetDataset",
            "Value": "{}.{}".format(db_lake, destination_dataset)
        }]
