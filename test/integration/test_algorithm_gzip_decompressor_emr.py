import json

import pytest
from mock import patch

from m3d import M3D
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from test.core.acon_helper import AconHelper
from test.core.s3_table_test_base import S3TableTestBase


class TestAlgorithmGzipDecompressorEMR(S3TableTestBase):
    destination_system = "bdp"
    destination_database = "emr_test"
    destination_environment = "dev"
    algorithm_instance = "gzip_decompressor_bytes"
    algo_class_name = "GzipDecompressorBytes"
    table = "test101"
    destination_table = "bi_test101"

    test_acon = "test/resources/test_algorithm_gzip_decompressor_bytes_emr/acon-emr_test-gzip_decompressor_bytes.json"
    param_file_name_part = "{}-{}".format(destination_environment, algorithm_instance)

    @pytest.mark.emr
    def test_run_algorithm(self):
        m3d_config_file, scon_emr_file, tconx_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                self.destination_system,
                self.destination_database,
                self.destination_environment,
                self.destination_table
            )

        _, acon_dict = AconHelper.setup_acon_from_file(
            m3d_config_dict["tags"]["config"],
            self.destination_database,
            self.destination_environment,
            self.algorithm_instance,
            self.test_acon
        )

        algorithm_args = [
            m3d_config_file,
            self.destination_system,
            self.destination_database,
            self.destination_environment,
            self.algorithm_instance,
        ]

        algorithm_kwargs = {
            "emr_cluster_id": self.emr_cluster_id,
            "ext_params": json.dumps({
                "environment": {
                    "spark": {
                        "spark.driver.memory": "5G",
                        "spark.executor.memory": "20G",
                        "spark.executor.instances": 10,
                        "spark.executor.cores": 1,
                        "spark.scheduler.mode": "FAIR"
                    }
                },
                "algorithm": {
                    "destination_table": self.destination_table,
                }
            })
        }

        bucket_landing = scon_emr_dict["environments"][self.destination_environment]["s3_buckets"]["landing"]

        expected_param_dict = {
            "directory": "s3://{bucket}/dev/bi/{table}/data/".format(
                bucket=bucket_landing,
                table=self.table
            ),
            "format": "csv",
            "thread_pool_size": 8
        }

        def run_command_in_cluster_patch(cmd, name):
            # Check command name
            assert "Running Spark Application" in str(name)
            print("Command is: {0}".format(cmd))
            command_components = cmd.split()

            # Check algorithm name from the spark command
            algorithm_class_name = command_components[-3]
            assert algorithm_class_name == ScalaClasses.GZIP_DECOMPRESSOR

            # Check configuration file content
            algorithm_config_file_name = command_components[-2]
            actual_config_file_content = self.get_object_content_from_s3(algorithm_config_file_name)
            print("Actual config content: {0}".format(actual_config_file_content))

            algorithm_config_file_dict = json.loads(actual_config_file_content)

            assert algorithm_config_file_dict == expected_param_dict

        with patch("m3d.hadoop.emr.emr_system.EMRSystem.run_command_in_cluster",
                   side_effect=run_command_in_cluster_patch):
            with patch("m3d.util.util.Util.send_email") as email_patch:
                M3D.run_algorithm(*algorithm_args, **algorithm_kwargs)

        # Check the successful execution of algorithm
        call_args, _ = email_patch.call_args
        assert str(call_args[1]).startswith("Success")
