import json

import pytest
from mock import patch

from m3d import M3D
from test.core.acon_helper import AconHelper
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase
from test.test_util.concurrent_executor import ConcurrentExecutor


class TestAlgorithmScalaRunnerEMR(EMRSystemUnitTestBase):
    destination_system = "bdp"
    destination_database = "emr_test"
    destination_environment = "dev"
    algorithm_instance = "scala_runner_custom"

    def env_setup(
            self,
            destination_system,
            destination_database,
            destination_environment,
            algorithm_instance,
            acon_dict
    ):
        tmpdir = self.local_run_dir

        m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict = \
            super(TestAlgorithmScalaRunnerEMR, self).env_setup(
                tmpdir,
                destination_system,
                destination_database,
                destination_environment
            )

        AconHelper.setup_acon_from_dict(
            m3d_config_dict["tags"]["config"],
            destination_database,
            destination_environment,
            algorithm_instance,
            acon_dict
        )

        return m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("m3d.util.s3_util.S3Util.delete_object")
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_run_algorithm(self, add_tags_patch, delete_object_patch, send_email_patch):
        parameters_dict = {
            "scala_class": "CustomScalaClass",
            "key_el": "val",
            "key_list": ["x", 15],
            "key_dict": {
                "first": 1,
                "second": "2nd"
            }
        }

        acon_dict = {
            "algorithm": {
                "python_class": "AlgorithmScalaRunner",
                "parameters": parameters_dict
            }
        }

        m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.destination_system,
                self.destination_database,
                self.destination_environment,
                self.algorithm_instance,
                acon_dict
            )

        algorithm_args = [
            m3d_config_file,
            self.destination_system,
            self.destination_database,
            self.destination_environment,
            self.algorithm_instance,
            self.emr_cluster_id
        ]

        spark_options = {
            "spark.driver.memory": "5G",
            "spark.executor.memory": "35G",
            "spark.executor.instances": 12,
            "spark.executor.cores": 2,
            "spark.scheduler.mode": "FAIR"
        }

        ext_params_dict = {
            "environment": {
                "spark": spark_options
            }
        }

        algorithm_kwargs = {
            "ext_params": json.dumps(ext_params_dict)
        }

        emr_steps_completer = self.create_emr_steps_completer(expected_steps_count=1, timeout_seconds=3)

        with ConcurrentExecutor(emr_steps_completer):
            M3D.run_algorithm(*algorithm_args, **algorithm_kwargs)

        # Check EMR step
        mock_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        assert len(mock_cluster.steps) == 1

        spark_step = mock_cluster.steps[0]

        assert spark_step.jar == "command-runner.jar"
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[5] == "--conf"
        assert spark_step.args[7] == "--conf"
        assert spark_step.args[9] == "--conf"
        assert spark_step.args[11] == "--conf"
        assert spark_step.args[13] == "--conf"

        expected_spark_conf_options = set(map(lambda p: "{}={}".format(p[0], p[1]), spark_options.items()))
        actual_spark_conf_options = set(map(lambda x: spark_step.args[x], [6, 8, 10, 12, 14]))
        assert expected_spark_conf_options == actual_spark_conf_options

        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-3] == "CustomScalaClass"
        config_json_s3 = spark_step.args[-2]
        assert spark_step.args[-1] == "s3"

        # Check config.json file content
        config_json_content = self.get_object_content_from_s3(config_json_s3)
        config_json_dict = json.loads(config_json_content)
        assert config_json_dict == parameters_dict

        # Check that config.json was removed in the end
        delete_object_patch.assert_called_once()
        delete_object_patch_call_args, _ = delete_object_patch.call_args
        assert delete_object_patch_call_args == (config_json_s3,)

        # Check the successful execution of algorithm
        send_email_patch.assert_called_once()
        send_email_patch_call_args, _ = send_email_patch.call_args
        assert str(send_email_patch_call_args[1]).startswith("Success")

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "ApiMethod", "Value": "run_algorithm"},
            {"Key": "AlgorithmClass", "Value": "AlgorithmScalaRunner"},
            {"Key": "AlgorithmInstance", "Value": "scala_runner_custom"}
        ], key=lambda x: x["Key"])
