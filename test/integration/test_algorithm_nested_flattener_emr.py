import json
import logging
import os

import py
import pytest
from mock import patch

from m3d import M3D
from m3d.config.config_service import ConfigService
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase
from test.test_util.concurrent_executor import ConcurrentExecutor


class TestAlgorithmNestedFlattenerEMR(EMRSystemUnitTestBase):
    destination_system = "bdp"
    destination_database = "emr_test"
    destination_environment = "dev"
    algorithm_instance = "nested_flattener"

    test_acon = "test/resources/test_algorithm_nested_flattener_emr/acon-emr_test-nested_flattener.json"

    def env_setup(
            self,
            local_run_dir,
            destination_system,
            destination_database,
            destination_environment
    ):
        m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict = \
            super(TestAlgorithmNestedFlattenerEMR, self).env_setup(
                self.local_run_dir,
                self.destination_system,
                self.destination_database,
                self.destination_environment
            )

        config_service = ConfigService(m3d_config_file)
        acon_path = config_service.get_acon_path(
            self.destination_database,
            self.destination_environment,
            self.algorithm_instance
        )

        os.makedirs(os.path.dirname(acon_path))

        acon_data = py.path.local(self.test_acon).read()
        py.path.local(acon_path).write(acon_data)

        return m3d_config_file, scon_emr_file, acon_path, \
            m3d_config_dict, scon_emr_dict

    @pytest.mark.emr
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    @patch("m3d.util.s3_util.S3Util.delete_object")
    @patch("m3d.util.util.Util.send_email")
    def test_run_algorithm(self, email_patch, delete_object_patch, add_tags_patch):
        m3d_config_file, _, acon_path, _, scon_emr_dict = self.env_setup(
            self.local_run_dir,
            self.destination_system,
            self.destination_database,
            self.destination_environment
        )

        schema_lake = scon_emr_dict["environments"][self.destination_environment]["schemas"]["lake"]
        bucket_lake = scon_emr_dict["environments"][self.destination_environment]["s3_buckets"]["lake"]

        spark_options = {
            "spark.driver.memory": "5G",
            "spark.executor.memory": "20G",
            "spark.executor.instances": 10,
            "spark.executor.cores": 1,
            "spark.scheduler.mode": "FAIR"
        }

        ext_params_dict = {
            "environment": {
                "spark": spark_options
            }
        }

        algorithm_args = [
            m3d_config_file,
            self.destination_system,
            self.destination_database,
            self.destination_environment,
            self.algorithm_instance,
            self.emr_cluster_id,
            json.dumps(ext_params_dict)
        ]

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        expected_step_count = 1
        timeout_seconds = 6

        emr_steps_completer = self.create_emr_steps_completer(
            expected_steps_count=expected_step_count,
            timeout_seconds=timeout_seconds
        )

        with ConcurrentExecutor(emr_steps_completer):
            M3D.run_algorithm(*algorithm_args)

        logging.info("Number of steps after execution: {}".format(len(fake_cluster.steps)))

        # Check the successful execution of algorithm
        email_patch.assert_called_once()
        call_args, _ = email_patch.call_args
        assert str(call_args[1]).startswith("Success")

        assert len(fake_cluster.steps) == expected_step_count

        spark_step = fake_cluster.steps[0]

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

        assert spark_step.args[-3] == "NestedFlattener"
        spark_json_s3 = spark_step.args[-2]

        assert spark_step.args[-1] == "s3"

        logging.info("Checking {}".format(spark_json_s3))

        # check that we tried to delete it
        delete_object_patch.assert_called_once()
        delete_object_call_args, _ = delete_object_patch.call_args
        assert str(delete_object_call_args[0]) == spark_json_s3

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert sorted(add_tags_patch_call_args_list[0][0][0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceTable", "Value": "s3://adidas-dev-lake/nest/nest_test/data"},
            {"Key": "TargetTable", "Value": "dev_lake.nest_flattened"}
        ], key=lambda x: x["Key"])
        assert sorted(add_tags_patch_call_args_list[1][0][0], key=lambda x: x["Key"]) == sorted([
            {"Key": "ApiMethod", "Value": "run_algorithm"},
            {"Key": "AlgorithmClass", "Value": "AlgorithmNestedFlattener"},
            {"Key": "AlgorithmInstance", "Value": "nested_flattener"}
        ], key=lambda x: x["Key"])

        # check content of config.json file
        spark_json_content = self.get_object_content_from_s3(spark_json_s3)

        spark_json_dict = json.loads(spark_json_content)

        assert spark_json_dict["source_location"] == os.path.join(ConfigService.Protocols.S3, bucket_lake,
                                                                  "nest/nest_test/data")
        assert spark_json_dict["target_table"] == schema_lake + "." + "nest_flattened"
        assert spark_json_dict["fields_to_flatten"] == [
            "user_attributes",
            "device_info",
            "events",
            "events__data",
            "events__data__device_current_state"
        ]
