import json
import logging
import os

import py
import pytest
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.dataset.dataset_factory import DataSetFactory
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.util.data_types import DataType
from test.core.acon_helper import AconHelper
from test.core.s3_table_test_base import S3TableTestBase


class TestLoadTableAppendS3(S3TableTestBase):

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    @patch("m3d.hadoop.core.spark_executor.SparkExecutor._remove_parameter_json")
    def test_load_table_append(self, remove_json_patch, add_tags_patch, _0, _1):

        target_partitions = ["year", "month", "day"]
        regex_filename = ["[0-9]{4}", "(?<=[0-9]{4})([0-9]{2})(?=[0-9]{2})", "(?<=[0-9]{6})([0-9]{2})"]
        spark_external_parameters = '''
                {
                    "spark.driver.memory": "99G",
                    "spark.executor.instances": "99",
                    "spark.executor.memory": "90G"
                }
                '''
        null_value = "test_null_value"
        quote_character = "test_quote"
        compute_table_statistics = True
        verify_schema = False
        data_type = DataType.STRUCTURED
        reader_mode = "DROPMALFORMED"
        metadata_update_strategy = "SparkRecoverPartitionsCustom"

        source_system = AppendLoadConfig.destination_table.split("_", 1)[0]
        table = AppendLoadConfig.destination_table.split("_", 1)[-1]
        test_target_dir = "s3://{lake_bucket}/{destination_environment}/{system}/{table}/data/".format(
            lake_bucket=self.default_dev_lake_bucket,
            destination_environment=AppendLoadConfig.destination_environment,
            system=source_system,
            table=table
        )

        config = AppendLoadConfig(
            self.local_run_dir,
            self.env_setup,
            target_partitions,
            regex_filename,
            null_value=null_value,
            quote_character=quote_character,
            metadata_update_strategy=metadata_update_strategy,
            compute_table_statistics=compute_table_statistics,
            verify_schema=verify_schema,
            data_type=data_type,
            reader_mode=reader_mode
        )
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        config.load_table(self.emr_cluster_id, spark_external_parameters)

        # Check EMR steps
        assert len(fake_cluster.steps) == 1

        # Check args of spark-submit EMR step
        spark_step = fake_cluster.steps[0]

        assert spark_step.jar == "command-runner.jar"
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == config.expected_algorithms_jar_path
        assert spark_step.args[-3] == config.load_type
        assert spark_step.args[-2] == config.config_filepath
        assert spark_step.args[-1] == "s3"

        # Check that config_file_s3 file is on application S3 bucket
        app_files = self.get_child_objects(config.dataset.dir_apps_append_load)
        app_json_files = list(filter(lambda app_file: os.path.basename(app_file).endswith(".json"), app_files))
        assert len(app_json_files) == 1
        assert app_json_files[0] == config.config_filepath

        # Check config file for Spark
        actual_parameters = json.loads(self.get_object_content_from_s3(config.config_filepath))
        expected_table_full_name = "{}.{}".format(config.db_name_lake, config.destination_table)
        expected_parameters = {
            "target_table": expected_table_full_name,
            "source_dir": config.dataset.dir_landing_final,
            "header_dir": config.dataset.dir_landing_header,
            "delimiter": "|",
            "has_header": False,
            "target_partitions": target_partitions,
            "regex_filename": regex_filename,
            "file_format": "dsv",
            "null_value": "test_null_value",
            "quote_character": "test_quote",
            "compute_table_statistics": True,
            "data_type": DataType.STRUCTURED,
            "verify_schema": False,
            "metadata_update_strategy": "SparkRecoverPartitionsCustom",
            "target_dir": test_target_dir,
            "reader_mode": "DROPMALFORMED"
        }
        assert actual_parameters == expected_parameters

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 1
        assert sorted(add_tags_patch_call_args_list[0][0][0], key=lambda x: x["Key"]) == sorted([
            {"Key": "ApiMethod", "Value": "load_table"},
            {"Key": "LoadType", "Value": "AppendLoad"},
            {"Key": "TargetTable", "Value": config.destination_table}
        ], key=lambda x: x["Key"])

        remove_json_patch.assert_called_once()
        assert remove_json_patch.call_args_list[0][0][0] == app_files[0]

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    @patch("m3d.hadoop.core.spark_executor.SparkExecutor._remove_parameter_json")
    def test_load_table_append_parquet(self, remove_json_patch, _0, _1, _2):
        target_partitions = ["year", "month", "day"]
        regex_filename = ["[0-9]{4}", "(?<=[0-9]{4})([0-9]{2})(?=[0-9]{2})", "(?<=[0-9]{6})([0-9]{2})"]
        spark_external_parameters = '''
                {
                    "spark.driver.memory": "99G",
                    "spark.executor.instances": "99",
                    "spark.executor.memory": "90G"
                }
                '''
        source_system = AppendLoadConfig.destination_table.split("_", 1)[0]
        table = AppendLoadConfig.destination_table.split("_", 1)[-1]
        test_target_dir = "s3://{lake_bucket}/{destination_environment}/{system}/{table}/data/".format(
            lake_bucket=self.default_dev_lake_bucket,
            destination_environment=AppendLoadConfig.destination_environment,
            system=source_system,
            table=table
        )

        config = AppendLoadConfig(
            self.local_run_dir,
            self.env_setup,
            target_partitions,
            regex_filename,
            file_format="parquet",
            metadata_update_strategy="SparkRecoverPartitionsNative"
        )
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        config.load_table(self.emr_cluster_id, spark_external_parameters)

        # Check EMR steps
        assert len(fake_cluster.steps) == 1

        # Check args of spark-submit EMR step
        spark_step = fake_cluster.steps[0]

        assert spark_step.jar == "command-runner.jar"
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == config.expected_algorithms_jar_path
        assert spark_step.args[-3] == config.load_type
        assert spark_step.args[-2] == config.config_filepath
        assert spark_step.args[-1] == "s3"

        # Check that config_file_s3 file is on application S3 bucket
        app_files = self.get_child_objects(config.dataset.dir_apps_append_load)
        app_json_files = list(filter(lambda app_file: os.path.basename(app_file).endswith(".json"), app_files))
        assert len(app_json_files) == 1
        assert app_json_files[0] == config.config_filepath

        # Check config file for Spark
        actual_parameters = json.loads(self.get_object_content_from_s3(config.config_filepath))
        expected_table_full_name = "{}.{}".format(config.db_name_lake, config.destination_table)
        expected_parameters = {
            "target_table": expected_table_full_name,
            "source_dir": config.dataset.dir_landing_final,
            "header_dir": config.dataset.dir_landing_header,
            "delimiter": "|",
            "has_header": False,
            "target_partitions": target_partitions,
            "regex_filename": regex_filename,
            "metadata_update_strategy": "SparkRecoverPartitionsNative",
            "file_format": "parquet",
            "target_dir": test_target_dir
        }
        assert actual_parameters == expected_parameters

        remove_json_patch.assert_called_once()
        assert remove_json_patch.call_args_list[0][0][0] == app_files[0]

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    def test_load_table_append_external_spark_parameters(self, _0, _1):

        target_partitions = ["year", "month", "day"]
        regex_filename = ["[0-9]{4}", "(?<=[0-9]{4})([0-9]{2})(?=[0-9]{2})", "(?<=[0-9]{6})([0-9]{2})"]

        spark_external_parameters = {
            "spark.driver.memory": "99G",
            "spark.executor.instances": "99",
            "spark.executor.memory": "90G"
        }

        config = AppendLoadConfig(self.local_run_dir, self.env_setup, target_partitions, regex_filename)
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        config.load_table(self.emr_cluster_id, json.dumps(spark_external_parameters))

        # Check EMR step.
        assert len(fake_cluster.steps) == 1

        spark_step = fake_cluster.steps[0]

        # Check args of EMR step
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[5] == "--conf"
        assert spark_step.args[7] == "--conf"
        assert spark_step.args[9] == "--conf"

        expected_spark_conf_options = set(map(lambda p: "{}={}".format(p[0], p[1]), spark_external_parameters.items()))
        actual_spark_conf_options = set(map(lambda x: spark_step.args[x], [6, 8, 10]))
        assert expected_spark_conf_options == actual_spark_conf_options

        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == config.expected_algorithms_jar_path
        assert spark_step.args[-3] == "AppendLoad"
        assert spark_step.args[-2] == config.config_filepath
        assert spark_step.args[-1] == "s3"

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    def test_load_table_append_invalid_parameters1(self, _0, _1):

        spark_external_parameters = '''
        {
            "spark.driver.memory": "99G",
            "spark.executor.instances": "99",
            "spark.executor.memory": "90G"
        }
        '''

        config = AppendLoadConfig(
            self.local_run_dir,
            self.env_setup,
            ["year", "month"],
            ["[0-9]{4}", "(?<=[0-9]{4})([0-9]{2})(?=[0-9]{2})", "(?<=[0-9]{6})([0-9]{2})"]
        )
        with pytest.raises(M3DIllegalArgumentException) as ex:
            config.load_table(self.emr_cluster_id, spark_external_parameters)

        assert str(ex.value).startswith("Lengths of target_partitions and regex_filename do not match")

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    def test_load_table_append_invalid_parameters2(self, _0, _1):

        spark_external_parameters = '''
        {
            "spark.driver.memory": "99G",
            "spark.executor.instances": "99",
            "spark.executor.memory": "90G"
        }
        '''

        config = AppendLoadConfig(self.local_run_dir, self.env_setup, ["year", "month", "day"], [])
        with pytest.raises(M3DIllegalArgumentException) as ex:
            config.load_table(self.emr_cluster_id, spark_external_parameters)

        assert str(ex.value).startswith("Lengths of target_partitions and regex_filename do not match")

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    @patch("m3d.hadoop.core.spark_executor.SparkExecutor._remove_parameter_json")
    def test_load_table_append_valid_parameters_semistructured_data(self, _0, _1, _2, _3):

        table = AppendLoadConfig.destination_table.split("_", 1)[-1]
        target_partitions = ["year", "month", "day"]
        regex_filename = ["[0-9]{4}", "(?<=[0-9]{4})([0-9]{2})(?=[0-9]{2})", "(?<=[0-9]{6})([0-9]{2})"]
        test_target_dir = "s3://{lake_bucket}/{destination_environment}/{system}/{table}/data/".format(
            lake_bucket=self.default_dev_lake_bucket,
            destination_environment=AppendLoadConfig.destination_environment,
            system=AppendLoadConfig.source_system,
            table=table
        )
        spark_external_parameters = {
                            "spark.driver.memory": "99G",
                            "spark.executor.instances": "99",
                            "spark.executor.memory": "90G"
                        }

        null_value = "test_null_value"
        quote_character = "test_quote"
        data_type = DataType.SEMISTRUCTURED
        verify_schema = True
        schema = {
            "type": "struct",
            "fields": [{
                "name": "first_name",
                "type": "string",
                "nullable": True,
                "metadata": {}
            }, {
                "name": "surname",
                "type": "string",
                "nullable": True,
                "metadata": {}
            }, {
                "name": "age",
                "type": "integer",
                "nullable": True,
                "metadata": {}
            }]
        }

        config = AppendLoadConfig(
            self.local_run_dir,
            self.env_setup,
            target_partitions,
            regex_filename,
            null_value=null_value,
            quote_character=quote_character,
            schema=schema,
            verify_schema=verify_schema,
            data_type=data_type
        )

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        config.load_table(self.emr_cluster_id, json.dumps(spark_external_parameters))

        # Check EMR step.
        assert len(fake_cluster.steps) == 1

        spark_step = fake_cluster.steps[0]

        # Check args of EMR step
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[5] == "--conf"
        assert spark_step.args[7] == "--conf"
        assert spark_step.args[9] == "--conf"

        expected_spark_conf_options = set(map(lambda p: "{}={}".format(p[0], p[1]), spark_external_parameters.items()))
        actual_spark_conf_options = set(map(lambda x: spark_step.args[x], [6, 8, 10]))
        assert expected_spark_conf_options == actual_spark_conf_options

        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == config.expected_algorithms_jar_path
        assert spark_step.args[-3] == "AppendLoad"
        assert spark_step.args[-2] == config.config_filepath
        assert spark_step.args[-1] == "s3"

        # Check that config_file_s3 file is on application S3 bucket
        app_files = self.get_child_objects(config.dataset.dir_apps_append_load)
        app_json_files = list(filter(lambda app_file: os.path.basename(app_file).endswith(".json"), app_files))
        assert len(app_json_files) == 1
        assert app_json_files[0] == config.config_filepath

        # Check config file for Spark
        actual_parameters = json.loads(self.get_object_content_from_s3(config.config_filepath))
        expected_parameters = {
            "target_table": "test101",
            "source_dir": config.dataset.dir_landing_final,
            "header_dir": config.dataset.dir_landing_header,
            "target_partitions": target_partitions,
            "regex_filename": regex_filename,
            "file_format": "dsv",
            "null_value": "test_null_value",
            "quote_character": "test_quote",
            "data_type": DataType.SEMISTRUCTURED,
            "verify_schema": True,
            "target_dir": test_target_dir,
            "schema": schema
        }
        assert actual_parameters == expected_parameters


class AppendLoadConfig:

    destination_system = "bdp"
    destination_database = "emr_test"
    destination_environment = "dev"
    source_system = "bi"
    destination_table = "bi_test101"

    load_type = "AppendLoad"

    def __init__(
            self,
            test_run_dir,
            setup_function,
            target_partitions,
            regex_filename,
            file_format=None,
            null_value=None,
            quote_character=None,
            compute_table_statistics=None,
            schema=None,
            verify_schema=None,
            data_type=None,
            reader_mode=None,
            metadata_update_strategy=None
    ):

        destination_params = [
            self.destination_system,
            self.destination_database,
            self.destination_environment,
            self.destination_table
        ]
        self.config_file, _, self.tconx_file, self.config_dict, self.scon_emr_dict = setup_function(
            *([test_run_dir] + destination_params)
        )

        self._write_acon(
            target_partitions,
            regex_filename,
            file_format=file_format,
            null_value=null_value,
            quote_character=quote_character,
            compute_table_statistics=compute_table_statistics,
            schema=schema,
            verify_schema=verify_schema,
            data_type=data_type,
            reader_mode=reader_mode,
            metadata_update_strategy=metadata_update_strategy
        )
        self._write_tconx()

        self.table_config = [self.config_file] + destination_params
        emr_system = EMRSystem(
            self.config_file,
            self.destination_system,
            self.destination_database,
            self.destination_environment
        )

        # self.s3_table = S3Table(emr_system, self.destination_table)
        if data_type is None:
            data_type = DataType.STRUCTURED

        self.dataset = DataSetFactory.create_dataset(
            emr_system,
            HiveTable.TableLoadType.APPEND,
            data_type,
            self.destination_table
        )

        config_filename = "append_load-{}-{}.json".format(self.destination_environment, self.dataset.table_lake)
        self.config_filepath = os.path.join(self.dataset.dir_apps_append_load, config_filename)
        self.db_name_lake = self.scon_emr_dict["environments"][self.destination_environment]["schemas"]["lake"]

        self.expected_algorithms_jar_path = "s3://" + os.path.join(
            (self.scon_emr_dict["environments"][self.destination_environment]["s3_buckets"]["application"]).strip("/"),
            (self.scon_emr_dict["environments"][self.destination_environment]["s3_deployment_dir_base"]).strip("/"),
            self.destination_environment,
            self.scon_emr_dict["subdir"]["m3d"],
            self.config_dict["subdir_projects"]["m3d_api"],
            self.scon_emr_dict["spark"]["jar_name"]
        )

    def _write_acon(
            self,
            target_partitions,
            regex_filename,
            file_format=None,
            null_value=None,
            quote_character=None,
            compute_table_statistics=None,
            schema=None,
            verify_schema=None,
            data_type=None,
            reader_mode=None,
            metadata_update_strategy=None
    ):
        logging.info("Creating acon file.")
        acon_dict = {
            "parameters": {
                "db_table_lake": self.destination_table,
                "target_partitions": target_partitions,
                "regex_filename": regex_filename,
                "metadata_update_strategy": metadata_update_strategy
            }
        }
        if file_format is not None:
            acon_dict["parameters"]["file_format"] = file_format
        if null_value is not None:
            acon_dict["parameters"]["null_value"] = null_value
        if quote_character is not None:
            acon_dict["parameters"]["quote_character"] = quote_character
        if compute_table_statistics is not None:
            acon_dict["parameters"]["compute_table_statistics"] = compute_table_statistics
        if schema is not None:
            acon_dict["parameters"]["schema"] = schema
        if verify_schema is not None:
            acon_dict["parameters"]["verify_schema"] = verify_schema
        if data_type is not None:
            acon_dict["parameters"]["data_type"] = data_type
        if reader_mode is not None:
            acon_dict["parameters"]["reader_mode"] = reader_mode

        AconHelper.setup_acon_from_dict(
            self.config_dict["tags"]["config"],
            self.destination_database,
            self.destination_environment,
            self.destination_table,
            acon_dict
        )

    def _write_tconx(self):
        logging.info("Creating tconx-file {}".format(self.tconx_file))
        tconx_resource_path = "test/resources/test_load_table_append_s3/tconx-bdp-emr_test-dev-bi_test101.json"
        tconx_content = py.path.local(tconx_resource_path).read()
        py.path.local(self.tconx_file).write(tconx_content)

    def load_table(self, emr_cluster_id, spark_parameters=None):
        if spark_parameters is None:
            M3D.load_table(*(self.table_config + [self.load_type, emr_cluster_id]))
        else:
            M3D.load_table(*(self.table_config + [self.load_type, emr_cluster_id, spark_parameters]))
