import json
import os

import py
import pytest
# import responses
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.hadoop.emr.s3_table import S3Table
from m3d.util.util import Util
from test.core.s3_table_test_base import S3TableTestBase


class TestLoadTableDeltaS3(S3TableTestBase):

    def env_setup(
            self,
            tmpdir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            tconx_content=None,
            tconx_cl_content=None
    ):
        m3d_config_file, scon_emr_file, tconx_file, m3d_config_dict, scon_emr_dict = \
            super(
                TestLoadTableDeltaS3,
                self
            ).env_setup(
                tmpdir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        tconx_filename_template = "tconx-{source_system}-{db_cd}-{environment}-{table}.json"

        tconx_cl_filename = tconx_filename_template.format(
            source_system=destination_system,
            db_cd=destination_database,
            environment=destination_environment,
            table=destination_table + "_cl"
        )

        tconx_cl_file = os.path.join(os.path.dirname(tconx_file), tconx_cl_filename)

        if tconx_content:
            py.path.local(tconx_file).write(tconx_content)

        if tconx_cl_content:
            py.path.local(tconx_cl_file).write(tconx_cl_content)

        return m3d_config_file, scon_emr_file, tconx_file, tconx_cl_file, \
            m3d_config_dict, scon_emr_dict

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    @patch("m3d.hadoop.core.spark_executor.SparkExecutor._remove_parameter_json")
    def test_load_table_delta(self, remove_json_patch, add_tags_patch, _0, _1):
        # responses.add_passthru(self.default_server_url)

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_active_table = "bi_test101"
        destination_changelog_table = "bi_test101_cl"

        load_type = "DeltaLoad"

        src_tconx_path = "test/resources/test_load_table_delta_s3/tconx-bdp-emr_test-dev-bi_test101.json"
        src_tconx_cl_table = "test/resources/test_load_table_delta_s3/tconx-bdp-emr_test-dev-bi_test101_cl.json"

        spark_external_parameters = '''{
                    "spark.driver.memory": "99G",
                    "spark.executor.instances": "99",
                    "spark.executor.memory": "90G"
                }
                '''

        # pass desired content of tconx files for active and changelog tables to self.env_setup()
        src_tconx_content = py.path.local(src_tconx_path).read()
        src_tconx_cl_content = py.path.local(src_tconx_cl_table).read()

        m3d_config_file, scon_emr_file, tconx_file, tconx_cl_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_active_table,
                src_tconx_content,
                src_tconx_cl_content
            )

        emr_system = EMRSystem(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment
        )
        s3_table_active = S3Table(emr_system, destination_active_table)
        s3_table_changelog = S3Table(emr_system, destination_changelog_table)

        # Extract bucket names
        bucket_application = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["application"]

        # Put lake data for changelog table, this should be archived
        self.dump_data_to_s3(
            os.path.join(s3_table_changelog.dir_lake_final, "changelog.parquet"),
            "t|e|s|t|a|d|i|d|a|s|m|3|d|",
        )

        M3D.load_table(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_active_table,
            load_type,
            self.emr_cluster_id,
            spark_params=spark_external_parameters
        )

        filename_json = "delta_load-{environment}-{table}.json".format(
            environment=destination_environment,
            table=destination_active_table
        )

        # Checking configuration file for m3d-engine
        app_files = self.get_child_objects(s3_table_active.dir_apps_delta_load)

        assert len(app_files) == 1

        assert app_files[0] == s3_table_active.dir_apps_delta_load + filename_json

        delta_load_config_s3 = app_files[0]
        delta_load_config_content = self.get_object_content_from_s3(delta_load_config_s3)

        load_table_parameters = json.loads(delta_load_config_content)

        assert load_table_parameters["active_records_table_lake"] == s3_table_active.db_table_lake
        assert load_table_parameters["active_records_dir_lake"] == s3_table_active.dir_lake_final
        assert load_table_parameters["delta_records_file_path"] == s3_table_active.dir_landing_data
        assert load_table_parameters["technical_key"] == ["m3d_timestamp", "datapakid", "partno", "record"]
        assert load_table_parameters["business_key"] == s3_table_active.business_key

        if s3_table_active.partitioned_by in Util.defined_partitions:
            target_partitions = Util.get_target_partitions_list(s3_table_active.partitioned_by)
        else:
            target_partitions = s3_table_active.partitioned_by

        assert load_table_parameters["target_partitions"] == target_partitions
        assert load_table_parameters["partition_column"] == s3_table_active.partition_column
        assert load_table_parameters["partition_column_format"] == s3_table_active.partition_column_format

        # Check EMR steps.
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        assert 1 == len(fake_cluster.steps)

        expected_algorithms_jar_path = "s3://" + bucket_application + os.path.join(
            scon_emr_dict["environments"][destination_environment]["s3_deployment_dir_base"],
            destination_environment,
            scon_emr_dict["subdir"]["m3d"],
            m3d_config_dict["subdir_projects"]["m3d_api"],
            scon_emr_dict["spark"]["jar_name"]
        )

        delta_load_step = fake_cluster.steps[0]

        assert delta_load_step.jar == "command-runner.jar"
        assert delta_load_step.args[0] == "spark-submit"

        assert delta_load_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert delta_load_step.args[-4] == expected_algorithms_jar_path
        assert delta_load_step.args[-3] == "DeltaLoad"
        assert delta_load_step.args[-2] == delta_load_config_s3
        assert delta_load_step.args[-1] == "s3"

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 1
        assert sorted(add_tags_patch_call_args_list[0][0][0], key=lambda x: x["Key"]) == sorted([
            {"Key": "ApiMethod", "Value": "load_table"},
            {"Key": "LoadType", "Value": "DeltaLoad"},
            {"Key": "TargetTable", "Value": "bi_test101"}
        ], key=lambda x: x["Key"])

        remove_json_patch.assert_called_once()
        assert remove_json_patch.call_args_list[0][0][0] == app_files[0]

    @pytest.mark.emr
    @patch("m3d.util.util.Util.send_email")
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.core.spark_executor.SparkExecutor._remove_parameter_json")
    def test_load_table_delta_external_spark_parameters(self, remove_json_patch, _0, _1):
        # responses.add_passthru(self.default_server_url)

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_active_table = "bi_test101"
        destination_changelog_table = "bi_test101_cl"

        spark_external_parameters = {
            "spark.driver.memory": "99G",
            "spark.executor.instances": "99",
            "spark.executor.memory": "90G"
        }

        load_type = "DeltaLoad"

        src_tconx_path = "test/resources/test_load_table_delta_s3/tconx-bdp-emr_test-dev-bi_test101.json"
        src_tconx_cl_table = "test/resources/test_load_table_delta_s3/tconx-bdp-emr_test-dev-bi_test101_cl.json"

        # pass desired content of tconx files for active and changelog tables to self.env_setup()
        src_tconx_content = py.path.local(src_tconx_path).read()
        src_tconx_cl_content = py.path.local(src_tconx_cl_table).read()

        m3d_config_file, scon_emr_file, tconx_file, tconx_cl_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_active_table,
                src_tconx_content,
                src_tconx_cl_content
            )

        emr_system = EMRSystem(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment
        )
        s3_table_active = S3Table(emr_system, destination_active_table)
        s3_table_changelog = S3Table(emr_system, destination_changelog_table)

        # Extract bucket names
        bucket_application = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["application"]

        # Put lake data for changelog table, this should be archived
        self.dump_data_to_s3(
            os.path.join(s3_table_changelog.dir_lake_final, "changelog.parquet"),
            "t|e|s|t|a|d|i|d|a|s|m|3|d|",
        )

        M3D.load_table(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_active_table,
            load_type,
            self.emr_cluster_id,
            spark_params=json.dumps(spark_external_parameters)
        )

        filename_json = "delta_load-{environment}-{table}.json".format(
            environment=destination_environment,
            table=destination_active_table
        )

        # Checking configuration file for m3d-engine
        app_files = self.get_child_objects(s3_table_active.dir_apps_delta_load)

        assert len(app_files) == 1

        assert app_files[0] == s3_table_active.dir_apps_delta_load + filename_json

        delta_load_config_s3 = app_files[0]
        delta_load_config_content = self.get_object_content_from_s3(delta_load_config_s3)

        load_table_parameters = json.loads(delta_load_config_content)

        assert load_table_parameters["active_records_table_lake"] == s3_table_active.db_table_lake
        assert load_table_parameters["active_records_dir_lake"] == s3_table_active.dir_lake_final
        assert load_table_parameters["delta_records_file_path"] == s3_table_active.dir_landing_data
        assert load_table_parameters["technical_key"] == ["m3d_timestamp", "datapakid", "partno", "record"]
        assert load_table_parameters["business_key"] == s3_table_active.business_key

        if s3_table_active.partitioned_by in Util.defined_partitions:
            target_partitions = Util.get_target_partitions_list(s3_table_active.partitioned_by)
        else:
            target_partitions = s3_table_active.partitioned_by

        assert load_table_parameters["target_partitions"] == target_partitions
        assert load_table_parameters["partition_column"] == s3_table_active.partition_column
        assert load_table_parameters["partition_column_format"] == s3_table_active.partition_column_format
        # Check EMR steps.
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        assert 1 == len(fake_cluster.steps)

        expected_algorithms_jar_path = "s3://" + bucket_application + os.path.join(
            scon_emr_dict["environments"][destination_environment]["s3_deployment_dir_base"],
            destination_environment,
            scon_emr_dict["subdir"]["m3d"],
            m3d_config_dict["subdir_projects"]["m3d_api"],
            scon_emr_dict["spark"]["jar_name"]
        )

        delta_load_step = fake_cluster.steps[0]

        assert delta_load_step.jar == "command-runner.jar"
        assert delta_load_step.args[0] == "spark-submit"

        assert delta_load_step.args[5] == "--conf"
        assert delta_load_step.args[7] == "--conf"
        assert delta_load_step.args[9] == "--conf"

        expected_spark_conf_options = set(map(lambda p: "{}={}".format(p[0], p[1]), spark_external_parameters.items()))
        actual_spark_conf_options = set(map(lambda x: delta_load_step.args[x], [6, 8, 10]))
        assert expected_spark_conf_options == actual_spark_conf_options

        assert delta_load_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert delta_load_step.args[-4] == expected_algorithms_jar_path
        assert delta_load_step.args[-3] == "DeltaLoad"
        assert delta_load_step.args[-2] == delta_load_config_s3
        assert delta_load_step.args[-1] == "s3"

        remove_json_patch.assert_called_once()
        assert remove_json_patch.call_args_list[0][0][0] == app_files[0]
