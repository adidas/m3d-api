import json
import os
import mock
import py
import pytest
from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.hadoop.emr.s3_table import S3Table
from test.core.acon_helper import AconHelper
from test.core.s3_table_test_base import S3TableTestBase


class TestLoadTableFullS3(S3TableTestBase):

    @pytest.mark.emr
    @mock.patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_full_load_emr(self, _0, _1):

        tconx_src_path = \
            "test/resources/test_create_out_view_hive/test_empty_table_lakeout/config/empty_tabl_cd_lakeout.json"

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        load_type = "FullLoad"
        landing_dataset = "landing-dataset.psv"

        spark_external_parameters = '''{
                    "spark.driver.memory": "99G",
                    "spark.executor.instances": "99",
                    "spark.executor.memory": "90G"
                }
                '''

        m3d_config_file, scon_emr_file, tconx_file, m3d_config_dict, scon_emr_dict = \
            super(TestLoadTableFullS3, self).env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        py.path.local(tconx_file).write(py.path.local(tconx_src_path).read())

        table_config = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            load_type,
            self.emr_cluster_id,
            spark_external_parameters
        ]

        # Extract bucket names
        bucket_application = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["application"]
        emr_system = EMRSystem(
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment
        )
        test_s3_table = S3Table(emr_system, destination_table)

        # Put landing data
        self.dump_data_to_s3(
            os.path.join(test_s3_table.dir_landing_final, landing_dataset),
            "t|e|s|t|a|d|i|d|a|s|m|3|d|"
        )

        M3D.load_table(*table_config)

        # Since we have offloaded data move operations to EMR Steps dir_landing_final will still have
        # old files in it and dir_landing_archive will not have new files
        landing_files = self.get_child_objects(test_s3_table.dir_landing_final)
        assert len(landing_files) == 1
        assert landing_files[0] == os.path.join(test_s3_table.dir_landing_final, landing_dataset)

        landing_archive_files = self.get_child_objects(test_s3_table.dir_landing_archive)
        assert len(landing_archive_files) == 0

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

        # Check args of spark-submit EMR step
        spark_step = fake_cluster.steps[0]

        assert spark_step.jar == "command-runner.jar"
        assert spark_step.args[0] == "spark-submit"

        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == expected_algorithms_jar_path
        assert spark_step.args[-3] == "FullLoad"
        assert spark_step.args[-2] == "s3://adidas-dev-application/m3d/dev/apps/loading/bdp/test101/" \
                                      "full_load/full_load-dev-bi_test101.json"
        assert spark_step.args[-1] == "s3"

    @pytest.mark.emr
    @mock.patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    def test_full_load_emr_external_spark_parameters(self, _0):
        # responses.add_passthru(self.default_server_url)

        tconx_src_path = \
            "test/resources/test_create_out_view_hive/test_empty_table_lakeout/config/empty_tabl_cd_lakeout.json"
        acon_src_path = "test/resources/test_load_table_full_s3/acon-emr_test-bi_test101.json"

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        spark_external_parameters = {
            "spark.driver.memory": "99G",
            "spark.executor.instances": "99",
            "spark.executor.memory": "90G"
        }

        load_type = "FullLoad"
        landing_dataset = "landing-dataset.psv"

        m3d_config_file, scon_emr_file, tconx_file, m3d_config_dict, scon_emr_dict = \
            super(TestLoadTableFullS3, self).env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )
        AconHelper.setup_acon_from_file(
            m3d_config_dict["tags"]["config"],
            destination_database,
            destination_environment,
            destination_table,
            acon_src_path
        )

        py.path.local(tconx_file).write(py.path.local(tconx_src_path).read())

        table_config = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            load_type,
            self.emr_cluster_id
        ]

        # Extract bucket names
        bucket_application = scon_emr_dict["environments"][destination_environment]["s3_buckets"]["application"]

        emr_system = EMRSystem(
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment
        )
        test_s3_table = S3Table(emr_system, destination_table)

        # Put landing data
        self.dump_data_to_s3(
            os.path.join(test_s3_table.dir_landing_final, landing_dataset),
            "t|e|s|t|a|d|i|d|a|s|m|3|d|"
        )

        M3D.load_table(*table_config, spark_params=json.dumps(spark_external_parameters))

        # psv file will still be in landing since move operation should be
        # performed by EMR Step which we mock here. Accordingly archive will
        # still be empty.
        landing_files = self.get_child_objects(test_s3_table.dir_landing_final)
        assert len(landing_files) == 1
        assert landing_files[0] == os.path.join(test_s3_table.dir_landing_final, landing_dataset)

        landing_archive_files = self.get_child_objects(test_s3_table.dir_landing_archive)
        assert len(landing_archive_files) == 0

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

        spark_step = fake_cluster.steps[0]

        assert spark_step.jar == "command-runner.jar"
        assert spark_step.args[0] == "spark-submit"
        assert spark_step.args[5] == "--conf"
        assert spark_step.args[7] == "--conf"
        assert spark_step.args[9] == "--conf"

        expected_spark_conf_options = set(map(lambda p: "{}={}".format(p[0], p[1]), spark_external_parameters.items()))
        actual_spark_conf_options = set(map(lambda x: spark_step.args[x], [6, 8, 10]))
        assert expected_spark_conf_options == actual_spark_conf_options

        assert spark_step.args[-5] == "com.adidas.analytics.AlgorithmFactory"
        assert spark_step.args[-4] == expected_algorithms_jar_path
        assert spark_step.args[-3] == "FullLoad"
        assert spark_step.args[-2] == "s3://adidas-dev-application/m3d/dev/apps/loading/bdp/test101/" \
                                      "full_load/full_load-dev-bi_test101.json"
        assert spark_step.args[-1] == "s3"
