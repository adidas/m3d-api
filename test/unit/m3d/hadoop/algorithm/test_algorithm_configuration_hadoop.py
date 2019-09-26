import json

import pytest
from mock import patch

from m3d.hadoop.algorithm.algorithm_configuration_hadoop import AlgorithmConfigurationHadoop
from m3d.util import util

from test.core.unit_test_base import UnitTestBase


class TestAlgorithmConfigurationHadoop(UnitTestBase):

    default_m3d_config = "config/m3d/config.json"

    def env_setup(self):
        """
        This functions creates test specific config.json
        :return: m3d_config_path: paths of test-specific config.json. Should be passed to M3D API calls.

        """
        tmpdir = self.local_run_dir

        m3d_config_dict = util.Util.load_dict(self.default_m3d_config)
        tag_config = m3d_config_dict["tags"]["config"]

        config_dir = tmpdir.mkdir(tag_config)

        m3d_config_dict["tags"]["config"] = str(config_dir)
        m3d_config_dict["dir_exec"] = str(self.local_run_dir.mkdir("tmp"))
        m3d_config_file = config_dir.mkdir("m3d").join("config.json")
        m3d_config_file.write(json.dumps(m3d_config_dict, indent=4))

        return str(m3d_config_file)

    @pytest.mark.algo
    def test_constructor(self):
        test_python_class = "test_python_class"
        test_algorithm_instance = "test_instance_cd"

        acon_dict = {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
                "spark": {
                    "spark.executor.instances": "5",
                    "spark.executor.memory": "25G"
                }
            },
            "algorithm": {
                "python_class": test_python_class,
                "parameters": {}
            }
        }

        expected_algorithm_section = {
            "python_class": "test_python_class",
            "parameters": {}
        }

        algorithm_configuration = AlgorithmConfigurationHadoop(test_algorithm_instance, acon_dict)

        # Check python class
        assert test_python_class == algorithm_configuration.get_python_class()
        assert test_algorithm_instance == algorithm_configuration.get_algorithm_instance()
        assert expected_algorithm_section == algorithm_configuration.get_algorithm_params()

    @pytest.mark.algo
    def test_from_acon_file(self):
        test_python_class = "python_test_class"
        test_emr_cluster_id = "test_id"

        config = self.env_setup()

        test_dict = {
            "environment": {
                "emr_cluster_id": test_emr_cluster_id,
                "spark": {
                    "spark.executor.instances": "5",
                    "spark.executor.memory": "25G"
                }
            },
            "algorithm": {
                "python_class": test_python_class,
                "parameters": {}
            }
        }

        algorithms = {
            "python_class": test_python_class,
            "parameters": {}
        }

        spark_params = {
            "spark.executor.instances": "5",
            "spark.executor.memory": "25G"
        }

        with patch('m3d.util.util.Util.load_dict', return_value={}):
            with patch('m3d.util.util.Util.merge_nested_dicts', return_value=test_dict):
                algorithm_configuration = AlgorithmConfigurationHadoop.create_with_ext_params(
                    config,
                    False,
                    "bdp_test",
                    "test",
                    "gzip_decompressor_bytes",
                    """
                    {
                        "environment": {
                            "emr_cluster_id": "test_id"
                        }
                    }
                    """
                )

        assert algorithm_configuration.get_python_class() == test_python_class
        assert algorithm_configuration.get_spark_params() == spark_params
        assert algorithm_configuration.get_algorithm_params() == algorithms
