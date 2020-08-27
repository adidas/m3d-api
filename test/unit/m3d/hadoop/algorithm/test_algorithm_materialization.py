import pytest
from mock import patch

from m3d.hadoop.algorithm.algorithm_configuration_hadoop import AlgorithmConfigurationHadoop
from m3d.hadoop.algorithm.algorithm_materialization import AlgorithmMaterialization
from m3d.hadoop.emr.emr_system import EMRSystem
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase


class TestAlgorithmMaterialization(EMRSystemUnitTestBase):

    KEYS = AlgorithmMaterialization.BaseMaterialization.ConfigKeys

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_full_materialization_params(self, add_tags_patch):
        view_name = "par_mat_test_view"
        target_partitions = ["year", "month"]
        metadata_update_strategy = "SparkRecoverPartitionsCustom"
        base_data_dir = "data/"
        num_versions_to_retain = 2

        acon_dict = {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
            },
            "algorithm": {
                "python_class": "FullMaterialization",
                "parameters": {
                    "view": view_name,
                    "target_partitions": target_partitions,
                    "metadata_update_strategy": metadata_update_strategy,
                    "base_data_dir": base_data_dir,
                    "num_versions_to_retain": num_versions_to_retain
                }
            }
        }

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmMaterialization.FullMaterialization(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_source_view = self._create_full_source_view_name(view_name)
        expected_target_table = self._create_full_target_table_name(view_name)
        expected_params = {
            self.KEYS.SOURCE_TABLE: expected_source_view,
            self.KEYS.TARGET_TABLE: expected_target_table,
            self.KEYS.TARGET_PARTITIONS: target_partitions,
            self.KEYS.METADATA_UPDATE_STRATEGY: metadata_update_strategy,
            self.KEYS.BASE_DATA_DIR: base_data_dir,
            self.KEYS.NUM_VERSIONS_TO_RETAIN: num_versions_to_retain
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceView", "Value": expected_source_view},
            {"Key": "TargetTable", "Value": expected_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_full_materialization_params_with_number_of_output_partitions(self, add_tags_patch):
        view_name = "par_mat_test_view"
        target_partitions = ["year"]
        output_partitions_num = 5
        base_data_dir = "data/"
        num_versions_to_retain = 2

        acon_dict = {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
                "stack_name": "test_stack_name"
            },
            "algorithm": {
                "python_class": "FullMaterialization",
                "parameters": {
                    "view": view_name,
                    "target_partitions": target_partitions,
                    "number_output_partitions": output_partitions_num,
                    "base_data_dir": base_data_dir,
                    "num_versions_to_retain": num_versions_to_retain
                }
            }
        }

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmMaterialization.FullMaterialization(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_source_view = self._create_full_source_view_name(view_name)
        expected_target_table = self._create_full_target_table_name(view_name)
        expected_params = {
            self.KEYS.SOURCE_TABLE: expected_source_view,
            self.KEYS.TARGET_TABLE: expected_target_table,
            self.KEYS.TARGET_PARTITIONS: target_partitions,
            self.KEYS.NUMBER_OUTPUT_PARTITIONS: output_partitions_num,
            self.KEYS.BASE_DATA_DIR: base_data_dir,
            self.KEYS.NUM_VERSIONS_TO_RETAIN: num_versions_to_retain
        }

        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceView", "Value": expected_source_view},
            {"Key": "TargetTable", "Value": expected_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_query_materialization_params(self, add_tags_patch):
        view_name = "par_mat_test_view"
        target_partitions = ["year", "month", "day"]
        select_conditions = ["year=2017", "month=2", "day=15"]

        acon_dict = {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
            },
            "algorithm": {
                "python_class": "QueryMaterialization",
                "parameters": {
                    "view": view_name,
                    "target_partitions": target_partitions,
                    "select_conditions": select_conditions
                }
            }
        }

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmMaterialization.QueryMaterialization(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_source_view = self._create_full_source_view_name(view_name)
        expected_target_table = self._create_full_target_table_name(view_name)
        expected_params = {
            self.KEYS.SOURCE_TABLE: expected_source_view,
            self.KEYS.TARGET_TABLE: expected_target_table,
            self.KEYS.TARGET_PARTITIONS: target_partitions,
            self.KEYS.SELECT_CONDITIONS: select_conditions
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceView", "Value": expected_source_view},
            {"Key": "TargetTable", "Value": expected_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_range_materialization_params(self, add_tags_patch):
        view_name = "par_mat_test_view"
        target_partitions = ["year", "month", "day"]

        date_from = "2018-02-01"
        date_to = "2018-02-28"

        acon_dict = {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
            },
            "algorithm": {
                "python_class": "RangeMaterialization",
                "parameters": {
                    "view": view_name,
                    "target_partitions": target_partitions,
                    "date_from": date_from,
                    "date_to": date_to
                }
            }
        }

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmMaterialization.RangeMaterialization(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_source_view = self._create_full_source_view_name(view_name)
        expected_target_table = self._create_full_target_table_name(view_name)
        expected_params = {
            self.KEYS.SOURCE_TABLE: expected_source_view,
            self.KEYS.TARGET_TABLE: expected_target_table,
            self.KEYS.TARGET_PARTITIONS: target_partitions,
            self.KEYS.DATE_FROM: date_from,
            self.KEYS.DATE_TO: date_to
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceView", "Value": expected_source_view},
            {"Key": "TargetTable", "Value": expected_target_table}
        ], key=lambda x: x["Key"])

    def _create_emr_system(self):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "prod"

        m3d_config_file, _, _, _ = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment
        )
        return EMRSystem(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            self.emr_cluster_id
        )

    @staticmethod
    def _create_algorithm_configuration(acon_dict):
        return AlgorithmConfigurationHadoop("materialization", acon_dict)

    @staticmethod
    def _create_full_source_view_name(view_name):
        return TestAlgorithmMaterialization._build_full_table_name("mart_mod", view_name)

    @staticmethod
    def _create_full_target_table_name(view_name):
        return TestAlgorithmMaterialization._build_full_table_name("mart_cal", view_name)

    @staticmethod
    def _build_full_table_name(database, table):
        return "{}.{}".format(database, table)
