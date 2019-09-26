import pytest
from mock import patch

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.algorithm.algorithm_configuration_hadoop import AlgorithmConfigurationHadoop
from m3d.hadoop.algorithm.algorithm_fixed_length_string_extractor import AlgorithmFixedLengthStringExtractor
from m3d.hadoop.emr.emr_system import EMRSystem
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase


class TestAlgorithmFixedLengthStringExtractor(EMRSystemUnitTestBase):

    SOURCE_TABLE = "test_src_table"
    TARGET_TABLE = "test_tgt_table"
    SOURCE_FIELD = "main_value"
    PARTITION_COLUMNS = ["year", "month"]
    SELECT_CONDITIONS = ["year=2019", "month=2"]
    SELECT_RULES = ["month-1"]
    SUBSTRING_POSITIONS = ["10,15", "20, 22"]

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_with_conditions(self, add_tags_patch):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmFixedLengthStringExtractor(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_full_source_table = self._create_full_table_name(self.SOURCE_TABLE)
        expected_full_target_table = self._create_full_table_name(self.TARGET_TABLE)
        expected_params = {
            "source_table": expected_full_source_table,
            "target_table": expected_full_target_table,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceTable", "Value": expected_full_source_table},
            {"Key": "TargetTable", "Value": expected_full_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_with_rules(self, add_tags_patch):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_rules": self.SELECT_RULES,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmFixedLengthStringExtractor(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_full_source_table = self._create_full_table_name(self.SOURCE_TABLE)
        expected_full_target_table = self._create_full_table_name(self.TARGET_TABLE)
        expected_params = {
            "source_table": expected_full_source_table,
            "target_table": expected_full_target_table,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_rules": self.SELECT_RULES,
            "substring_positions": self.SUBSTRING_POSITIONS
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceTable", "Value": expected_full_source_table},
            {"Key": "TargetTable", "Value": expected_full_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_empty_non_required_fields(self, add_tags_patch):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmFixedLengthStringExtractor(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_full_source_table = self._create_full_table_name(self.SOURCE_TABLE)
        expected_full_target_table = self._create_full_table_name(self.TARGET_TABLE)
        expected_params = {
            "source_table": expected_full_source_table,
            "target_table": expected_full_target_table,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceTable", "Value": expected_full_source_table},
            {"Key": "TargetTable", "Value": expected_full_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_missing_non_required_fields(self, add_tags_patch):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)
        algorithm = AlgorithmFixedLengthStringExtractor(
            emr_system,
            configuration.get_algorithm_instance(),
            configuration.get_algorithm_params()
        )

        generated_params = algorithm.build_params()

        expected_full_source_table = self._create_full_table_name(self.SOURCE_TABLE)
        expected_full_target_table = self._create_full_table_name(self.TARGET_TABLE)
        expected_params = {
            "source_table": expected_full_source_table,
            "target_table": expected_full_target_table,
            "source_field": self.SOURCE_FIELD,
            "substring_positions": self.SUBSTRING_POSITIONS
        }
        assert generated_params == expected_params

        add_tags_patch.assert_called_once()
        add_tags_patch_call_args, _ = add_tags_patch.call_args
        assert sorted(add_tags_patch_call_args[0], key=lambda x: x["Key"]) == sorted([
            {"Key": "SourceTable", "Value": expected_full_source_table},
            {"Key": "TargetTable", "Value": expected_full_target_table}
        ], key=lambda x: x["Key"])

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_missing_source_table(self, _):
        acon_dict = self._create_acon_dict({
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Source table name is missing in the acon-file")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_missing_target_table(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Target table name is missing in the acon-file")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_missing_source_field(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Source field name is missing in the acon-file")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_missing_substring_positions(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Substring positions specification is missing in the acon-file")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_with_rules_and_conditions(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "partition_columns": self.PARTITION_COLUMNS,
            "select_conditions": self.SELECT_CONDITIONS,
            "select_rules": self.SELECT_RULES,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Unable to use both select_conditions and select_rules at the same time")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_with_rules_unpartitioned(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "select_rules": self.SELECT_RULES,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Unable to use select_rules for unpartitioned table")

    @pytest.mark.algo
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_build_params_with_conditions_unpartitioned(self, _):
        acon_dict = self._create_acon_dict({
            "source_table": self.SOURCE_TABLE,
            "target_table": self.TARGET_TABLE,
            "source_field": self.SOURCE_FIELD,
            "select_conditions": self.SELECT_CONDITIONS,
            "substring_positions": self.SUBSTRING_POSITIONS
        })

        emr_system = self._create_emr_system()
        configuration = self._create_algorithm_configuration(acon_dict)

        with pytest.raises(M3DIllegalArgumentException) as ex_info:
            AlgorithmFixedLengthStringExtractor(
                emr_system,
                configuration.get_algorithm_instance(),
                configuration.get_algorithm_params()
            )

        assert str(ex_info.value).startswith("Unable to use select_conditions for unpartitioned table")

    def _create_emr_system(self):
        cluster_mode = False
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
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            self.emr_cluster_id
        )

    @staticmethod
    def _create_acon_dict(parameters):
        return {
            "environment": {
                "emr_cluster_id": "j-D1LSS423N",
            },
            "algorithm": {
                "python_class": "AlgorithmFixedLengthStringExtractor",
                "parameters": parameters
            }
        }

    @staticmethod
    def _create_algorithm_configuration(acon_dict):
        return AlgorithmConfigurationHadoop("partition_materialization", acon_dict)

    @staticmethod
    def _create_full_table_name(table_name):
        return "lake.{}".format(table_name)
