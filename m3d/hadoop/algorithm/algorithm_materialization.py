from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem


class AlgorithmMaterialization(object):
    class BaseMaterialization(AlgorithmHadoop):
        class ConfigKeys(object):
            VIEW = "view"
            SOURCE_TABLE = "source_table"
            TARGET_TABLE = "target_table"
            TARGET_PARTITIONS = "target_partitions"
            SELECT_CONDITIONS = "select_conditions"
            DATE_FROM = "date_from"
            DATE_TO = "date_to"
            NUMBER_OUTPUT_PARTITIONS = "number_output_partitions"
            METADATA_UPDATE_STRATEGY = "metadata_update_strategy"
            BASE_DATA_DIR = "base_data_dir"
            NUM_VERSIONS_TO_RETAIN = "num_versions_to_retain"

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Base Partition Materialization

            :param execution_system: an instance of EMRSystem object
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmMaterialization.BaseMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

            view_name = self._parameters[self.ConfigKeys.VIEW]
            self.target_partitions = self._parameters[self.ConfigKeys.TARGET_PARTITIONS]
            self.metadata_update_strategy = self._parameters.get(self.ConfigKeys.METADATA_UPDATE_STRATEGY, None)
            self.source_view = "{}.{}".format(execution_system.db_mart_mod, view_name)
            self.target_table = "{}.{}".format(execution_system.db_mart_cal, view_name)

            execution_system.add_cluster_tags({
                EMRSystem.EMRClusterTag.SOURCE_VIEW: self.source_view,
                EMRSystem.EMRClusterTag.TARGET_TABLE: self.target_table
            })

        def build_params(self):
            params = {
                self.ConfigKeys.SOURCE_TABLE: self.source_view,
                self.ConfigKeys.TARGET_TABLE: self.target_table,
                self.ConfigKeys.TARGET_PARTITIONS: self.target_partitions
            }

            # Optional Parameter(s)
            if self.metadata_update_strategy is not None:
                metadata_update = self._parameters[self.ConfigKeys.METADATA_UPDATE_STRATEGY]
                params.update({self.ConfigKeys.METADATA_UPDATE_STRATEGY: metadata_update})

            additional_parameters = self._get_additional_parameters()
            params.update(additional_parameters)

            if self.ConfigKeys.NUMBER_OUTPUT_PARTITIONS in self._parameters:
                partitions_num = self._parameters[self.ConfigKeys.NUMBER_OUTPUT_PARTITIONS]
                params.update({self.ConfigKeys.NUMBER_OUTPUT_PARTITIONS: partitions_num})

            return params

        def get_scala_class(self):
            raise NotImplementedError()

        def _get_additional_parameters(self):
            raise NotImplementedError()

    class FullMaterialization(BaseMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Full Partition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmMaterialization.FullMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

            self.num_versions_to_retain = self._parameters.get(self.ConfigKeys.NUM_VERSIONS_TO_RETAIN, 2)

        def get_scala_class(self):
            return ScalaClasses.FULL_MATERIALIZATION

        def _get_additional_parameters(self):
            return {
                self.ConfigKeys.BASE_DATA_DIR: self._execution_system.subdir_data,
                self.ConfigKeys.NUM_VERSIONS_TO_RETAIN: self.num_versions_to_retain
            }

    class RangeMaterialization(BaseMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Range Partition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmMaterialization.RangeMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

        def get_scala_class(self):
            return ScalaClasses.RANGE_MATERIALIZATION

        def _get_additional_parameters(self):
            date_from = self._parameters[self.ConfigKeys.DATE_FROM]
            date_to = self._parameters[self.ConfigKeys.DATE_TO]
            return {
                self.ConfigKeys.DATE_FROM: date_from,
                self.ConfigKeys.DATE_TO: date_to
            }

    class QueryMaterialization(BaseMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm QueryPartition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmMaterialization.QueryMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

        def get_scala_class(self):
            return ScalaClasses.QUERY_MATERIALIZATION

        def _get_additional_parameters(self):
            select_conditions = self._parameters[self.ConfigKeys.SELECT_CONDITIONS]
            return {
                self.ConfigKeys.SELECT_CONDITIONS: select_conditions
            }
