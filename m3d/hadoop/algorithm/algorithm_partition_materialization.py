from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem


class AlgorithmPartitionMaterialization(object):

    class BasePartitionMaterialization(AlgorithmHadoop):

        class ConfigKeys(object):
            VIEW = "view"
            SOURCE_TABLE = "source_table"
            TARGET_TABLE = "target_table"
            TARGET_PARTITIONS = "target_partitions"
            SELECT_CONDITIONS = "select_conditions"
            DATE_FROM = "date_from"
            DATE_TO = "date_to"
            NUMBER_OUTPUT_PARTITIONS = "number_output_partitions"

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Base Partition Materialization

            :param execution_system: an instance of EMRSystem object
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmPartitionMaterialization.BasePartitionMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

            view_name = self._parameters[self.ConfigKeys.VIEW]
            self.target_partitions = self._parameters[self.ConfigKeys.TARGET_PARTITIONS]
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

    class FullPartitionMaterialization(BasePartitionMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Full Partition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmPartitionMaterialization.FullPartitionMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

        def get_scala_class(self):
            return ScalaClasses.PARTITION_FULL_MATERIALIZATION

        def _get_additional_parameters(self):
            return {}

    class RangePartitionMaterialization(BasePartitionMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm Range Partition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmPartitionMaterialization.RangePartitionMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

        def get_scala_class(self):
            return ScalaClasses.PARTITION_RANGE_MATERIALIZATION

        def _get_additional_parameters(self):
            date_from = self._parameters[self.ConfigKeys.DATE_FROM]
            date_to = self._parameters[self.ConfigKeys.DATE_TO]
            return {
                self.ConfigKeys.DATE_FROM: date_from,
                self.ConfigKeys.DATE_TO: date_to
            }

    class QueryPartitionMaterialization(BasePartitionMaterialization):

        def __init__(self, execution_system, algorithm_instance, algorithm_params):
            """
            Initialize algorithm QueryPartition Materialization

            :param execution_system: an instance of EMRSystem
            :param algorithm_instance: name of the algorithm instance
            :param algorithm_params: algorithm configuration
            """

            super(AlgorithmPartitionMaterialization.QueryPartitionMaterialization, self).__init__(
                execution_system,
                algorithm_instance,
                algorithm_params
            )

        def get_scala_class(self):
            return ScalaClasses.PARTITION_QUERY_MATERIALIZATION

        def _get_additional_parameters(self):
            select_conditions = self._parameters[self.ConfigKeys.SELECT_CONDITIONS]
            return {
                self.ConfigKeys.SELECT_CONDITIONS: select_conditions
            }
