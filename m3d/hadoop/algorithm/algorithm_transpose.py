from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem


class AlgorithmTranspose(AlgorithmHadoop):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize Algorithm To Transpose Data

        :param execution_system: an instance of EMRSystem object
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmTranspose, self).__init__(execution_system, algorithm_instance, algorithm_params)

        self.source_table = self._execution_system.db_lake + "." + self._parameters["source_table"]

        self.target_table = self._execution_system.db_lake + "." + self._parameters["target_table"]

        self.output_dictionary = {
            "source_table": self.source_table,
            "target_table": self.target_table,
            "aggregation_column": self._parameters["aggregation_column"],
            "pivot_column": self._parameters["pivot_column"],
            "group_by_column": self._parameters["group_by_column"],
            "verify_schema": self._parameters["verify_schema"]
        }

        self.target_partitions = self._parameters.get("target_partitions", None)
        if self.target_partitions is not None:
            self.output_dictionary["target_partitions"] = self.target_partitions

        execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.SOURCE_TABLE: self.source_table,
            EMRSystem.EMRClusterTag.TARGET_TABLE: self.target_table
        })

    def get_scala_class(self):
        return ScalaClasses.TRANSPOSE

    def build_params(self):
        return self.output_dictionary
