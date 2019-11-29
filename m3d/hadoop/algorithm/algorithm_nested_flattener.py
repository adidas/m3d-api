from m3d.config.config_service import ConfigService
from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem
import os


class AlgorithmNestedFlattener(AlgorithmHadoop):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize Algorithm NestedFlattener

        :param execution_system: an instance of EMRSystem object
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmNestedFlattener, self).__init__(execution_system, algorithm_instance, algorithm_params)

        self.source_location = os.path.join(ConfigService.Protocols.S3, self._execution_system.bucket_lake,
                                            self._parameters["source_location"])

        self.target_table = self._execution_system.db_lake + "." + self._parameters["target_table"]

        self.output_dictionary = {
            "source_location": self.source_location,
            "target_table": self.target_table,
            "chars_to_replace": self._parameters["chars_to_replace"],
            "replacement_char": self._parameters["replacement_char"],
            "fields_to_flatten": self._parameters["fields_to_flatten"],
            "column_mapping": self._parameters["column_mapping"]
        }

        self.target_partitions = self._parameters.get("target_partitions", None)
        if self.target_partitions is not None:
            self.output_dictionary["target_partitions"] = self.target_partitions

        execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.SOURCE_TABLE: self.source_location,
            EMRSystem.EMRClusterTag.TARGET_TABLE: self.target_table
        })

    def get_scala_class(self):
        return ScalaClasses.NESTED_FLATTENER

    def build_params(self):
        return self.output_dictionary
