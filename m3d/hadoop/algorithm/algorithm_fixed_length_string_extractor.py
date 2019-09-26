from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem


class AlgorithmFixedLengthStringExtractor(AlgorithmHadoop):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize Algorithm Clickstream Preprocessor

        :param execution_system: an instance of EMRSystem object
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmFixedLengthStringExtractor, self).__init__(
            execution_system,
            algorithm_instance,
            algorithm_params
        )

        self.validate_parameters()

        self.source_table = self._execution_system.db_lake + "." + self._parameters["source_table"]
        self.target_table = self._execution_system.db_lake + "." + self._parameters["target_table"]

        execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.SOURCE_TABLE: self.source_table,
            EMRSystem.EMRClusterTag.TARGET_TABLE: self.target_table
        })

    def get_scala_class(self):
        return ScalaClasses.FIXED_LENGTH_STRING_EXTRACTOR

    def build_params(self):
        params = {
            "source_table": self.source_table,
            "target_table": self.target_table,
            "source_field": self._parameters["source_field"],
            "substring_positions": self._parameters["substring_positions"]
        }

        if "partition_columns" in self._parameters:
            params["partition_columns"] = self._parameters["partition_columns"]

        if "select_conditions" in self._parameters:
            params["select_conditions"] = self._parameters["select_conditions"]

        if "select_rules" in self._parameters:
            params["select_rules"] = self._parameters["select_rules"]

        return params

    def validate_parameters(self):
        if "source_table" not in self._parameters:
            raise M3DIllegalArgumentException("Source table name is missing in the acon-file")

        if "target_table" not in self._parameters:
            raise M3DIllegalArgumentException("Target table name is missing in the acon-file")

        if "source_field" not in self._parameters:
            raise M3DIllegalArgumentException("Source field name is missing in the acon-file")

        if "substring_positions" not in self._parameters:
            raise M3DIllegalArgumentException("Substring positions specification is missing in the acon-file")

        if "select_conditions" in self._parameters and "partition_columns" not in self._parameters:
            raise M3DIllegalArgumentException("Unable to use select_conditions for unpartitioned table")

        if "select_rules" in self._parameters and "partition_columns" not in self._parameters:
            raise M3DIllegalArgumentException("Unable to use select_rules for unpartitioned table")

        if "select_conditions" in self._parameters and "select_rules" in self._parameters:
            raise M3DIllegalArgumentException("Unable to use both select_conditions and select_rules at the same time")
