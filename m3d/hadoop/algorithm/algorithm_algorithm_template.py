from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.emr_system import EMRSystem


class AlgorithmAlgorithmTemplate(AlgorithmHadoop):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize Algorithm Algorithm Template

        :param execution_system: an instance of EMRSystem object
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmAlgorithmTemplate, self).__init__(execution_system, algorithm_instance, algorithm_params)

        self.source_table = self._execution_system.db_lake + "." + self._parameters["source_table"]
        # you can use a source location as parquet files on the lake instead of a hive table
        # make sure not the repeat the full path again on the acon file if you have the following concatenation logic
        # self.source_location = os.path.join("s3://",
        #                                      self._execution_system.bucket_lake, self._parameters["source_location"])
        self.target_table = self._execution_system.db_lake + "." + self._parameters["target_table"]

        self.output_dictionary = {
            "source_table": self.source_table,
            # you can use a source location as parquet files on the lake instead of a hive table
            # "source_location": self.source_location,
            "target_table": self.target_table,
            "date_from": self._parameters["date_from"],
            "date_to": self._parameters["date_to"]
        }

        execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.SOURCE_TABLE: self.source_table,
            EMRSystem.EMRClusterTag.TARGET_TABLE: self.target_table
        })

    def get_scala_class(self):
        return ScalaClasses.ALGORITHM_TEMPLATE

    def build_params(self):
        return self.output_dictionary
