from m3d import M3D
from m3d.exceptions.m3d_exceptions import M3DUnsupportedDatabaseTypeException, M3DUnsupportedAlgorithmException
from m3d.hadoop.algorithm.algorithm_algorithm_template import AlgorithmAlgorithmTemplate
from m3d.hadoop.algorithm.algorithm_configuration_hadoop import AlgorithmConfigurationHadoop
from m3d.hadoop.algorithm.algorithm_fixed_length_string_extractor import AlgorithmFixedLengthStringExtractor
from m3d.hadoop.algorithm.algorithm_gzip_decompressor import AlgorithmGzipDecompressor
from m3d.hadoop.algorithm.algorithm_nested_flattener import AlgorithmNestedFlattener
from m3d.hadoop.algorithm.algorithm_materialization import AlgorithmMaterialization
from m3d.hadoop.algorithm.algorithm_transpose import AlgorithmTranspose
from m3d.hadoop.algorithm.algorithm_scala_runner import AlgorithmScalaRunner
from m3d.hadoop.core.spark_executor import SparkExecutor
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.system.data_system import DataSystem


class AlgorithmExecutorHadoop(SparkExecutor):

    def __init__(self, execution_system, algorithm_config):
        """
        Initialize Algorithm Executor

        :param execution_system: an instance of EMR system
        :param algorithm_config: algorithm configuration
        """

        super(AlgorithmExecutorHadoop, self).__init__(execution_system)

        python_class = algorithm_config.get_python_class()
        available_algorithms = self._get_supported_emr_algorithms()
        if python_class not in available_algorithms:
            raise M3DUnsupportedAlgorithmException(python_class)

        self._spark_parameters = algorithm_config.get_spark_params()
        self._algorithm_instance = algorithm_config.get_algorithm_instance()
        self._algorithm_wrapper = available_algorithms[python_class](
            execution_system=self._execution_system,
            algorithm_instance=algorithm_config.get_algorithm_instance(),
            algorithm_params=algorithm_config.get_algorithm_params()
        )

        self._execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.API_METHOD: M3D.run_algorithm.__name__,
            EMRSystem.EMRClusterTag.ALGORITHM_CLASS: python_class,
            EMRSystem.EMRClusterTag.ALGORITHM_INSTANCE: algorithm_config.get_algorithm_instance()
        })

    @staticmethod
    def _get_supported_emr_algorithms():
        """
        Return a list of the available EMR algorithms

        :return: dictionary algorithm-name -> algorithm-class
        """

        return {
            "AlgorithmGzipDecompressorBytes": AlgorithmGzipDecompressor,
            "AlgorithmScalaRunner": AlgorithmScalaRunner,
            "AlgorithmFullMaterialization": AlgorithmMaterialization.FullMaterialization,
            "AlgorithmQueryMaterialization": AlgorithmMaterialization.QueryMaterialization,
            "AlgorithmRangeMaterialization": AlgorithmMaterialization.RangeMaterialization,
            "AlgorithmFixedLengthStringExtractor": AlgorithmFixedLengthStringExtractor,
            "AlgorithmNestedFlattener": AlgorithmNestedFlattener,
            "AlgorithmTranspose": AlgorithmTranspose,
            "AlgorithmAlgorithmTemplate": AlgorithmAlgorithmTemplate
        }

    def _get_spark_submit_str(self):
        """
        Execute spark-submit command for algorithm to be executed

        :return: spark submit command
        """

        # get spark submit String
        spark_str = self._execution_system.create_spark_submit_str(
            self._spark_parameters,
            self._algorithm_wrapper.get_scala_class(),
            self._algorithm_wrapper.get_params_uri_cluster()
        )

        return spark_str

    def _run_steps(self):
        """
        Orchestration method for hadoop algorithms
        - Compiles algorithm parameter JSON & uploads it to the cluster
        - Triggers the execution of the spark job
        - Cleans up
        :return:
        """

        try:
            self._algorithm_wrapper.pre_submit_tasks()
            algorithm_parameters = self._algorithm_wrapper.build_params()
            self._upload_parameter_json(
                algorithm_parameters,
                self._algorithm_wrapper.get_params_uri_local(),
                self._algorithm_wrapper.get_params_uri_cluster()
            )
            self._spark_submit(self._algorithm_wrapper.get_scala_class())
            self._algorithm_wrapper.post_submit_tasks()

        except Exception:
            self._report_error(self._algorithm_instance)
            raise

        finally:
            self._remove_parameter_json(self._algorithm_wrapper.get_params_uri_cluster())
            self._algorithm_wrapper.cleanup()

        self._report_success(self._algorithm_instance)

    @staticmethod
    def create(
            config_path,
            destination_system,
            destination_database,
            destination_environment,
            algorithm_instance,
            emr_cluster_id,
            ext_params_str
    ):
        data_system = DataSystem(
            config_path,
            destination_system,
            destination_database,
            destination_environment
        )
        if data_system.database_type == DataSystem.DatabaseType.EMR:
            config = AlgorithmConfigurationHadoop.create(
                config_path,
                destination_database,
                destination_environment,
                algorithm_instance,
                ext_params_str
            )

            execution_system = EMRSystem.from_data_system(data_system, emr_cluster_id)
            return AlgorithmExecutorHadoop(execution_system, config)
        else:
            raise M3DUnsupportedDatabaseTypeException(data_system.database_type)
