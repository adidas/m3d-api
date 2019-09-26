import json
import logging

from m3d import M3D
from m3d.exceptions.m3d_exceptions import M3DUnsupportedDatabaseTypeException, M3DUnsupportedLoadTypeException
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.core.spark_executor import SparkExecutor
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.hadoop.emr.s3_table import S3Table
from m3d.hadoop.load.append_load import AppendLoad
from m3d.hadoop.load.delta_load import DeltaLoad
from m3d.hadoop.load.full_load import FullLoad
from m3d.system.data_system import DataSystem


class LoadExecutorHadoop(SparkExecutor):

    def __init__(self, execution_system, load_type, destination_table, spark_params_dict):
        """
        Initialize Load Executor

        :param execution_system: execution system
        :param load_type: load type
        :param destination_table: table to load
        :param spark_params_dict: spark parameters
        """

        super(LoadExecutorHadoop, self). __init__(execution_system)

        self._destination_table = destination_table
        self._spark_params_dict = spark_params_dict

        available_loads = self._get_available_emr_load_types()
        if load_type not in available_loads:
            raise M3DUnsupportedLoadTypeException(
                load_type=load_type,
                message="Loading algorithm {} not available.".format(load_type)
            )

        table = S3Table(
            emr_system=execution_system,
            destination_table=destination_table
        )

        self._load_wrapper = available_loads[load_type](
            execution_system=self._execution_system,
            table=table
        )

        self._execution_system.add_cluster_tags({
            EMRSystem.EMRClusterTag.API_METHOD: M3D.load_table.__name__,
            EMRSystem.EMRClusterTag.LOAD_TYPE: load_type,
            EMRSystem.EMRClusterTag.TARGET_TABLE: table.db_table_lake
        })

    @staticmethod
    def _get_available_emr_load_types():
        """
        Return a list of the available EMR load

        :return: dictionary load-name -> load-class
        """

        return {
            HiveTable.TableLoadType.FULL: FullLoad,
            HiveTable.TableLoadType.DELTA: DeltaLoad,
            HiveTable.TableLoadType.APPEND: AppendLoad
        }

    def _get_spark_submit_str(self):
        """
        Execute spark-submit command for load to be executed

        :return: spark submit command
        """

        # get spark submit String
        spark_str = self._execution_system.create_spark_submit_str(
            self._spark_params_dict,
            self._load_wrapper.get_scala_class(),
            self._load_wrapper.get_load_config_remote_path()
        )

        return spark_str

    def _run_steps(self):
        """
        Orchestration method for loads
        - Compiles load parameter JSON & uploads it to the cluster
        - Triggers the execution of the spark job
        - Cleans up
        """

        try:
            logging.info("Executing pre-submit tasks")
            self._load_wrapper.pre_submit_tasks()
            logging.info("Preparing load parameters")
            load_parameters = self._load_wrapper.build_params()
            logging.info("Executing {}".format(self._load_wrapper.get_load_type()))
            self._upload_parameter_json(
                load_parameters,
                self._load_wrapper.get_load_config_local_path(),
                self._load_wrapper.get_load_config_remote_path()
            )
            self._spark_submit(self._load_wrapper.get_scala_class())
            logging.info("Executing post-submit tasks")
            self._load_wrapper.post_submit_tasks()

        except Exception:
            self._report_error(self._destination_table)
            raise

        finally:
            self._remove_parameter_json(self._load_wrapper.get_load_config_remote_path())
            self._load_wrapper.cleanup()

        self._report_success(self._destination_table)

    @staticmethod
    def create(
            config_path,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            load_type,
            emr_cluster_id,
            spark_params_str
    ):
        data_system = DataSystem(
            config_path,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment
        )
        if data_system.database_type == DataSystem.DatabaseType.EMR:
            execution_system = EMRSystem.from_data_system(data_system, emr_cluster_id)
            spark_params_dict = json.loads(spark_params_str)
            return LoadExecutorHadoop(execution_system, load_type, destination_table, spark_params_dict)
        else:
            raise M3DUnsupportedDatabaseTypeException(data_system.database_type)
