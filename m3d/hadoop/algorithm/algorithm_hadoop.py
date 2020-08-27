import logging
import os

from m3d.config.config_service import ConfigService
from m3d.hadoop.algorithm.algorithm_configuration_hadoop import AlgorithmConfigurationHadoop
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.util.util import Util


class AlgorithmHadoop(object):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize generic Algorithm class

        :param execution_system: an instance of execution system
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        self._execution_system = execution_system
        self._parameters = algorithm_params.get(AlgorithmConfigurationHadoop.Keys.PARAMETERS, {})

        param_file_basename = "{system}-{database}-{environment}.{algorithm}.{time}{extension}".format(
            system=self._execution_system.source_system,
            database=self._execution_system.database,
            environment=self._execution_system.environment,
            algorithm=algorithm_instance,
            time=Util.get_formatted_utc_now(EMRSystem.DATETIME_FORMAT),
            extension=ConfigService.Extensions.JSON
        )

        # derived
        dir_apps_algorithm_instance = os.path.join(
            self._execution_system.dir_apps_algorithm,
            algorithm_instance
        )

        self._params_uri_cluster = os.path.join(dir_apps_algorithm_instance, param_file_basename)
        self._params_uri_local = os.path.join(self._execution_system.config_service.dir_exec, param_file_basename)

    def pre_submit_tasks(self):
        """
        Executes pre-submit tasks
        """
        logging.debug("No overriding implementation for method {} in subclass {}.".format(
            "pre_submit_tasks",
            self.__class__.__name__
        ))

    def post_submit_tasks(self):
        """
        Executes post-submit tasks
        """
        logging.debug("No overriding implementation for method {} in subclass {}.".format(
            "post_submit_tasks",
            self.__class__.__name__
        ))

    def cleanup(self):
        """
        Performs cleanup tasks
        """
        logging.debug("No overriding implementation for method {} in subclass {}.".format(
            "cleanup",
            self.__class__.__name__
        ))

    def get_params_uri_cluster(self):
        return self._params_uri_cluster

    def get_params_uri_local(self):
        return self._params_uri_local

    def get_scala_class(self):
        raise NotImplementedError("There is currently no implementation for {}.get_scala_class()."
                                  .format(self.__class__.__name__))

    def build_params(self):
        raise NotImplementedError("There is currently no implementation for {}.build_params()."
                                  .format(self.__class__.__name__))
