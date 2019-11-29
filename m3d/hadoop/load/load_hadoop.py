import logging
import os

from m3d.config.config_service import ConfigService
from m3d.util.util import Util


class LoadHadoop(object):
    PARAMETERS_KEY = "parameters"

    def __init__(self, execution_system, dataset):
        """
        Initialize Algorithm Config

        :param execution_system: execution system (EMR or Cloudera System)
        :param dataset: an instance of dataset object
        """

        self._dataset = dataset
        self._execution_system = execution_system

        load_config_filename = self._create_load_config_filename()
        execution_dir = self._execution_system.config_service.dir_exec
        remote_config_dir = self._get_remote_config_dir()
        self._load_config_local_path = os.path.join(execution_dir, load_config_filename)
        self._load_config_remote_path = os.path.join(remote_config_dir, load_config_filename)

    def get_load_config_local_path(self):
        return self._load_config_local_path

    def get_load_config_remote_path(self):
        return self._load_config_remote_path

    def pre_submit_tasks(self):
        logging.debug("No overriding implementation for method {} in subclass {}.".format(
            "pre_submit_tasks",
            self.__class__.__name__
        ))

    def post_submit_tasks(self):
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

    def get_scala_class(self):
        raise NotImplementedError("There is currently no implementation for {}.get_scala_class()."
                                  .format(self.__class__.__name__))

    def build_params(self):
        raise NotImplementedError("There is currently no implementation for {}.build_params()."
                                  .format(self.__class__.__name__))

    def get_load_type(self):
        raise NotImplementedError("There is currently no implementation for {}.get_load_type()."
                                  .format(self.__class__.__name__))

    def _get_remote_config_dir(self):
        raise NotImplementedError("There is currently no implementation for {}._get_remote_config_dir()."
                                  .format(self.__class__.__name__))

    def _get_load_load_tag(self):
        raise NotImplementedError("There is currently no implementation for {}._get_load_load_tag()."
                                  .format(self.__class__.__name__))

    def _create_load_config_filename(self):
        return "{load_tag}-{environment}-{table}{extension}".format(
            load_tag=self._get_load_load_tag(),
            environment=self._execution_system.environment,
            table=self._dataset.table_lake,
            extension=ConfigService.Extensions.JSON
        )

    @staticmethod
    def read_acon_params(execution_system, table_name):
        config_service = ConfigService(execution_system.config)

        acon_path = config_service.get_acon_path(
            execution_system.database,
            execution_system.environment,
            table_name)

        acon_dict = Util.load_dict(acon_path)
        return acon_dict.get(LoadHadoop.PARAMETERS_KEY, {})
