import json

from m3d.config.config_service import ConfigService
from m3d.util.util import Util


class AlgorithmConfigurationHadoop(object):

    class Sections(object):
        ENVIRONMENT = "environment"
        ALGORITHM = "algorithm"

    class Keys(object):
        # environment section
        SPARK = "spark"

        # algorithm section
        PYTHON_CLASS = "python_class"
        PARAMETERS = "parameters"

    def __init__(self, algorithm_instance, acon_dict):
        """
        Constructor for AlgorithmConfigurationHadoop object.
        It will extract values required for it from acon_dict.

        :param algorithm_instance: algorithm instance code
        :param acon_dict: dictionary corresponding to content of acon file
        """

        self._algorithm_instance = algorithm_instance

        environment_section = acon_dict[self.Sections.ENVIRONMENT]
        # TODO: this should not be a class field because all the parameters should be contained in algorithm/parameters,
        #  at the moment it is used for compatibility with Reconciliation and GzipDecompressor
        self._algorithm_params = acon_dict[self.Sections.ALGORITHM]

        self._python_class = self._algorithm_params[self.Keys.PYTHON_CLASS]

        # spark params are not mandatory.
        self._spark_params = environment_section.get(self.Keys.SPARK, {})

    def get_algorithm_params(self):
        return self._algorithm_params

    def get_spark_params(self):
        return self._spark_params

    def get_python_class(self):
        return self._python_class

    def get_algorithm_instance(self):
        return self._algorithm_instance

    @staticmethod
    def create(
            config_path,
            destination_database,
            destination_environment,
            algorithm_instance,
            ext_params_str=None
    ):
        """
        Create algorithm configuration object from acon file. Method will discover acon file based on the
        parameters passed to it.

        :return: Returns algorithm configuration object of the type that is used for calling the method.
        """

        # Create config service to get acon file path.
        config_service = ConfigService(config_path)
        acon_path = config_service.get_acon_path(
            destination_database,
            destination_environment,
            algorithm_instance
        )
        acon_dict = Util.load_dict(acon_path)

        if ext_params_str:
            ext_params_dict = json.loads(ext_params_str)
            acon_dict = Util.merge_nested_dicts(acon_dict, ext_params_dict)

        return AlgorithmConfigurationHadoop(algorithm_instance, acon_dict)
