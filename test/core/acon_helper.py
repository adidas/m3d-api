import json
import os

import py

from m3d.config.config_service import ConfigService
from m3d.util.util import Util


class AconHelper(object):

    class Tags(object):
        ALGORITHM = "m3d-engine"

    @staticmethod
    def setup_acon_from_file(
            config_dir_path,
            destination_database,
            destination_environment,
            algorithm_instance,
            base_acon_path
    ):
        acon_file_path = AconHelper.get_acon_file_path(
            config_dir_path,
            destination_database,
            destination_environment,
            algorithm_instance
        )

        if not os.path.isdir(os.path.dirname(acon_file_path)):
            os.makedirs(os.path.dirname(acon_file_path))

        py.path.local(acon_file_path).write(py.path.local(base_acon_path).read())
        acon_dict = Util.load_dict(base_acon_path)

        return acon_file_path, acon_dict

    @staticmethod
    def setup_acon_from_dict(
            config_dir_path,
            destination_database,
            destination_environment,
            algorithm_instance,
            base_acon_dict
    ):
        acon_file_path = AconHelper.get_acon_file_path(
            config_dir_path,
            destination_database,
            destination_environment,
            algorithm_instance
        )

        if not os.path.isdir(os.path.dirname(acon_file_path)):
            os.makedirs(os.path.dirname(acon_file_path))

        py.path.local(acon_file_path).write(json.dumps(base_acon_dict, indent=4))

        return acon_file_path, base_acon_dict

    @staticmethod
    def get_acon_file_name(destination_database, algorithm_instance):
        acon_file_name = "acon-{database}-{instance}{extension}".format(
            database=destination_database,
            instance=algorithm_instance,
            extension=ConfigService.Extensions.JSON
        )
        return acon_file_name

    @staticmethod
    def get_acon_file_path(config_dir_path, destination_database, destination_environment, algorithm_instance):
        acon_file_name = AconHelper.get_acon_file_name(destination_database, algorithm_instance)

        config_dir = py.path.local(config_dir_path)
        acon_file = config_dir \
            .join(AconHelper.Tags.ALGORITHM) \
            .join(destination_database) \
            .join(destination_environment) \
            .join(acon_file_name)

        return str(acon_file)
