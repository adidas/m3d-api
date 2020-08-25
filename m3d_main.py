#!/usr/bin/python
import argparse
import sys
import json

import logging
import logging.config

from m3d import M3D

MANDATORY_ARGS = "mandatory_arguments"
OPTIONAL_ARGS = "optional_arguments"

DEFAULT_LOGGING_CONFIG = "config/logging/logging.json"
DEFAULT_LOG_FORMAT = "%(asctime)s|%(levelname)s|%(name)s|%(filename)s:%(lineno)d|%(message)s"
DEFAULT_LOG_LEVEL = logging.INFO


def setup_console_logging(log_level, log_format):
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(log_format)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def setup_logging(logging_config_path, default_log_level, default_log_format):
    """
    Setup logging configuration.

    :param logging_config_path: path of file from where logging configuration should be loaded
    :param default_log_level: default log level to fall back to
    :param default_log_format: default log format to fall back to
    """
    try:
        # Read logging configuration from logging_config_path
        with open(logging_config_path, 'rt') as f:
            config_dict = json.load(f)

        logging.config.dictConfig(config_dict)

    except Exception as exc:
        # Fall back to default logging configuration
        setup_console_logging(default_log_level, default_log_format)

        logging.warning("Failed to set up logger from config \"{}\": {}".format(logging_config_path, exc))
        logging.warning("Falling back to default console logger.")


if __name__ == "__main__":

    setup_logging(
        DEFAULT_LOGGING_CONFIG,
        DEFAULT_LOG_LEVEL,
        DEFAULT_LOG_FORMAT
    )

    # parse arguments
    parser = argparse.ArgumentParser(description='Wrapper for schema creation')
    parser.add_argument('-function', action="store", dest="function", help="function to be executed")
    parser.add_argument('-config', action="store", dest="config", help="global system config file")
    parser.add_argument('-destination_system', action="store",
                        dest="destination_system", help="destination system code")
    parser.add_argument('-destination_database', action="store",
                        dest="destination_database", help="destination database code")
    parser.add_argument('-destination_environment', action="store",
                        dest="destination_environment", help="destination environment code")
    parser.add_argument('-destination_table', action="store",
                        dest="destination_table", help="destination table code")
    parser.add_argument('-destination_table_location_prefix', action="store",
                        dest="destination_table_location_prefix", help="destination table location prefix")
    parser.add_argument('-destination_dataset', action="store",
                        dest="destination_dataset", help="destination dataset code")
    parser.add_argument('-algorithm_instance', action="store",
                        dest="algorithm_instance", help="algorithm instance code")
    parser.add_argument('-load_type', action='store', dest="load_type", default="FullLoad",
                        help="load function code (default = FullLoad)")
    parser.add_argument('-ext_params', action='store', dest="ext_params", default=None,
                        help="external configuration in JSON format (default=None)")
    parser.add_argument('-emr_cluster_id', action="store", dest="emr_cluster_id", default=None, help="EMR cluster id")
    parser.add_argument('-spark_params', action="store", dest="spark_params", default=None, help="Spark parameters")
    parser.add_argument('-core_instance_count', action="store", dest="core_instance_count", help="number of instances")
    parser.add_argument('-core_instance_type', action="store", dest="core_instance_type", help="type of the instance")
    parser.add_argument('-master_instance_type', action="store", dest="master_instance_type",
                        help="type of the master instance")
    parser.add_argument('-emr_version', action="store", dest="emr_version", help="EMR version in stack")

    args = parser.parse_args()

    # check if function is missing
    if args.function is None:
        raise Exception("Required arguments: -function")

    api = {
        "create_table": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table"],
            OPTIONAL_ARGS: ["emr_cluster_id", "destination_table_location_prefix"]
        },
        "drop_table": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table"],
            OPTIONAL_ARGS: ["emr_cluster_id"]
        },
        "load_table": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table",
                             "load_type"],
            OPTIONAL_ARGS: ["emr_cluster_id", "spark_params"]
        },
        "truncate_table": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table"],
            OPTIONAL_ARGS: ["emr_cluster_id"]
        },
        "create_out_view": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table"],
            OPTIONAL_ARGS: ["emr_cluster_id"]
        },
        "drop_out_view": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_table"],
            OPTIONAL_ARGS: ["emr_cluster_id"]
        },
        "run_algorithm": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "algorithm_instance"],
            OPTIONAL_ARGS: ["emr_cluster_id", "ext_params"]
        },
        "create_emr_cluster": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "core_instance_count", "core_instance_type",
                             "master_instance_type"],
            OPTIONAL_ARGS: ["emr_version", "ebs_size"]
        },
        "delete_emr_cluster": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "emr_cluster_id"]
        },
        "drop_dataset": {
            MANDATORY_ARGS: ["config", "destination_system", "destination_database",
                             "destination_environment", "destination_dataset"],
            OPTIONAL_ARGS: ["emr_cluster_id"]
        }
    }

    if args.function not in api.keys():
        raise Exception('Unknown service: ' + args.function)

    fn_kwargs = {}

    # load mandatory arguments
    for argument in api[args.function][MANDATORY_ARGS]:
        argument_value = getattr(args, argument)

        if argument_value is None:
            raise Exception(
                "Wrong usage of '{}' function: missing '{}' mandatory argument.".format(args.function, argument)
            )

        fn_kwargs[argument] = argument_value

    # load optional arguments
    if OPTIONAL_ARGS in api[args.function]:
        for argument in api[args.function][OPTIONAL_ARGS]:
            if argument in args:
                fn_kwargs[argument] = getattr(args, argument)

    api_function = getattr(M3D, args.function)

    try:
        logging.info("Calling M3D.{}().".format(args.function))
        api_function(**fn_kwargs)

    except Exception as e:
        logging.error("M3D.{}() call failed because of an error: {}".format(args.function, e))
        raise
