import json
import os


class ConfigService(object):

    class Prefixes(object):
        ACON = "acon"
        SCON = "scon"
        DDIC = "ddic"
        TCONX = "tconx"

    class Protocols(object):
        S3A = "s3a://"
        S3 = "s3://"

    class Extensions(object):
        JSON = ".json"
        SQL = ".sql"
        HQL = ".hql"
        SH = ".sh"

    def __init__(self, config):
        # store parameters
        self.config = config

        # open system config file
        with open(config) as data_file:
            params = json.load(data_file)

        self.emails = params["emails"]

        self.python_main = params["python"]["main"]
        self.python_base_package = params["python"]["base_package"]

        self.dir_exec = params["dir_exec"]

        self.subdir_projects_m3d_engine = params["subdir_projects"]["m3d_engine"]
        self.subdir_projects_m3d_api = params["subdir_projects"]["m3d_api"]

        self.data_dict_delimiter = params["data_dict_delimiter"]

        # prefixes for algorithm configuration files
        self.tag_full_load = params["tags"]["full_load"]
        self.tag_delta_load = params["tags"]["delta_load"]
        self.tag_append_load = params["tags"]["append_load"]

        # suffixes for staging and swap tables
        self.tag_table_suffix_stage = params["tags"]["table_suffix_stage"]
        self.tag_table_suffix_swap = params["tags"]["table_suffix_swap"]

        # names of configuration directories
        self.tag_system = params["tags"]["system"]
        self.tag_table = params["tags"]["table"]
        self.tag_view = params["tags"]["view"]
        self.tag_algorithm = params["tags"]["algorithm"]
        self.tag_config = params["tags"]["config"]
        self.tag_pushdown = params["tags"]["pushdown"]
        self.tag_upload = params["tags"]["upload"]
        self.tag_aws = params["tags"]["aws"]

        # protocol tags and required constants for them
        # TODO: Remove hdfs tag from config
        self.tag_file = params["tags"]["file"]

    def get_scon_path(self, source_system, database):
        """
        Return system config file path

        :param source_system: system code
        :param database: database code

        :return: system config file path
        """

        filename = "{scon}-{system}-{database}{extension}".format(
            scon=ConfigService.Prefixes.SCON,
            system=source_system,
            database=database,
            extension=ConfigService.Extensions.JSON
        )

        base_path = os.path.join(self.tag_config, self.tag_system, filename)

        return base_path

    def get_tconx_path(
            self,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
    ):
        """
        Return TCONX file path

        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param destination_table: destination table code
        :return: TCONX file path for export and import
        """

        extension = ConfigService.Extensions.JSON
        filename = "{prefix}-{system}-{database}-{environment}-{table}{extension}".format(
            prefix=ConfigService.Prefixes.TCONX,
            system=destination_system,
            database=destination_database,
            environment=destination_environment,
            table=destination_table,
            extension=extension
        )

        base_path = os.path.join(
            self.tag_config,
            self.tag_table,
            self.tag_pushdown,
            destination_system,
            destination_database,
            destination_environment,
            filename
        )

        return base_path

    def get_hql_path(self, destination_system, destination_database, destination_environment, destination_view):
        """
        Return HQL file path

        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param destination_view: destination view code
        :return: TCONX file path for export and import
        """
        extension = ConfigService.Extensions.HQL
        filename = "{system}-{database}-{environment}-{view}{extension}".format(
            system=destination_system,
            database=destination_database,
            environment=destination_environment,
            view=destination_view,
            extension=extension
        )

        base_path = os.path.join(
            self.tag_config,
            self.tag_view,
            self.tag_pushdown,
            destination_system,
            destination_database,
            destination_environment,
            filename
        )

        return base_path

    def get_acon_path(self, destination_database, destination_environment, algorithm_instance):
        """
        Return algorithm config file path

        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param algorithm_instance: algorithm instance code

        :return: algorithm config file path
        """

        filename = "{acon}-{database}-{algorithm}{extension}".format(
            acon=ConfigService.Prefixes.ACON,
            database=destination_database,
            algorithm=algorithm_instance,
            extension=ConfigService.Extensions.JSON
        )

        base_path = os.path.join(
            self.tag_config,
            self.tag_algorithm,
            destination_database,
            destination_environment,
            filename
        )

        return base_path

    def get_ddic_path(self, source_system, src_database, source_schema, source_table):
        """
        Return ddic path for upload system export

        :param source_system source system code
        :param src_database source database code
        :param source_schema upload schema code
        :param source_table: upload table code
        :return: ddic file for upload system export
        """
        filename = "-".join([
            ConfigService.Prefixes.DDIC,
            source_system,
            src_database,
            source_schema,
            source_table
        ]) + ConfigService.Extensions.CSV

        base_path = os.path.join(self.tag_config, self.tag_table, self.tag_upload, source_system, filename)

        return base_path
