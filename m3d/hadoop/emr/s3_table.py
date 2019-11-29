import logging
import os

from m3d.config.config_service import ConfigService
from m3d.exceptions.m3d_exceptions import M3DDatabaseException, M3DException
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.core.spark_parameters import SparkParameters
from m3d.hadoop.emr.emr_exceptions import M3DEMRStepException
from m3d.util.hql_generator import HQLGenerator


class S3Table(HiveTable):

    def __init__(self, emr_system, destination_table, spark_params=None, **kwargs):
        """
        Initialize representation of Hive table on S3

        :param config: system config file
        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param destination_table: destination table code
        :param emr_cluster_id: emr cluster id
        :param spark_params: external spark parameters to override scon defaults
        """

        # call super constructor
        super(S3Table, self).__init__(
            emr_system.config,
            emr_system.source_system,
            emr_system.database,
            emr_system.environment,
            destination_table,
            **kwargs
        )

        self.emr_system = emr_system

        # parse Spark parameters
        self.spark_params = SparkParameters(self.emr_system.spark_params)
        self.spark_params.merge_spark_params_str(spark_params)

        # derived directories
        self.dir_landing_source_system = os.path.join(
            ConfigService.Protocols.S3 + self.emr_system.bucket_landing,
            self.emr_system.environment,
            self.source_system
        )

        self.dir_landing_table = os.path.join(self.dir_landing_source_system, self.table)
        self.dir_landing_data = os.path.join(self.dir_landing_table, self.emr_system.subdir_data)
        self.dir_landing_work = os.path.join(self.dir_landing_table, self.emr_system.subdir_work)
        self.dir_landing_archive = os.path.join(self.dir_landing_table, self.emr_system.subdir_archive)
        self.dir_landing_header = os.path.join(self.dir_landing_table, self.emr_system.subdir_header)
        self.dir_landing_final = self.dir_landing_data  # used for compatibility with HDFSTable

        self.dir_lake_source_system = os.path.join(
            ConfigService.Protocols.S3 + self.emr_system.bucket_lake,
            self.emr_system.environment,
            self.source_system
        )

        self.dir_lake_table = os.path.join(self.dir_lake_source_system, self.table)
        self.dir_lake_final = os.path.join(self.dir_lake_table, self.emr_system.subdir_data)
        self.dir_lake_backup = os.path.join(self.dir_lake_table, self.emr_system.subdir_data_backup)

        # apps
        self.dir_apps_system = os.path.join(self.emr_system.dir_apps_loading, self.source_system)
        self.dir_apps_table = os.path.join(self.dir_apps_system, self.table)
        self.dir_apps_full_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_full_load)
        self.dir_apps_delta_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_delta_load)
        self.dir_apps_append_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_append_load)

    def create_tables(self):
        hql = "\n".join([
            self._get_create_database_if_not_exists(self.db_landing),
            self._get_create_database_if_not_exists(self.db_lake),
            self._get_create_landing_statement(self.dir_landing_final).with_semicolon(),
            HQLGenerator.generate_repair_table(self.db_table_landing).with_semicolon(),
            self._get_create_lake_statement(self.dir_lake_final).with_semicolon(),
            HQLGenerator.generate_repair_table(self.db_table_lake).with_semicolon()
        ])

        try:
            self.emr_system.execute_hive(hql)
        except Exception:
            msg = "Failed to create {} and {} tables.".format(self.db_table_landing, self.db_table_lake)
            logging.error(msg)
            raise

        logging.info("Successfully created {} and {} tables.".format(self.db_table_landing, self.db_table_lake))

    def drop_tables(self):
        """
        Delete landing and lake tables from both Hive metastore and S3
        """

        # noinspection PyBroadException
        def drop_table(db_table):
            drop_table_hql = HQLGenerator.generate_drop_table(db_table).with_semicolon()

            try:
                logging.info("Dropping table {} from Hive".format(db_table))
                self.emr_system.execute_hive(drop_table_hql)
                return 1
            except Exception:
                logging.warning("Unable to drop {} table".format(db_table))
                return 0

        logging.info("Deleting S3 tables...")

        table_names = [self.db_table_landing, self.db_table_lake]
        dropped = list(filter(lambda x: x > 0, map(lambda x: drop_table(x), table_names)))

        if len(dropped) == 0:
            raise M3DException("Unable to drop any of the following tables: {}".format(table_names))

        logging.info("S3 tables were successfully deleted")

    def create_lake_out_view(self):
        """
        Create lake_out view with synchronized column names. Drop and recreate view if it exists in Hive.
        """

        # ensure view name is not empty
        if not self.table_lakeout:
            raise M3DDatabaseException(message="lake_out view name does not exist")

        # ensure at least one field can be renamed
        if len(list(filter(lambda x: x, self.columns_lakeout))) == 0:
            raise M3DDatabaseException(
                message="View {} cannot be created. The view would have no columns.".format(self.db_view_lake_out)
            )

        create_lake_out_view_ddl = "\n".join([
            self._get_drop_lakeout_statement().with_semicolon(),
            self._get_create_lakeout_statement().with_semicolon()
        ])

        try:
            self.emr_system.execute_hive(create_lake_out_view_ddl)
        except Exception:
            msg = "Failed to create {} view.".format(self.db_view_lake_out)
            logging.error(msg)
            raise

        logging.info("Successfully created {} view.".format(self.db_view_lake_out))

    def drop_lake_out_view(self):
        """
        Drop  lake out view.
        """

        # ensure view name is not empty
        if self.table_lakeout == "":
            raise M3DDatabaseException(message="lake_out view name does not exist")

        drop_lake_out_view_ddl = self._get_drop_lakeout_statement().with_semicolon()

        try:
            self.emr_system.execute_hive(drop_lake_out_view_ddl)
        except Exception:
            msg = "Failed to drop {} view.".format(self.db_view_lake_out)
            logging.error(msg)
            raise

        logging.info("Successfully dropped {} view.".format(self.db_view_lake_out))

    def truncate_tables(self):
        """
        Delete underlying data from landing and lake.
        """

        def reset_table(db_table, create_table_hql, table_location, table_partitioned_flag):
            if table_partitioned_flag:
                drop_table_hql = HQLGenerator.generate_drop_table(db_table).with_semicolon()
                repair_table_hql = HQLGenerator.generate_repair_table(db_table).with_semicolon()
                hql = "\n".join([drop_table_hql, create_table_hql, repair_table_hql])
            else:
                hql = HQLGenerator.generate_alter_table_location(db_table, table_location).with_semicolon()

            try:
                logging.info("Resetting '{}' table.".format(db_table))
                self.emr_system.execute_hive(hql)
            except M3DEMRStepException as e:
                if "Table not found" in str(e):
                    pass  # the table might already not be present, so we will ignore error arising from that case
                else:
                    logging.info("Failed to reset '{}' table: {}".format(db_table, e))
                    raise

        def truncate_table(db_table, create_table_hql, table_location, table_partitioned_flag, s3_dir_list):
            # noinspection PyBroadException
            try:
                for s3_dir in s3_dir_list:
                    logging.info("Deleting data from '{}'.".format(s3_dir))
                    self.emr_system.s3_util.delete_child_objects(s3_dir)

                logging.info("Data for '{}' table has been successfully deleted".format(db_table))

                reset_table(db_table, create_table_hql, table_location, table_partitioned_flag)
                logging.info("Successfully truncated '{}' table".format(db_table))
                return 1

            except Exception as e:
                logging.error("Failed to perform truncation of '{}' table: {}".format(db_table, e))
                return 0

        landing_dirs = [self.dir_landing_work, self.dir_landing_archive, self.dir_landing_final]
        landing_create_table_ddl = self._get_create_landing_statement(self.dir_landing_final).with_semicolon()

        lake_dirs = [self.dir_lake_final]
        lake_create_table_ddl = self._get_create_lake_statement(self.dir_lake_final).with_semicolon()

        truncation_specs = [
            (self.db_table_landing, landing_create_table_ddl, self.dir_landing_final, False, landing_dirs),
            (self.db_table_lake, lake_create_table_ddl, self.dir_lake_final, bool(self.partitioned_by), lake_dirs)
        ]

        truncated = list(filter(lambda x: x > 0, map(lambda specs: truncate_table(*specs), truncation_specs)))
        if len(truncated) == 0:
            raise M3DException("Failed to truncate any of the following tables: {}, {}".format(
                self.db_table_landing,
                self.db_table_lake)
            )

        logging.info("Successfully truncated tables: {}, {}".format(self.db_table_landing, self.db_table_lake))
