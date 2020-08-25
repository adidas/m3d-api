import os
import logging

from m3d.config.config_service import ConfigService
from m3d.hadoop.dataset.dataset import DataSet


class SemistructuredDataSet(DataSet):

    def __init__(self, emr_system, dataset_name):
        """
        Initialize representation of a semi-structured dataset on S3 without Hive metadata

        :param emr_system: system config file
        :param dataset_name: name of the dataset to load.
        """
        # call super constructor
        super(SemistructuredDataSet, self).__init__(emr_system)

        # derived directories
        self.source_system = dataset_name.split("_", 1)[0]
        self.dataset_subdir = dataset_name.split("_", 1)[-1]
        self.table_lake = self.dataset_subdir
        self.db_table_lake = self.dataset_subdir
        self.dir_landing_source_system = os.path.join(
            ConfigService.Protocols.S3 + self.emr_system.bucket_landing,
            self.emr_system.environment,
            self.source_system
        )
        self.dir_landing_table = os.path.join(self.dir_landing_source_system, self.dataset_subdir)
        self.dir_landing_delta_table = os.path.join(self.dir_landing_table, self.emr_system.subdir_delta_table)
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

        self.dir_lake_table = os.path.join(self.dir_lake_source_system, self.dataset_subdir)
        self.dir_lake_final = os.path.join(self.dir_lake_table, self.emr_system.subdir_data)
        self.dir_lake_backup = os.path.join(self.dir_lake_table, self.emr_system.subdir_data_backup)

        # apps
        self.dir_apps_system = os.path.join(self.emr_system.dir_apps_loading, self.source_system)
        self.dir_apps_table = os.path.join(self.dir_apps_system, self.dataset_subdir)
        self.dir_apps_full_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_full_load)
        self.dir_apps_delta_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_delta_load)
        self.dir_apps_delta_lake_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_delta_lake_load)
        self.dir_apps_append_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_append_load)

    def drop_datasets(self):
        """
        Deletes the landing and lake folders associated with a semi-structured dataset
        """
        folders_to_drop = [self.dir_landing_table, self.dir_lake_table]
        try:
            for folder in folders_to_drop:
                logging.info("Deleting data from '{}'.".format(folder))
                self.emr_system.s3_util.delete_objects(folder)
            logging.info("Dataset '{}.{}' has been successfully deleted"
                         .format(self.source_system, self.dataset_subdir))
            return 1

        except Exception as e:
            logging.error("Failed to perform deletion of '{}.{}' dataset: {}"
                          .format(self.source_system, self.dataset_subdir, e))
            return 0
