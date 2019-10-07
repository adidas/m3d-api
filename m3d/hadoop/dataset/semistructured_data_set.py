import os

from m3d.config.config_service import ConfigService
from m3d.hadoop.dataset.data_set import DataSet


class SemistructuredDataSet(DataSet):

    def __init__(self, emr_system, data_set_name):
        """
        Initialize representation of Hive table on S3

        :param emr_system: system config file
        :param data_set_name: flag for cluster mode
        """
        # call super constructor
        super(SemistructuredDataSet, self).__init__(emr_system)

        # derived directories
        self.source_system = data_set_name.split("_", 1)[0]
        self.data_set_subdir = data_set_name.split("_", 1)[-1]
        self.table_lake = self.data_set_subdir
        self.db_table_lake = self.data_set_subdir
        self.dir_landing_source_system = os.path.join(
            ConfigService.Protocols.S3 + self.emr_system.bucket_landing,
            self.emr_system.environment,
            self.source_system
        )
        self.dir_landing_table = os.path.join(self.dir_landing_source_system, self.data_set_subdir)
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

        self.dir_lake_table = os.path.join(self.dir_lake_source_system, self.data_set_subdir)
        self.dir_lake_final = os.path.join(self.dir_lake_table, self.emr_system.subdir_data)
        self.dir_lake_backup = os.path.join(self.dir_lake_table, self.emr_system.subdir_data_backup)

        # apps
        self.dir_apps_system = os.path.join(self.emr_system.dir_apps_loading, self.source_system)
        self.dir_apps_table = os.path.join(self.dir_apps_system, self.data_set_subdir)
        self.dir_apps_full_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_full_load)
        self.dir_apps_delta_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_delta_load)
        self.dir_apps_append_load = os.path.join(self.dir_apps_table, self.emr_system.subdir_append_load)
