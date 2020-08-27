import json

from m3d.system import abstract_system


class DataSystem(abstract_system.AbstractSystem):
    class SystemTechnology(object):
        HIVE = "hive"

    class StorageType(object):
        S3 = "s3"

    class DatabaseType(object):
        EMR = "emr"

    class DirectoryName(object):
        DATA_BACKUP = "data_backup/"

    def __init__(self, config, source_system, database, environment):
        """
        Initialize System Config

        :param config: global system config file
        :param source_system: system code
        :param database: database code
        :param environment: environment code
        """
        # call super constructor
        super(DataSystem, self).__init__(config, source_system, database)

        # store parameters
        self.environment = environment
        self.source_system = source_system
        self.database = database

        # init destination schemas
        self.db_landing = None
        self.db_lake = None
        self.db_lake_out = None
        self.db_mart_mod = None
        self.db_mart_cal = None
        self.db_mart_out = None
        self.db_m3d = None
        self.db_work = None
        self.db_error = None
        self.db_stages = None

        # open system config file
        with open(self.config_service.get_scon_path(source_system, database)) as data_file:
            params = json.load(data_file)

        self.database_type = params["database_type"]
        self.storage_type = params["storage_type"]

        if self.environment is not None:
            self.db_landing = params["environments"][self.environment]["schemas"]["landing"]
            self.db_lake = params["environments"][self.environment]["schemas"]["lake"]
            self.db_lake_out = params["environments"][self.environment]["schemas"]["lake_out"]
            self.db_mart_mod = params["environments"][self.environment]["schemas"]["mart_mod"]
            self.db_mart_cal = params["environments"][self.environment]["schemas"]["mart_cal"]
            self.db_mart_out = params["environments"][self.environment]["schemas"]["mart_out"]
            self.db_m3d = params["environments"][self.environment]["schemas"]["m3d"]
            self.db_work = params["environments"][self.environment]["schemas"]["work"]
            self.db_error = params["environments"][self.environment]["schemas"]["error"]
            self.db_stages = [self.db_landing, self.db_lake, self.db_lake_out, self.db_mart_mod, self.db_mart_cal,
                              self.db_mart_out]
