from m3d.system.data_system import DataSystem


class DataSet(DataSystem):
    def __init__(self, emr_system):
        """
        Initialize representation of Hive table on S3

        :param emr_system: execution system
        """
        # call super constructor
        super(DataSet, self).__init__(
            emr_system.config,
            emr_system.source_system,
            emr_system.database,
            emr_system.environment
        )

        self.emr_system = emr_system
