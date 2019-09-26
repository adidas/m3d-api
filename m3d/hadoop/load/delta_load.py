from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop
from m3d.util import util


class DeltaLoad(LoadHadoop):

    def __init__(self, execution_system, table):
        """
        Initialize delta load class for S3 tables.

        :param execution_system an instance of EMRSystem
        :param table: destination table code
        """

        # call super constructor
        super(DeltaLoad, self).__init__(execution_system, table)

    def build_params(self):
        params = DeltaLoadParams(
            self._table.db_table_lake,
            self._table.dir_lake_final,
            self._table.dir_landing_data,
            ["m3d_timestamp", "datapakid", "partno", "record"],
            self._table.business_key,
            util.Util.get_partition_columns_list(self._table.partitioned_by),
            self._table.partition_column,
            self._table.partition_column_format
        )
        return params

    def get_load_type(self):
        return HiveTable.TableLoadType.DELTA

    def get_scala_class(self):
        return ScalaClasses.DELTA_LOAD

    def _get_remote_config_dir(self):
        return self._table.dir_apps_delta_load

    def _get_load_load_tag(self):
        return self._table.config_service.tag_delta_load


class DeltaLoadParams(object):
    """
    Class resembling the contents of the algorithm parameter file
    """

    def __init__(
            self,
            active_records_table_lake,
            active_records_dir_lake,
            delta_records_file_path,
            technical_key,
            business_key,
            partition_columns,
            partition_column,
            partition_column_format
    ):
        self.active_records_table_lake = active_records_table_lake
        self.active_records_dir_lake = active_records_dir_lake
        self.delta_records_file_path = delta_records_file_path
        self.technical_key = technical_key
        self.business_key = business_key
        self.partition_columns = partition_columns
        self.partition_column = partition_column
        self.partition_column_format = partition_column_format
