from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop
from m3d.util import util


class FullLoad(LoadHadoop):

    def __init__(self, execution_system, table):
        """
        Initialize Load for FullLoad

        :param execution_system: execution system
        :param table: merged algorithm configuration object
        """
        # # call super constructor
        super(FullLoad, self). __init__(execution_system, table)
        try:
            self._load_params = self._read_acon_params(execution_system, self._table)
        except Exception:
            self._load_params = {}

    def build_params(self):
        if "null_value" in self._load_params:
            null_value = self._load_params["null_value"]
        else:
            null_value = None
        if "compute_table_statistics" in self._load_params:
            compute_table_statistics = self._load_params["compute_table_statistics"]
        else:
            compute_table_statistics = None
        full_load_params = FullLoadParams(
            self._table.db_table_lake,
            self._table.dir_landing_data,
            util.Util.get_partition_columns_list(self._table.partitioned_by),
            self._table.partition_column,
            self._table.delimiter,
            (self._table.header_lines != 0),
            self._table.dir_lake_final,
            self._table.dir_lake_backup,
            self._table.partition_column_format,
            null_value,
            compute_table_statistics
        )
        return full_load_params

    def get_scala_class(self):
        return ScalaClasses.FULL_LOAD

    def get_load_type(self):
        return HiveTable.TableLoadType.FULL

    def _get_remote_config_dir(self):
        return self._table.dir_apps_full_load

    def _get_load_load_tag(self):
        return self._table.config_service.tag_full_load


class FullLoadParams(object):
    """
    Class resembling the contents of the algorithm parameter file
    """

    def __init__(
            self,
            table_name,
            input_directory,
            partition_columns,
            partition_column,
            delimiter,
            has_header,
            current_dir,
            backup_dir,
            partition_column_format,
            null_value=None,
            compute_table_statistics=None
    ):
        self.file_format = "dsv"  # TODO: Make this dynamic in the future. For now, we are only dealing with csv files.
        self.target_table = table_name
        self.source_dir = input_directory
        self.partition_columns = partition_columns
        self.partition_column = partition_column
        self.delimiter = delimiter
        self.has_header = has_header
        self.current_dir = current_dir
        self.backup_dir = backup_dir
        self.partition_column_format = partition_column_format
        if null_value is not None:
            self.null_value = null_value
        if compute_table_statistics is not None:
            self.compute_table_statistics = compute_table_statistics
