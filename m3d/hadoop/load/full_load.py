from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop
from m3d.util import util


class FullLoad(LoadHadoop):

    def __init__(self, execution_system, dataset, load_params):
        """
        Initialize Load for FullLoad

        :param execution_system: execution system
        :param dataset:
        :param load_params:
        """
        self._load_params = load_params
        self._dataset = dataset

        # call super constructor
        super(FullLoad, self). __init__(execution_system, dataset)

    def build_params(self):
        null_value = self._load_params.get("null_value", None)
        compute_table_statistics = self._load_params.get("compute_table_statistics", None)
        data_type = self._load_params.get("data_type", None)
        reader_mode = self._load_params.get("reader_mode", None)

        full_load_params = FullLoadParams(
            self._dataset.db_table_lake,
            self._dataset.dir_landing_data,
            util.Util.get_target_partitions_list(self._dataset.partitioned_by),
            self._dataset.partition_column,
            self._dataset.delimiter,
            (self._dataset.header_lines != 0),
            self._dataset.dir_lake_final,
            self._dataset.dir_lake_backup,
            self._dataset.partition_column_format,
            null_value=null_value,
            compute_table_statistics=compute_table_statistics,
            data_type=data_type,
            reader_mode=reader_mode
        )
        return full_load_params

    def get_scala_class(self):
        return ScalaClasses.FULL_LOAD

    def get_load_type(self):
        return HiveTable.TableLoadType.FULL

    def _get_remote_config_dir(self):
        return self._dataset.dir_apps_full_load

    def _get_load_load_tag(self):
        return self._dataset.config_service.tag_full_load


class FullLoadParams(object):
    """
    Class resembling the contents of the algorithm parameter file
    """

    def __init__(
            self,
            table_name,
            input_directory,
            target_partitions,
            partition_column,
            delimiter,
            has_header,
            current_dir,
            backup_dir,
            partition_column_format,
            null_value=None,
            compute_table_statistics=None,
            data_type=None,
            reader_mode=None
    ):
        self.file_format = "dsv"  # TODO: Make this dynamic in the future. For now, we are only dealing with csv files.
        self.target_table = table_name
        self.source_dir = input_directory
        self.target_partitions = target_partitions
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
        if data_type is not None:
            self.data_type = data_type
        if reader_mode is not None:
            self.reader_mode = reader_mode
