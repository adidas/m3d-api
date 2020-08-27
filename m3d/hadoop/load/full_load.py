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
        super(FullLoad, self).__init__(execution_system, dataset)

    def build_params(self):
        null_value = self._load_params.get("null_value", None)
        compute_table_statistics = self._load_params.get("compute_table_statistics", None)
        additional_task = self._load_params.get("additional_task", None)
        file_format = self._load_params.get("file_format", None)
        data_type = self._load_params.get("data_type", None)
        reader_mode = self._load_params.get("reader_mode", None)
        # property to ignore partitions in schema check when using using failfast and permissive (they are introduced
        # artificially in m3d)
        drop_date_derived_columns = self._load_params.get("drop_date_derived_columns", None)
        # property to add corrupt_record column when using permissive (for debug purposes)
        add_corrupt_record_column = self._load_params.get("add_corrupt_record_column", None)
        is_multiline_json = self._load_params.get("is_multiline_json", None)
        schema = self._load_params.get("schema", None)

        full_load_params = FullLoadParams(
            self._dataset.db_table_lake,
            self._dataset.dir_landing_data,
            util.Util.get_target_partitions_list(self._dataset.partitioned_by),
            self._dataset.partition_column,
            self._dataset.delimiter,
            (self._dataset.header_lines != 0),
            self._dataset.emr_system.subdir_data,
            self._dataset.dir_lake_final,
            self._dataset.dir_lake_backup,
            self._dataset.partition_column_format,
            null_value=null_value,
            compute_table_statistics=compute_table_statistics,
            reader_mode=reader_mode,
            drop_date_derived_columns=drop_date_derived_columns,
            add_corrupt_record_column=add_corrupt_record_column,
            additional_task=additional_task,
            file_format=file_format,
            data_type=data_type,
            is_multiline_json=is_multiline_json,
            schema=schema,
            output_files_num=self._load_params.get('output_files_num', None),
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
            base_data_dir,
            current_dir,
            backup_dir,
            partition_column_format,
            null_value=None,
            compute_table_statistics=None,
            reader_mode=None,
            drop_date_derived_columns=None,
            add_corrupt_record_column=None,
            additional_task=None,
            file_format=None,
            data_type=None,
            is_multiline_json=None,
            schema=None,
            output_files_num=None
    ):
        self.target_table = table_name
        self.source_dir = input_directory
        self.target_partitions = target_partitions
        self.partition_column = partition_column
        self.delimiter = delimiter
        self.has_header = has_header
        self.base_data_dir = base_data_dir
        self.current_dir = current_dir
        self.backup_dir = backup_dir
        self.partition_column_format = partition_column_format
        if null_value is not None:
            self.null_value = null_value
        if compute_table_statistics is not None:
            self.compute_table_statistics = compute_table_statistics
        if reader_mode is not None:
            self.reader_mode = reader_mode
        if drop_date_derived_columns is not None:
            self.drop_date_derived_columns = drop_date_derived_columns
        if add_corrupt_record_column is not None:
            self.add_corrupt_record_column = add_corrupt_record_column
        if additional_task is not None:
            self.additional_task = additional_task
        if is_multiline_json is not None:
            self.is_multiline_json = is_multiline_json
        if schema is not None:
            self.schema = schema
        if file_format is not None:
            self.file_format = file_format
        else:
            self.file_format = "dsv"
        if data_type is not None:
            self.data_type = data_type
        if output_files_num is not None:
            self.output_files_num = output_files_num
