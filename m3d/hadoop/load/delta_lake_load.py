from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop
from m3d.util import util


class DeltaLakeLoad(LoadHadoop):

    def __init__(self, execution_system, dataset, load_params):
        """
        Initialize delta lake load class for S3 tables.

        :param execution_system an instance of EMRSystem
        :param dataset: destination table code
        :param load_params: load parameters
        """

        self._load_params = load_params
        self._dataset = dataset
        super(DeltaLakeLoad, self).__init__(execution_system, self._dataset)

    def build_params(self):
        params = DeltaLakeLoadParams(
            self._load_params.get('file_format', 'parquet'),
            self._load_params.get('file_delimiter', None),
            self._load_params.get('has_header', None),
            self._load_params.get('null_value', None),
            self._load_params.get('quote_char', None),
            self._load_params.get('multi_line', None),
            self._load_params.get('schema', None),
            self._dataset.dir_landing_data,
            self._load_params.get('source_dir_suffix', '*'),
            self._dataset.dir_landing_delta_table,
            self._dataset.business_key,
            self._load_params.get('condensation_key', ["actrequest_timestamp", "datapakid", "partno", "record"]),
            self._load_params.get('record_mode_column', 'recordmode'),
            self._load_params.get('init_condensation', None),
            self._load_params.get('init_condensation_with_record_mode', None),
            self._load_params.get('records_to_condense', [None, "", "N", "R", "D", "X"]),
            self._load_params.get('records_to_delete', ["R", "D", "X"]),
            self._dataset.db_table_lake,
            util.Util.get_target_partitions_list(self._dataset.partitioned_by),
            self._dataset.partition_column,
            self._dataset.partition_column_format,
            self._load_params.get('affected_partitions_merge', None),
            self._load_params.get('output_partitions_num', None),
            self._load_params.get('compute_table_statistics', None)
        )
        return params

    def get_load_type(self):
        return HiveTable.TableLoadType.DELTALAKE

    def get_scala_class(self):
        return ScalaClasses.DELTA_LAKE_LOAD

    def _get_remote_config_dir(self):
        return self._dataset.dir_apps_delta_lake_load

    def _get_load_load_tag(self):
        return self._dataset.config_service.tag_delta_lake_load


class DeltaLakeLoadParams(object):
    """
    Class resembling the contents of the algorithm parameter file
    """

    def __init__(
            self,
            file_format,
            file_delimiter,
            has_header,
            null_value,
            quote_char,
            multi_line,
            schema,
            source_dir,
            source_dir_suffix,
            delta_table_dir,
            business_key,
            condensation_key,
            record_mode_column,
            init_condensation,
            init_condensation_with_record_mode,
            records_to_condense,
            records_to_delete,
            target_table,
            target_partitions,
            partition_column,
            partition_column_format,
            affected_partitions_merge,
            output_partitions_num,
            compute_table_statistics
    ):
        self.file_format = file_format
        if file_delimiter is not None:
            self.file_delimiter = file_delimiter
        if has_header is not None:
            self.has_header = has_header
        if null_value is not None:
            self.null_value = null_value
        if quote_char is not None:
            self.quote_char = quote_char
        if multi_line is not None:
            self.multi_line = multi_line
        if schema is not None:
            self.schema = schema
        self.source_dir = source_dir + source_dir_suffix
        self.delta_table_dir = delta_table_dir
        self.business_key = business_key
        self.condensation_key = condensation_key
        self.record_mode_column = record_mode_column
        if init_condensation is not None:
            self.init_condensation = init_condensation
        if init_condensation_with_record_mode is not None:
            self.init_condensation_with_record_mode = init_condensation_with_record_mode
        self.records_to_condense = records_to_condense
        self.records_to_delete = records_to_delete
        self.target_table = target_table
        self.target_partitions = target_partitions
        self.partition_column = partition_column
        self.partition_column_format = partition_column_format
        if affected_partitions_merge is not None:
            self.affected_partitions_merge = affected_partitions_merge
        if output_partitions_num is not None:
            self.output_partitions_num = output_partitions_num
        if compute_table_statistics is not None:
            self.compute_table_statistics = compute_table_statistics
