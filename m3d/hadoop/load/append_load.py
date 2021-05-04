from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop


class AppendLoad(LoadHadoop):
    def __init__(self, execution_system, dataset, load_params):
        """
        Initialize append load class for S3 tables.

        :param execution_system an instance of EMRSystem
        :param dataset: destination table
        """

        self.dataset = dataset
        self.load_params = load_params

        super(AppendLoad, self).__init__(execution_system, dataset)

    def build_params(self):
        null_value = self.load_params.get("null_value", None)
        quote_character = self.load_params.get("quote_character", None)
        file_format = self.load_params.get("file_format", "dsv")
        verify_schema = self.load_params.get("verify_schema", None)
        schema = self.load_params.get("schema", None)
        compute_table_statistics = self.load_params.get("compute_table_statistics", None)
        # data_type can be one of DataType.STRUCTURED, DataType.SEMISTRUCTURED or DataType.UNSTRUCTURED
        data_type = self.load_params.get("data_type", None)
        reader_mode = self.load_params.get("reader_mode", None)
        # property to ignore partitions in schema check when using using failfast and permissive (they are introduced
        # artificially in m3d)
        drop_date_derived_columns = self.load_params.get("drop_date_derived_columns", None)
        # property to add corrupt_record column when using permissive (for debug purposes)
        add_corrupt_record_column = self.load_params.get("add_corrupt_record_column", None)
        metadata_update_strategy = self.load_params.get("metadata_update_strategy", None)
        partition_column = self.load_params.get("partition_column", None)
        partition_column_format = self.load_params.get("partition_column_format", None)
        date_format = self.load_params.get("date_format", None)
        regex_filename = self.load_params.get("regex_filename", None)
        multi_line = self.load_params.get("multi_line", None)

        delimiter = self.load_params.get("delimiter", None)
        if delimiter:
            delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")
        elif hasattr(self._dataset, "delimiter"):
            delimiter = bytes(self._dataset.delimiter, "utf-8").decode("unicode_escape")
        else:
            delimiter = None

        has_header = self.load_params.get("has_header", None)
        if has_header is None and hasattr(self._dataset, "header_lines"):
            has_header = int(self._dataset.header_lines) > 0
        else:
            has_header = None

        params = AppendLoadConfiguration(
            self._dataset.db_table_lake,
            self._dataset.dir_landing_final,
            self._dataset.dir_landing_header,
            self.load_params["target_partitions"],
            regex_filename=regex_filename,
            metadata_update_strategy=metadata_update_strategy,
            file_format=file_format,
            delimiter=delimiter,
            has_header=has_header,
            null_value=null_value,
            quote_character=quote_character,
            compute_table_statistics=compute_table_statistics,
            schema=schema,
            verify_schema=verify_schema,
            target_dir=self._dataset.dir_lake_final,
            data_type=data_type,
            reader_mode=reader_mode,
            drop_date_derived_columns=drop_date_derived_columns,
            add_corrupt_record_column=add_corrupt_record_column,
            partition_column=partition_column,
            partition_column_format=partition_column_format,
            date_format=date_format,
            multi_line=multi_line
        )

        self._validate_params(params)
        return params

    def get_scala_class(self):
        return ScalaClasses.APPEND_LOAD

    def get_load_type(self):
        return HiveTable.TableLoadType.APPEND

    def _get_remote_config_dir(self):
        return self._dataset.dir_apps_append_load

    def _get_load_load_tag(self):
        return self._dataset.config_service.tag_append_load

    @staticmethod
    def _validate_params(params):
        if not hasattr(params, "partition_column") and len(params.target_partitions) != len(params.regex_filename):
            message = "Lengths of target_partitions and regex_filename do not match:\n{}\n{}".format(
                params.target_partitions,
                params.regex_filename
            )
            raise M3DIllegalArgumentException(message)


class AppendLoadConfiguration(object):

    def __init__(
            self,
            target_table,
            source_dir,
            header_dir,
            target_partitions,
            file_format,
            regex_filename=None,
            metadata_update_strategy=None,
            delimiter=None,
            has_header=None,
            null_value=None,
            quote_character=None,
            compute_table_statistics=None,
            schema=None,
            verify_schema=None,
            target_dir=None,
            data_type=None,
            reader_mode=None,
            drop_date_derived_columns=None,
            add_corrupt_record_column=None,
            partition_column=None,
            partition_column_format=None,
            date_format=None,
            multi_line=None
    ):
        self.target_table = target_table
        self.source_dir = source_dir
        self.header_dir = header_dir
        self.target_partitions = target_partitions
        self.file_format = file_format

        if metadata_update_strategy is not None:
            self.metadata_update_strategy = metadata_update_strategy
        if delimiter is not None:
            self.delimiter = delimiter
        if has_header is not None:
            self.has_header = has_header
        if null_value is not None:
            self.null_value = null_value
        if quote_character is not None:
            self.quote_character = quote_character
        if compute_table_statistics is not None:
            self.compute_table_statistics = compute_table_statistics
        if verify_schema is not None:
            self.verify_schema = verify_schema
        if target_dir is not None:
            self.target_dir = target_dir
        if schema is not None:
            self.schema = schema
        if data_type is not None:
            self.data_type = data_type
        if reader_mode is not None:
            self.reader_mode = reader_mode
        if drop_date_derived_columns is not None:
            self.drop_date_derived_columns = drop_date_derived_columns
        if add_corrupt_record_column is not None:
            self.add_corrupt_record_column = add_corrupt_record_column
        if partition_column is not None:
            self.partition_column = partition_column
        if partition_column_format is not None:
            self.partition_column_format = partition_column_format
        if date_format is not None:
            self.date_format = date_format
        if regex_filename is not None:
            self.regex_filename = regex_filename
        if multi_line is not None:
            self.multi_line = multi_line
