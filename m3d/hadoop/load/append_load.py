from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop


class AppendLoad(LoadHadoop):
    def __init__(self, execution_system, data_set, load_params):
        """
        Initialize append load class for S3 tables.

        :param execution_system an instance of EMRSystem
        :param data_set: destination table
        """

        self.data_set = data_set
        self.load_params = load_params

        super(AppendLoad, self).__init__(execution_system, data_set)

    def build_params(self):
        null_value = self.load_params.get("null_value", None)
        quote_character = self.load_params.get("quote_character", None)
        file_format = self.load_params.get("file_format", "dsv")
        verify_schema = self.load_params.get("verify_schema", None)
        schema = self.load_params.get("schema", None)
        compute_table_statistics = self.load_params.get("compute_table_statistics", None)
        # data_type can be one of DataType.STRUCTURED, DataType.SEMISTRUCTURED or DataType.UNSTRUCTURED
        data_type = self.load_params.get("data_type", None)

        if hasattr(self._data_set, "delimiter"):
            delimiter = bytes(self._data_set.delimiter, "utf-8").decode("unicode_escape")
        else:
            delimiter = None

        if hasattr(self._data_set, "header_lines"):
            has_header = int(self._data_set.header_lines) > 0
        else:
            has_header = None

        params = AppendLoadConfiguration(
            self._data_set.db_table_lake,
            self._data_set.dir_landing_final,
            self._data_set.dir_landing_header,
            self.load_params["partition_columns"],
            self.load_params["regex_filename"],
            file_format=file_format,
            delimiter=delimiter,
            has_header=has_header,
            null_value=null_value,
            quote_character=quote_character,
            compute_table_statistics=compute_table_statistics,
            schema=schema,
            verify_schema=verify_schema,
            target_dir=self._data_set.dir_lake_final,
            data_type=data_type
        )

        self._validate_params(params)
        return params

    def get_scala_class(self):
        return ScalaClasses.APPEND_LOAD

    def get_load_type(self):
        return HiveTable.TableLoadType.APPEND

    def _get_remote_config_dir(self):
        return self._data_set.dir_apps_append_load

    def _get_load_load_tag(self):
        return self._data_set.config_service.tag_append_load

    @staticmethod
    def _validate_params(params):
        if len(params.partition_columns) != len(params.regex_filename):
            message = "Lengths of partition_columns and regex_filename do not match:\n{}\n{}".format(
                params.partition_columns,
                params.regex_filename
            )
            raise M3DIllegalArgumentException(message)


class AppendLoadConfiguration(object):

    def __init__(
            self,
            target_table,
            source_dir,
            header_dir,
            partition_columns,
            regex_filename,
            file_format,
            delimiter=None,
            has_header=None,
            null_value=None,
            quote_character=None,
            compute_table_statistics=None,
            schema=None,
            verify_schema=None,
            target_dir=None,
            data_type=None
    ):
        self.target_table = target_table
        self.source_dir = source_dir
        self.header_dir = header_dir
        self.partition_columns = partition_columns
        self.regex_filename = regex_filename
        self.file_format = file_format

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
