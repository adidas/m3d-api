from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.load.load_hadoop import LoadHadoop


class AppendLoad(LoadHadoop):
    def __init__(self, execution_system, table):
        """
        Initialize append load class for S3 tables.

        :param execution_system an instance of EMRSystem
        :param table: destination table code
        """

        # call super constructor
        super(AppendLoad, self).__init__(execution_system, table)

        self._load_params = self._read_acon_params(execution_system, self._table)

    def build_params(self):
        if "null_value" in self._load_params:
            null_value = self._load_params["null_value"]
        else:
            null_value = None

        if "quote_character" in self._load_params:
            quote_character = self._load_params["quote_character"]
        else:
            quote_character = None

        if "file_format" in self._load_params:
            file_format = self._load_params["file_format"]
        else:
            file_format = "dsv"

        if "compute_table_statistics" in self._load_params:
            compute_table_statistics = self._load_params["compute_table_statistics"]
        else:
            compute_table_statistics = None

        delimiter = bytes(self._table.delimiter, "utf-8").decode("unicode_escape")
        has_header = int(self._table.header_lines) > 0

        params = AppendLoadConfiguration(
            self._table.db_table_lake,
            self._table.dir_landing_final,
            self._table.dir_landing_header,
            self._load_params["partition_columns"],
            self._load_params["regex_filename"],
            delimiter,
            has_header,
            file_format,
            null_value,
            quote_character,
            compute_table_statistics
        )

        self._validate_params(params)
        return params

    def get_scala_class(self):
        return ScalaClasses.APPEND_LOAD

    def get_load_type(self):
        return HiveTable.TableLoadType.APPEND

    def _get_remote_config_dir(self):
        return self._table.dir_apps_append_load

    def _get_load_load_tag(self):
        return self._table.config_service.tag_append_load

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
            delimiter,
            has_header,
            file_format,
            null_value=None,
            quote_character=None,
            compute_table_statistics=None
    ):
        self.target_table = target_table
        self.source_dir = source_dir
        self.header_dir = header_dir
        self.partition_columns = partition_columns
        self.regex_filename = regex_filename
        self.delimiter = delimiter
        self.has_header = has_header
        self.file_format = file_format

        if null_value is not None:
            self.null_value = null_value
        if quote_character is not None:
            self.quote_character = quote_character
        if compute_table_statistics is not None:
            self.compute_table_statistics = compute_table_statistics
