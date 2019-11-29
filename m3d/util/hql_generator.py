import functools
import os
import re


class HQLGenerator(object):
    ESCAPE_KEYWORDS_CHAR = "`"

    class Statement(object):
        SPACE_CHAR = " "
        RETURN_CHAR = "\n"
        MULTIPLE_SPACE_REGEX = re.compile(r"[ \t]{2,}")
        MULTIPLE_RETURN_REGEX = re.compile(r"[\n]{2,}|\n[\s]+|[\s]+\n")

        def __init__(self, hql):
            self._hql = self._beautify(hql)

        def no_semicolon(self):
            return self._hql

        def with_semicolon(self):
            return "{};".format(self._hql)

        @staticmethod
        def _beautify(hql):
            format_hql = functools.reduce(lambda f1, f2: lambda x: f2(f1(x)), [
                lambda s: HQLGenerator.Statement.MULTIPLE_SPACE_REGEX.sub(HQLGenerator.Statement.SPACE_CHAR, s),
                lambda s: HQLGenerator.Statement.MULTIPLE_RETURN_REGEX.sub(HQLGenerator.Statement.RETURN_CHAR, s)
            ])
            return format_hql(hql).strip("\n ")

    class CreateHiveTableStatementBuilder(object):

        def __init__(self, table_name, table_location, columns):
            self._table_name = table_name
            self._columns = columns
            self._target_partitions = []
            self._table_properties = {}

            # Making sure that table_location has exactly one trailing slash.
            self._table_location = os.path.join(table_location, "")

        def with_properties(self, tbl_properties):
            if tbl_properties is None:
                self._table_properties = {}
            else:
                self._table_properties = tbl_properties
            return self

        def partitioned_by(self, target_partitions):
            if target_partitions is None:
                self._target_partitions = []
            else:
                self._target_partitions = target_partitions
            return self

        def build(self, is_external=True):
            raise NotImplementedError("Unable to build a generic create table statement")

        def _generate_ddl(self, format_str, is_external):
            def make_tblproperties_str():
                if len(self._table_properties) > 0:
                    return "TBLPROPERTIES({table_properties})".format(
                        table_properties=self._get_table_properties_as_string(self._table_properties)
                    )
                else:
                    return ""

            def make_partition_by_str():
                if len(self._target_partitions) > 0:
                    return "PARTITIONED BY ({partition_column_definitions})".format(
                        partition_column_definitions=self._get_columns_as_string(self._target_partitions)
                    )
                else:
                    return ""

            def make_create_str():
                table_type = "EXTERNAL TABLE" if is_external else "TABLE"
                return "CREATE {table_type} {table_name}({column_definitions})".format(
                    table_type=table_type,
                    table_name=self._table_name,
                    column_definitions=self._get_columns_as_string(self._columns)
                )

            def make_location_str():
                return "LOCATION '{table_location}'".format(table_location=self._table_location)

            hql_statement = """
                {create_table_str}
                {partition_by_str}
                {format_str}
                {location_str}
                {tblproperties_str}
            """.format(
                create_table_str=make_create_str(),
                partition_by_str=make_partition_by_str(),
                format_str=format_str,
                location_str=make_location_str(),
                tblproperties_str=make_tblproperties_str()
            )
            return HQLGenerator.Statement(hql_statement)

        @staticmethod
        def _get_table_properties_as_string(table_properties):
            return ", ".join(["\"{}\"=\"{}\"".format(k, v) for k, v in table_properties.items()])

        @staticmethod
        def _get_columns_as_string(columns):
            def format_column(column):
                if isinstance(column, tuple) or isinstance(column, list):
                    return "{} {}".format(column[0], column[1])
                else:
                    raise AttributeError("Column definition should be an instance of Tuple or List")

            return ", ".join(map(format_column, columns))

    class CreateDSVTableStatementBuilder(CreateHiveTableStatementBuilder):

        def __init__(self, table_name, table_location, columns, delimiter):
            super(HQLGenerator.CreateDSVTableStatementBuilder, self).__init__(table_name, table_location, columns)
            self._format = self._create_format_string(delimiter)

        def build(self, is_external=True):
            return self._generate_ddl(self._format, is_external)

        @staticmethod
        def _create_format_string(delimiter):
            return "ROW FORMAT DELIMITED FIELDS TERMINATED BY '{}' ESCAPED BY '{}' LINES TERMINATED BY '{}'".format(
                delimiter, "\\\\", "\\n"
            )

    class CreateParquetTableStatementBuilder(CreateHiveTableStatementBuilder):

        def __init__(self, table_name, table_location, columns):
            super(HQLGenerator.CreateParquetTableStatementBuilder, self).__init__(table_name, table_location, columns)
            self._format = "STORED AS PARQUET"

        def build(self, is_external=True):
            return self._generate_ddl(self._format, is_external)

    class SelectStatementBuilder(object):

        def __init__(self, from_table, columns):
            self._from_table = from_table
            self._columns = columns
            self._distribute_columns = []

        def build(self):
            def make_select_str():
                return "SELECT {columns_str}".format(columns_str=self._get_columns_as_string())

            def make_from_str():
                return "FROM {from_table}".format(from_table=self._from_table)

            def make_distribute_by_str():
                if len(self._distribute_columns) > 0:
                    return "DISTRIBUTE BY ({})".format(", ".join(self._distribute_columns))
                else:
                    return ""

            hql_statement = """
                {select_str}
                {from_str}
                {distribute_str}
            """.format(
                select_str=make_select_str(),
                from_str=make_from_str(),
                distribute_str=make_distribute_by_str()
            )
            return HQLGenerator.Statement(hql_statement)

        def distribute_by(self, columns):
            self._distribute_columns = columns
            return self

        def _get_columns_as_string(self):
            def format_column(column):
                if isinstance(column, tuple) or isinstance(column, list):
                    return "{} AS {}".format(column[0], column[1]) if len(column) > 1 else column[0]
                else:
                    raise AttributeError("Column definition should be an instance of Tuple or List")

            return ", ".join(map(format_column, self._columns))

    def __init__(self):
        super(HQLGenerator, self).__init__()

    @staticmethod
    def generate_alter_table_location(table_name, table_location):
        hql_statement = 'ALTER TABLE {} SET LOCATION "{}"'.format(table_name, table_location)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_drop_table(table_name):
        hql_statement = 'DROP TABLE {}'.format(table_name)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_drop_table_if_exists(table_name):
        hql_statement = 'DROP TABLE IF EXISTS {}'.format(table_name)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_drop_view(view_name):
        hql_statement = 'DROP VIEW {}'.format(view_name)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_drop_view_if_exists(view_name):
        hql_statement = 'DROP VIEW IF EXISTS {}'.format(view_name)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_describe(entity):
        hql_statement = "DESCRIBE {}".format(entity)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_repair_table(table_name):
        hql_statement = 'MSCK REPAIR TABLE {}'.format(table_name)
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_create_view_as_select(target_view, select_statement):
        hql_statement = "\n".join([
            "CREATE VIEW {}".format(target_view),
            "AS",
            select_statement.no_semicolon()
        ])
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_insert_as_select(target_table, select_statement, target_partitions=None):
        def make_partition_str():
            if target_partitions is not None and len(target_partitions) > 0:
                return "PARTITION({})".format(", ".join(target_partitions))
            else:
                return ""

        def make_insert_str():
            return "INSERT INTO TABLE {}".format(target_table)

        hql_statement = """
            {insert_str} {partition_str}
            {select_str}
        """.format(
            insert_str=make_insert_str(),
            partition_str=make_partition_str(),
            select_str=select_statement.no_semicolon()
        )
        return HQLGenerator.Statement(hql_statement)

    @staticmethod
    def generate_create_database_if_not_exits(database_name):
        hql_statement = "CREATE DATABASE IF NOT EXISTS {}".format(database_name)
        return HQLGenerator.Statement(hql_statement)
