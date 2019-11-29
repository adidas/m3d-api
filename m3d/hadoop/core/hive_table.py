from m3d.system import table
from m3d.util import util
from m3d.util.hql_generator import HQLGenerator
from m3d.util.util import Util


class HiveTable(table.Table):
    TEMP_TS_COLUMN_NAME = "__temp_timestamp_column__"

    class TableLoadType(object):
        FULL = "FullLoad"
        DELTA = "DeltaLoad"
        APPEND = "AppendLoad"

    def __init__(
            self,
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            **kwargs
    ):
        """
        Initialize Hive table

        :param config: system config file
        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param destination_table: destination table code
        """

        # call super constructor
        super(HiveTable, self).__init__(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            **kwargs
        )

    @staticmethod
    def _get_create_database_if_not_exists(database_name):
        return HQLGenerator.generate_create_database_if_not_exits(database_name).with_semicolon()

    def _get_create_landing_statement(self, table_location):
        table_properties = {
            "serialization.encoding": "UTF-8",
        }

        if int(self.header_lines) > 0:
            table_properties["skip.header.line.count"] = str(self.header_lines)

        return HQLGenerator.CreateDSVTableStatementBuilder(
            self.db_table_landing,
            table_location,
            self.columns_lake,
            self.delimiter
        ).with_properties(table_properties).build(is_external=True)

    def _get_create_lake_statement(self, table_location):
        def create_statement(_columns, _target_partitions=None):
            return HQLGenerator.CreateParquetTableStatementBuilder(self.db_table_lake, table_location, _columns) \
                .partitioned_by(_target_partitions) \
                .with_properties({"serialization.encoding": "UTF-8"}) \
                .build(is_external=True)

        if self.partitioned_by in Util.defined_partitions:
            return create_statement(self.columns_lake, Util.get_typed_target_partitions_hive(self.partitioned_by))
        elif len(self.partitioned_by) > 0:
            matched_columns = list(filter(lambda x: x[0] == self.partitioned_by, self.columns_lake))
            if len(matched_columns) > 0:
                # when table is partitioned by one of its columns
                # then partition column should to excluded from list of regular columns
                columns = filter(lambda x: x[0] != self.partitioned_by, self.columns_lake)
                target_partitions = [(matched_columns[0][0], matched_columns[0][1])]
                return create_statement(columns, target_partitions)
            else:
                raise Exception("Partitioned field doesn't match any column".format(self.partitioned_by))
        else:
            return create_statement(self.columns_lake)

    def _get_create_lakeout_statement(self):
        projection_columns = self.get_projection_columns(self.get_lake_column_names(), self.columns_lakeout)
        select_statement = HQLGenerator.SelectStatementBuilder(self.db_table_lake, projection_columns).build()
        return HQLGenerator.generate_create_view_as_select(self.db_view_lake_out, select_statement)

    def _get_drop_lakeout_statement(self):
        return HQLGenerator.generate_drop_view_if_exists(self.db_view_lake_out)

    def get_target_partitions(self):
        if not self.partitioned_by or self.partitioned_by in util.Util.defined_partitions:
            return util.Util.get_target_partitions_list(self.partitioned_by)
        else:
            return self.partitioned_by

    def create_tables(self):
        raise NotImplementedError("Subclasses should implement HiveTable.create_tables() method.")

    def create_lake_out_view(self):
        raise NotImplementedError("Subclasses should implement HiveTable.create_lake_out_view() method.")
