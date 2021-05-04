import copy
import functools
import json
import os
import time
import logging
from enum import Enum

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.system import data_system
from m3d.util.util import Util


class TconxTableInitType(object):
    FROM_JSON_FILE = "jsonfile"
    FROM_PROVIDED_JSON = "providedjson"


class TconxParser(object):

    def __init__(self, tconx_file, **kwargs):
        self.tconx_file = open(tconx_file, 'r')
        self.file_representation = self._get_file_representation()

    def _get_file_representation(self):
        raise NotImplementedError

    def get_value(self, *keys, **kwargs):
        pass


class JSONTconxParser(TconxParser):

    def __init__(self, tconx_file, **kwargs):
        super(JSONTconxParser, self).__init__(tconx_file, **kwargs)

    def _get_file_representation(self):
        json_rep = json.load(self.tconx_file)
        self.tconx_file.close()

        return json_rep

    def get_value(self, *keys, **kwargs):
        def get_or_throw(data, key):
            if key in data:
                return data.get(key)
            else:
                raise KeyError

        val = functools.reduce(lambda d, key: get_or_throw(d, key), keys, self.file_representation)
        return val if isinstance(val, list) else [val]


class DataLayers(Enum):
    LANDING = 0
    LAKE = 1

    @staticmethod
    def get_layer_object(layer):
        """
        Takes a string as param to return data layer object.
        :param layer: string representing the name of the layer to be returned.
        :return: the layer object
        :exception: raises exception if the string passed does not match any layer.
        """

        if layer.lower() == "landing":
            return DataLayers.LANDING
        elif layer.lower() == "lake":
            return DataLayers.LAKE
        else:
            raise M3DIllegalArgumentException("Invalid layer: {}".format(layer))

    @staticmethod
    def return_all_layers_excluding(layer):
        return [data_layer for data_layer in DataLayers if data_layer != layer]

    @staticmethod
    def return_all_layers():
        return DataLayers

    @staticmethod
    def validate_layers(data_layers):
        if not data_layers:
            raise M3DIllegalArgumentException("No data layer has been specified.")

        valid_layers = DataLayers.return_all_layers()

        for data_layer in data_layers:
            if data_layer not in valid_layers:
                raise M3DIllegalArgumentException("Not a valid data layer: {}".format(data_layer))


class Table(data_system.DataSystem):
    INIT_TYPE_FLAG = "inittype"
    INIT_PAYLOAD = "initpayload"

    def __init__(self, config, source_system, database, environment, table, **kwargs):
        """
        Initialize table config

        :param config: system config file
        :param source_system: system code
        :param database: database code
        :param environment: environment code
        :param table: table code
        """

        # call super constructor
        super(Table, self).__init__(config, source_system, database, environment)

        if not kwargs:
            kwargs[Table.INIT_TYPE_FLAG] = TconxTableInitType.FROM_JSON_FILE

        init_type = kwargs[Table.INIT_TYPE_FLAG]

        # store parameters
        self.table = table

        # init member variables
        self.columns_src = []  # column names AND types as in source system
        self.columns_lake = []  # column names AND types for lake
        self.columns_lakeout = []  # column names for lake_out
        self.business_key = []  # list of business key columns
        self.datetime_columns = {}  # datetime columns and date formats (e.g. yyyy-MM-dd HH:mm:ss)
        self.partitioned_by = ""  # type of partition (year, month, day)
        self.partition_column = ""  # lake name of partition column
        self.partition_column_format = ""  # format of partition column

        tconx_provided_file_name = "temptconx_{0}".format(str(int(time.time() * 1000.0)))
        remove_tconx_provided_file_name = False

        logging.info("PROVIDED TYPE: {0}".format(init_type))

        if init_type == TconxTableInitType.FROM_PROVIDED_JSON or init_type == TconxTableInitType.FROM_JSON_FILE:
            if init_type == TconxTableInitType.FROM_PROVIDED_JSON:
                tconx_file_path = tconx_provided_file_name + ".json"
            else:
                tconx_file_path = self.config_service.get_tconx_path(source_system, database,
                                                                     environment, table)
            if init_type == TconxTableInitType.FROM_PROVIDED_JSON:
                with open(tconx_file_path, 'w') as writer:
                    writer.write("" if Table.INIT_PAYLOAD not in kwargs else kwargs.__getitem__(Table.INIT_PAYLOAD))

            json_parser = JSONTconxParser(tconx_file_path)

            self.source_system = json_parser.get_value("source", "system")[0]
            self.destination_technology = json_parser.get_value("destination", "technology")[0]
            self.table_src = json_parser.get_value("source", "table")[0]
            self.table_lake = json_parser.get_value("destination", "table_lake")[0]
            self.table_changelog = json_parser.get_value("destination", "table_lake_changelog")[0]
            self.table_lakeout = json_parser.get_value("destination", "table_lakeout")[0]
            self.delimiter = json_parser.get_value("source_files", "delimiter")[0]
            self.source_schema = json_parser.get_value("source", "schema")[0]

            # number of header lines in upload CSV files
            self.header_lines = json_parser.get_value("source_files", "header_lines")[0]

            self.table = self.table_lake.split("_", 1)[-1]

            # source column names
            self.columns_src = [col["name_source"] for col in json_parser.get_value("destination", "columns")]

            # lake column names AND types
            self.columns_lake = [
                [col["name_lake"],
                 self._get_data_type(
                     col["datatype"],
                     col["length"],
                     col["scale"])
                 ] for col in json_parser.get_value("destination", "columns")
            ]

            self.columns_lakeout = \
                [col["name_lakeout"] for col in json_parser.get_value("destination", "columns")]

            # business key
            self.business_key = [
                col["name_lake"]
                for col in json_parser.get_value("destination", "columns")
                if col["is_part_of_business_key"] and col["is_part_of_business_key"].lower() == "y"
            ]

            # datetime columns
            self.datetime_columns = {
                col["name_lake"]: {"timezone": col["timezone"], "datetime_format": col["datetime_format"]}
                for col in json_parser.get_value("destination", "columns") if col["datetime_format"]
            }

            part_col = json_parser.get_value("destination", "table_partition_strategy")[0]
            self.partitioned_by = "" if part_col.lower() == "no_part" else part_col

            tuple_partition_column_format_list = [
                [col["name_lake"], col["datetime_format"]]
                for col in json_parser.get_value("destination", "columns")
                if col["is_partition_column"] and col["is_partition_column"].lower() == "y"
            ]

            if len(self.partitioned_by.split(",")) > 1 and len(tuple_partition_column_format_list) > 0:
                # partitioning strategy based on multiple attributes
                self.partition_columns = [
                    [col["name_lake"], self._get_data_type(col["datatype"], col["length"], col["scale"])]
                    for col in json_parser.get_value("destination", "columns")
                    if col["is_partition_column"] and col["is_partition_column"].lower() == "y"
                ]
            elif len(tuple_partition_column_format_list) > 0:
                # partitioning based on single attribute or single data derivation level (e.g., day)
                self.partition_column, self.partition_column_format = tuple_partition_column_format_list[0]

            if remove_tconx_provided_file_name:
                os.remove(tconx_file_path)

        # derived table tags and full names
        self.table_landing = self.table_lake + self.config_service.tag_table_suffix_stage
        self.db_table_landing = self.db_landing + "." + self.table_landing
        self.db_table_lake = self.db_lake + "." + self.table_lake

        # out_view name
        if not self.table_lakeout or self.table_lakeout == "":
            self.db_view_lake_out = ""
        else:
            logging.info("db_lake_out: {0}, table_lakeout: {1}".format(self.db_lake_out, self.table_lakeout))
            self.db_view_lake_out = self.db_lake_out + "." + self.table_lakeout

    @staticmethod
    def _get_data_type(data_type, length, scale):
        """
        Concatenate data type with length and scale.

        Examples:
        > number 3 4 => number(3,4)
        > int "" "" => int

        :param data_type: type (varchar, int)
        :param length: length of data type
        :param scale: scale of data type
        :return:
        """
        if (not length or length == "") and (not scale or scale == ""):
            return data_type
        elif (length and length != "") and (not scale or scale == ""):
            return "{}({})".format(data_type, length)
        elif (length and length != "") and (scale and scale != ""):
            return "{}({}, {})".format(data_type, length, scale)
        else:
            raise Exception("Unsupported data type format: ")

    def get_destination_technology(self):
        """
        Return technology code of the table destination system

        :return: source technology code
        """
        return self.destination_technology

    def get_columns_create_statement(self, excluded_column=None, quote_character=""):
        """
        Return columns (comma separated String) with data types adjusted to target system
        Example: a int, b varchar(4)

        :param excluded_column: column that should be excluded from self.columns_lake before generation of columns
                                create statement
        :param quote_character: character to quote column name to avoid execution of reserved keywords
        :return: columns (comma separated String) with data types adjusted to target system
        """
        columns = copy.deepcopy(self.columns_lake)

        if excluded_column:
            columns.remove(excluded_column)

        return functools.reduce(lambda x, y: x + ",\n" + y,
                                map(lambda x: quote_character + x[0] + quote_character + " " + x[1], columns))

    def get_columns_rename_statement(self):
        """
        Return columns with old and new name (comma separated String) required for the out_view creation.
        Example: "z_article group_article, zspmcol1 primary_color"

        :return: columns (comma separated String)
        """
        return functools.reduce(lambda x, y: x + ", " + y,
                                map(lambda x: "{} {}".format(x[0][0], x[1]),
                                    filter(lambda x: x[1] is not None and x[1] != "",
                                           zip(self.columns_lake, self.columns_lakeout))))

    def get_lake_column_names(self):
        return list(map(lambda x: x[0], self.columns_lake))

    def get_projection_columns(self, src_column_names, destination_column_names):
        columns = list(filter(lambda x: x[1], zip(src_column_names, destination_column_names)))
        if self.partitioned_by in Util.defined_partitions:
            target_partitions = list(map(lambda x: (x, x), Util.get_target_partitions_list(self.partitioned_by)))
            return columns + target_partitions
        else:
            return columns
