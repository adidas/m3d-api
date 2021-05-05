import datetime
import functools
import json
import os
import subprocess

from collections.abc import Mapping
from m3d.exceptions.m3d_exceptions import M3DExecutionException, M3DIOException


class Util(object):
    # Global dictionary for the defined partitions that are outside
    defined_partitions = {
        "year": ["year"],
        "month": ["year", "month"],
        "day": ["year", "month", "day"],
        "week": ["year", "week"]
    }

    @staticmethod
    def get_formatted_utc_now(datetime_format):
        utc_now = datetime.datetime.utcnow()
        utc_now_str = utc_now.strftime(datetime_format)
        return utc_now_str

    @staticmethod
    def load_dict(file_path):
        """
        Load JSON from local filesystem.

        :param file_path: path of JSON file
        :return: dict corresponding to the JSON file
        :raises M3DIOException
        """

        try:
            with open(file_path) as data_file:
                return json.load(data_file)
        except Exception as e:
            raise M3DIOException("Unable to load JSON file: {}".format(e))

    @staticmethod
    def merge_nested_dicts(dict1, dict2):
        """
        Merge two dictionaries overwriting values from dict1 with ones contained in dict2.
        Merge will take nesting into consideration.

        :param dict1: dictionary with default values
        :param dict2: dictionary containing overwrite values
        :return: dictionary containing combined parameters
        """
        combined = dict1.copy()

        for key, value in dict2.items():
            if isinstance(value, Mapping):
                combined[key] = Util.merge_nested_dicts(combined.get(key, {}), value)
            else:
                combined[key] = value

        return combined

    @staticmethod
    def execute_shell(command):
        """
        Execute a shell command

        :param command: shell command to be executed
        :return:
        :raises Exception: in case of an error of os.system()
        """

        if os.system(command) != 0:
            raise M3DExecutionException(command)

    @staticmethod
    def execute_subprocess(command, redirect_stderr=True):
        """
        Execute a shell command using subprocess routines.

        :param redirect_stderr: specifies if stderr should be redirected to stdout
        :param command: shell command to be executed
        :return: output of shell command
        :raises M3DExecutionException: in case of non-zero return code
        """

        # Here we are going to redirect stderr to stdout so that we see error messages in case functions raises
        # exception. One other thing to note is that functions reading output of the call will also see data from
        # stderr.
        try:
            if redirect_stderr:
                return subprocess.check_output(command, stderr=subprocess.STDOUT, shell=True)
            else:
                return subprocess.check_output(command, shell=True)
        except subprocess.CalledProcessError as e:
            raise M3DExecutionException(command, e.returncode, e.output)

    @staticmethod
    def send_email(recipient_list, subject, body):
        """
        Send an email with a given subject and body to a list of recipients

        :param recipient_list: list of recipient email addresses
        :param subject: subject of email
        :param body: body of email
        :return:
        """

        email = "echo \"" + body + "\" | mailx -s \"" + subject + "\" "
        email += functools.reduce(lambda x, y: x + " " + y, recipient_list)
        Util.execute_shell(email)

    @staticmethod
    def get_target_partitions_list(partitioned_by):
        """
        Return list of partition columns based on partitioned_by

        :param partitioned_by: type of partition
        :return: list of partition columns
        :raises Exception: if partition type is not supported
        """

        if not partitioned_by:
            return []
        elif len(partitioned_by.split(",")) > 1:
            # multi column partitioning
            return partitioned_by.split(",")
        elif partitioned_by in Util.defined_partitions:
            # date derivation partitioning
            return Util.defined_partitions[partitioned_by]
        else:
            # single column partitioning
            return [partitioned_by]

    @staticmethod
    def get_target_partitions_string(partitioned_by):
        """
        Return String of comma separated partition column names (for insert statement)

        :param partitioned_by: type of partition (year, month, day, or "")
        :return: String of comma separated partition column names
        """

        cols = Util.get_target_partitions_list(partitioned_by)
        return functools.reduce(lambda x, y: x + "," + y, cols) if cols else ""

    @staticmethod
    def get_defined_target_partitions_hive(partitioned_by):
        """
        Return partition columns as comma separated String with Hive data types

        :param partitioned_by: type of partition (year, month, day, or "")
        :return: partition columns as comma separated String with Hive data types
        """

        cols = Util.get_target_partitions_list(partitioned_by)
        return functools.reduce(lambda x, y: x + "," + y, map(lambda x: x + " smallint", cols)) if cols else ""

    @staticmethod
    def get_typed_target_partitions_hive(partitioned_by):
        """
        Return a list of partition columns tuples in form (name, type)

        :param partitioned_by: type of partition (year, month, day, or "")
        :return: list of partition columns tuples in form (name, type)
        """

        cols = Util.get_target_partitions_list(partitioned_by)
        return list(map(lambda x: (x, "smallint"), cols))
