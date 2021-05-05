import pytest
from mock import patch, call

from m3d.util.util import Util


class TestUtil(object):

    @pytest.mark.bdp
    @patch('os.system', return_value=0)
    def test_send_mail(self, os_system_patch):
        Util.send_email(["firstname.lastname@m3d.com"], "hello", "hello")
        os_system_patch.assert_has_calls([
            call('echo "hello" | mailx -s "hello" firstname.lastname@m3d.com')
        ])

    @pytest.mark.bdp
    def test_get_target_partitions_list(self):
        """
        This method tests the correct functionality of get_partition_column_list of Util class
        :return:
        """
        assert Util.get_target_partitions_list("year") == ["year"]
        assert Util.get_target_partitions_list("month") == ["year", "month"]
        assert Util.get_target_partitions_list("day") == ["year", "month", "day"]
        assert Util.get_target_partitions_list("") == []

        assert Util.get_target_partitions_list("country") == ["country"]

    @pytest.mark.bdp
    def test_get_target_partitions_string(self):
        """
        This method tests the correct functionality of get_partition_column_string of Util class
        :return:
        """
        assert Util.get_target_partitions_string("year") == "year"
        assert Util.get_target_partitions_string("month") == "year,month"
        assert Util.get_target_partitions_string("day") == "year,month,day"
        assert Util.get_target_partitions_string("") == ""

        assert Util.get_target_partitions_string("country") == "country"

    @pytest.mark.bdp
    def test_get_defined_partition_columns_hive(self):
        """
        This method tests the correct functionality of get_partition_column_string of Util class
        :return:
        """
        assert Util.get_defined_target_partitions_hive("year") == "year smallint"
        assert Util.get_defined_target_partitions_hive("month") == "year smallint,month smallint"
        assert Util.get_defined_target_partitions_hive("day") == "year smallint,month smallint,day smallint"
        assert Util.get_defined_target_partitions_hive("") == ""
