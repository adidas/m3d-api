import pytest
from mock import patch, mock_open

from m3d.config.config_service import ConfigService
from test.core.unit_test_base import UnitTestBase


class TestConfigService(UnitTestBase):

    @pytest.mark.bdp
    def test_parse_config_file(self):
        test_config_json = \
            """
            {
              "emails": [
                "this.is@adidas-group.com",
                "not.a@adidas-group.com",
                "love.song@adidas.com"
              ],
              "dir_exec": "/tmp/",
              "python": {
                "main": "m3d_main.pyc",
                "base_package": "m3d"
              },
              "subdir_projects": {
                "m3d_engine": "m3d-engine/target/scala-2.10/",
                "m3d_api": "m3d-api/"
              },
              "tags": {
                "table_suffix_stage": "_stg1",
                "table_suffix_swap": "_swap",

                "full_load": "full_load",
                "delta_load": "delta_load",
                "append_load": "append_load",
                "oozie": "oozie",
                "decom_gzip": "gzip_decompressor",

                "false": "false",

                "config": "config",
                "system": "system",
                "algorithm": "algorithm",
                "table": "table",
                "view": "view",
                "upload": "upload",
                "pushdown": "pushdown",

                "aws": "aws",
                "hdfs": "hdfs",
                "file": "file"
              },
              "data_dict_delimiter": "|"
            }
            """

        with patch("builtins.open", new_callable=mock_open, read_data=test_config_json):
            config_service = ConfigService("_")

        expected_params = {
            "emails": [
                "this.is@adidas-group.com",
                "not.a@adidas-group.com",
                "love.song@adidas.com"
            ],

            "python_main": "m3d_main.pyc",
            "python_base_package": "m3d",

            "dir_exec": "/tmp/",

            "subdir_projects_m3d_engine": "m3d-engine/target/scala-2.10/",
            "subdir_projects_m3d_api": "m3d-api/",

            "data_dict_delimiter": "|",

            "tag_table_suffix_stage": "_stg1",
            "tag_table_suffix_swap": "_swap",

            "tag_full_load": "full_load",
            "tag_delta_load": "delta_load",
            "tag_append_load": "append_load",

            "tag_system": "system",
            "tag_table": "table",
            "tag_algorithm": "algorithm",
            "tag_config": "config",
            "tag_pushdown": "pushdown",
            "tag_upload": "upload",

            "tag_aws": "aws",
            "tag_file": "file"
        }

        for param in expected_params:
            assert getattr(config_service, param) == expected_params[param]
