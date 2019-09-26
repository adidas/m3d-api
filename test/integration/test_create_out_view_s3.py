import logging

import py
import pytest
from mock import patch

from m3d import M3D
from m3d.exceptions.m3d_exceptions import M3DDatabaseException
from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.hadoop.emr.s3_table import S3Table
from test.core.s3_table_test_base import S3TableTestBase
from test.test_util.concurrent_executor import ConcurrentExecutor


class TestCreateOutViewS3(S3TableTestBase):

    @pytest.mark.emr
    def test_empty_table_lakeout(self):
        tconx_src_path = \
            "test/resources/test_create_out_view_hive/test_empty_table_lakeout/config/empty_tabl_cd_lakeout.json"

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, tconx_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        # Use test case specific tconx
        py.path.local(tconx_file).write(py.path.local(tconx_src_path).read())

        table_config = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        with pytest.raises(M3DDatabaseException) as exc_info:
            M3D.create_lake_out_view(*table_config, **table_config_kwargs)

        assert "lake_out view name does not exist" == str(exc_info.value)

    @pytest.mark.emr
    def test_empty_columns_lakeout(self):
        tconx_src_path = \
            "test/resources/test_create_out_view_hive/test_empty_columns_lakeout/config/empty_cols_cd_lakeout.json"

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, tconx_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        # Use test case specific tconx
        py.path.local(tconx_file).write(py.path.local(tconx_src_path).read())

        table_config = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        # Value of TABLE_LAKEOUT column in tconx file
        table_lakeout = "bi_retail_test"
        # DB for lake_out
        db_lake_out = scon_emr_dict["environments"][destination_environment]["schemas"]["lake_out"]
        db_view_lake_out = db_lake_out + "." + table_lakeout

        with pytest.raises(M3DDatabaseException) as exc_info:
            M3D.create_lake_out_view(*table_config, **table_config_kwargs)

        err_msg = "View {} cannot be created. The view would have no columns.".format(db_view_lake_out)
        assert err_msg == str(exc_info.value)

    @pytest.mark.emr
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_lakeout_view_hql(self, add_tags_patch):
        tconx_src_path = "test/resources/test_create_out_view_hive/test_lakeout_view_structure/config/tconx.json"

        cluster_mode = False
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, tconx_file, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        # Use test case specific tconx
        py.path.local(tconx_file).write(py.path.local(tconx_src_path).read())

        table_config = [
            m3d_config_file,
            cluster_mode,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        emr_steps_completer = self.create_emr_steps_completer(expected_steps_count=1, timeout_seconds=3)

        with ConcurrentExecutor(emr_steps_completer, delay_sec=0.4):
            logging.info("Calling M3D.create_lake_out_view().")
            M3D.create_lake_out_view(*table_config, **table_config_kwargs)

        emr_system = EMRSystem(*table_config[:5])
        s3_table = S3Table(emr_system, destination_table)

        mock_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        assert 1 == len(mock_cluster.steps)

        hive_step = mock_cluster.steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])

        column_name_pairs = [
            ("record_date", "v_record_date"),
            ("p_string", "v_string"),
            ("p_int", "v_int"),
            ("p_bigint", "v_bigint"),
            ("p_float", "v_float"),
            ("p_varchar_1", "v_varchar_10"),
            ("p_varchar_2", "v_varchar_100"),
            ("p_char_1", "v_char"),
            ("p_boolean", "v_boolean"),
            ("year", "year"),
            ("month", "month")
        ]
        columns_str = ", ".join(map(lambda x: "{} AS {}".format(x[0], x[1]), column_name_pairs))

        drop_view = "DROP VIEW IF EXISTS {};".format(s3_table.db_view_lake_out)

        # S3Table is partitioned by year and month
        create_view = "\n".join([
            "CREATE VIEW {}".format(s3_table.db_view_lake_out),
            "AS",
            "SELECT {}".format(columns_str),
            "FROM {};".format(s3_table.db_table_lake)
        ])

        expected_hql = "\n".join([drop_view, create_view])

        assert actual_hql_content_in_bucket == expected_hql

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "create_lake_out_view"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetView",
            "Value": "dev_lake_out.bi_test101"
        }]
