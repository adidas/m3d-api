import logging
import pytest

from mock import patch
from moto.emr.models import FakeStep

from m3d import M3D

from test.core.s3_table_test_base import S3TableTestBase
from test.core.tconx_helper import TconxHelper


class TestCreateTableS3(S3TableTestBase):

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_hql(self, add_tags_patch, _):
        logging.info("Starting TestCreateTableS3.test_check_hql()")

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, _, _, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        table_config = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        logging.info("Calling  M3D.create_table().")
        M3D.create_table(*table_config, **table_config_kwargs)

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        assert 1 == len(fake_cluster.steps)

        hive_step = fake_cluster.steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        ddl_landing = "CREATE DATABASE IF NOT EXISTS dev_landing;\n" \
                      "CREATE DATABASE IF NOT EXISTS dev_lake;\n" \
                      "CREATE EXTERNAL TABLE dev_landing.bi_test101_stg1(name1 varchar(21), name2 varchar(6), " \
                      "name3 varchar(4))\n" \
                      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'\n" \
                      "LOCATION 's3://m3d-dev-landing/dev/bi/test101/data/'\n" \
                      "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        ddl_lake = "CREATE EXTERNAL TABLE dev_lake.bi_test101(name1 varchar(21), name2 varchar(6), " \
                   "name3 varchar(4))\n" \
                   "PARTITIONED BY (year smallint, month smallint)\n" \
                   "STORED AS PARQUET\n" \
                   "LOCATION 's3://m3d-dev-lake/dev/bi/test101/data/'\n" \
                   "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        # Get content of hql in s3 bucket
        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])
        expected_hql = \
            ddl_landing + "\n" + \
            "MSCK REPAIR TABLE {}.{}_stg1;".format(db_landing, destination_table) + "\n" + \
            ddl_lake + "\n" + \
            "MSCK REPAIR TABLE {}.{};".format(db_lake, destination_table)

        logging.info("Expected: {0}\n".format(expected_hql))
        logging.info("Actual: {0}\n".format(actual_hql_content_in_bucket))

        assert actual_hql_content_in_bucket == expected_hql

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "create_table"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_hql_multi_partitioning(self, add_tags_patch, _):
        logging.info("Starting TestCreateTableS3.test_check_hql_multi_partitioning()")

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test102"

        m3d_config_file, _, _, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        TconxHelper.setup_tconx_from_file(
            m3d_config_dict["tags"]["config"],
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            S3TableTestBase.multi_partition_tconx
        )

        table_config = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        logging.info("Calling  M3D.create_table().")
        M3D.create_table(*table_config, **table_config_kwargs)

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        executed_steps = fake_cluster.steps

        assert len(executed_steps) == 1

        hive_step = executed_steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        ddl_landing = "CREATE DATABASE IF NOT EXISTS dev_landing;\n" \
                      "CREATE DATABASE IF NOT EXISTS dev_lake;\n" \
                      "CREATE EXTERNAL TABLE dev_landing.bi_test102_stg1(name1 varchar(21), name2 varchar(6), " \
                      "name3 varchar(4))\n" \
                      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'\n" \
                      "LOCATION 's3://m3d-dev-landing/dev/bi/test102/data/'\n" \
                      "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        ddl_lake = "CREATE EXTERNAL TABLE dev_lake.bi_test102(name3 varchar(4))\n" \
                   "PARTITIONED BY (name1 varchar(21), name2 varchar(6))\n" \
                   "STORED AS PARQUET\n" \
                   "LOCATION 's3://m3d-dev-lake/dev/bi/test102/data/'\n" \
                   "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        # Get content of hql in s3 bucket
        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])
        expected_hql = \
            ddl_landing + "\n" + \
            "MSCK REPAIR TABLE {}.{}_stg1;".format(db_landing, destination_table) + "\n" + \
            ddl_lake + "\n" + \
            "MSCK REPAIR TABLE {}.{};".format(db_lake, destination_table)

        print("Expected: {0}\n".format(expected_hql))
        print("Actual: {0}\n".format(actual_hql_content_in_bucket))

        assert actual_hql_content_in_bucket == expected_hql

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_hql_single_partitioning(self, add_tags_patch, _):
        logging.info("Starting TestCreateTableS3.test_check_hql_single_partitioning()")

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test103"

        m3d_config_file, _, _, m3d_config_dict, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        TconxHelper.setup_tconx_from_file(
            m3d_config_dict["tags"]["config"],
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            S3TableTestBase.single_partition_tconx
        )

        table_config = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        logging.info("Calling  M3D.create_table().")
        M3D.create_table(*table_config, **table_config_kwargs)

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        executed_steps = fake_cluster.steps

        assert len(executed_steps) == 1

        hive_step = executed_steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        ddl_landing = "CREATE DATABASE IF NOT EXISTS dev_landing;\n" \
                      "CREATE DATABASE IF NOT EXISTS dev_lake;\n" \
                      "CREATE EXTERNAL TABLE dev_landing.bi_test103_stg1(name1 varchar(21), name2 varchar(6), " \
                      "name3 varchar(4))\n" \
                      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'\n" \
                      "LOCATION 's3://m3d-dev-landing/dev/bi/test103/data/'\n" \
                      "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        ddl_lake = "CREATE EXTERNAL TABLE dev_lake.bi_test103(name2 varchar(6), name3 varchar(4))\n" \
                   "PARTITIONED BY (name1 varchar(21))\n" \
                   "STORED AS PARQUET\n" \
                   "LOCATION 's3://m3d-dev-lake/dev/bi/test103/data/'\n" \
                   "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        # Get content of hql in s3 bucket
        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])
        expected_hql = \
            ddl_landing + "\n" + \
            "MSCK REPAIR TABLE {}.{}_stg1;".format(db_landing, destination_table) + "\n" + \
            ddl_lake + "\n" + \
            "MSCK REPAIR TABLE {}.{};".format(db_lake, destination_table)

        print("Expected: {0}\n".format(expected_hql))
        print("Actual: {0}\n".format(actual_hql_content_in_bucket))

        assert actual_hql_content_in_bucket == expected_hql

    @pytest.mark.emr
    @patch("moto.emr.models.ElasticMapReduceBackend.describe_step", return_value=FakeStep("COMPLETED"))
    @patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient._do_add_emr_cluster_tags")
    def test_check_hql_with_custom_location(self, add_tags_patch, _):
        logging.info("Starting TestCreateTableS3.test_check_hql_with_custom_location()")

        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"
        destination_table_location_prefix = "data_20200101100015123"

        m3d_config_file, _, _, _, scon_emr_dict = \
            self.env_setup(
                self.local_run_dir,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        table_config = [
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            destination_table_location_prefix
        ]

        table_config_kwargs = {
            "emr_cluster_id": self.emr_cluster_id
        }

        logging.info("Calling  M3D.create_table().")
        M3D.create_table(*table_config, **table_config_kwargs)

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        assert 1 == len(fake_cluster.steps)

        hive_step = fake_cluster.steps[0]

        assert hive_step.args[0] == "hive"
        assert hive_step.args[1] == "--silent"
        assert hive_step.args[2] == "-f"

        db_landing = scon_emr_dict["environments"][destination_environment]["schemas"]["landing"]
        db_lake = scon_emr_dict["environments"][destination_environment]["schemas"]["lake"]

        ddl_landing = "CREATE DATABASE IF NOT EXISTS dev_landing;\n" \
                      "CREATE DATABASE IF NOT EXISTS dev_lake;\n" \
                      "CREATE EXTERNAL TABLE dev_landing.bi_test101_stg1(name1 varchar(21), name2 varchar(6), " \
                      "name3 varchar(4))\n" \
                      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'\n" \
                      "LOCATION 's3://m3d-dev-landing/dev/bi/test101/data/'\n" \
                      "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");"

        ddl_lake = "CREATE EXTERNAL TABLE dev_lake.bi_test101(name1 varchar(21), name2 varchar(6), " \
                   "name3 varchar(4))\n" \
                   "PARTITIONED BY (year smallint, month smallint)\n" \
                   "STORED AS PARQUET\n" \
                   "LOCATION 's3://m3d-dev-lake/dev/bi/test101/{}/'\n" \
                   "TBLPROPERTIES(\"serialization.encoding\"=\"UTF-8\");".format(destination_table_location_prefix)

        # Get content of hql in s3 bucket
        actual_hql_content_in_bucket = self.get_object_content_from_s3(hive_step.args[3])
        expected_hql = \
            ddl_landing + "\n" + \
            "MSCK REPAIR TABLE {}.{}_stg1;".format(db_landing, destination_table) + "\n" + \
            ddl_lake + "\n" + \
            "MSCK REPAIR TABLE {}.{};".format(db_lake, destination_table)

        logging.info("Expected: {0}\n".format(expected_hql))
        logging.info("Actual: {0}\n".format(actual_hql_content_in_bucket))

        assert actual_hql_content_in_bucket == expected_hql

        add_tags_patch_call_args_list = add_tags_patch.call_args_list
        assert len(add_tags_patch_call_args_list) == 2
        assert add_tags_patch_call_args_list[0][0][0] == [{
            "Key": "ApiMethod",
            "Value": "create_table"
        }]
        assert add_tags_patch_call_args_list[1][0][0] == [{
            "Key": "TargetTable",
            "Value": "dev_lake.bi_test101"
        }]
