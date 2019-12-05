import pytest
from mock import patch

from m3d.hadoop.emr.emr_system import EMRSystem
from m3d.util.aws_credentials import AWSCredentials

from test.core.s3_table_test_base import S3TableTestBase


class MockConfigService(object):
    subdir_projects_m3d_api = 'test_subdir_projects_m3d_api'
    subdir_projects_m3d_engine = 'test_subdir_projects_m3d_engine'

    scon_path = None

    @staticmethod
    def get_scon_path(source_system, database):
        assert source_system == 'test_source_system'
        assert database == 'test_database'
        return MockConfigService.scon_path


class TestEMRSystem(S3TableTestBase):
    """
    Suite of unit tests for EMRSystem.
    """

    test_scon_json_template = '''
    {{
      "database_type": "emr",
      "storage_type": "s3",
      "environments": {{
        "prod_environment": {{
          "schemas": {{
            "landing": "landing",
            "lake": "lake",
            "lake_out": "lake_out",
            "mart_mod": "mart_mod",
            "mart_cal": "mart_cal",
            "mart_out": "mart_out",
            "m3d": "m3d",
            "work": "work",
            "error": "error"
          }},
          "s3_buckets": {{
            "landing": "adidas-da-bdp-prod-landing",
            "lake": "adidas-da-bdp-prod-lake",
            "mart_cal": "adidas-da-bdp-prod-mart-cal",
            "application": "adidas-da-landing-application",
            "log": "io.3stripes.factory.prod.ireland.infrastructure-logs"
          }},
          "s3_deployment_dir_base": "/m3d-prod/"
        }},
        "test_environment": {{
          "schemas": {{
            "landing": "test_landing",
            "lake": "test_lake",
            "lake_out": "test_lake_out",
            "mart_mod": "test_mart_mod",
            "mart_cal": "test_mart_cal",
            "mart_out": "test_mart_out",
            "m3d": "m3d",
            "work": "work",
            "error": "error"
          }},
          "s3_buckets": {{
            "landing": "adidas-da-bdp-test-landing",
            "lake": "adidas-da-bdp-test-lake",
            "mart_cal": "adidas-da-bdp-test-mart-cal",
            "metadata": "das-lab3-management-test",
            "application": "adidas-da-landing-application",
            "log": "io.3stripes.factory.test.ireland.infrastructure-logs"
          }},
          "s3_deployment_dir_base": "/m3d-test/"
        }},
        "dev_environment": {{
          "schemas": {{
            "landing": "dev_landing",
            "lake": "dev_lake",
            "lake_out": "dev_lake_out",
            "mart_mod": "dev_mart_mod",
            "mart_cal": "dev_mart_cal",
            "mart_out": "dev_mart_out",
            "m3d": "m3d",
            "work": "work",
            "error": "error"
          }},
          "s3_buckets": {{
            "landing": "adidas-da-bdp-dev-landing",
            "lake": "adidas-da-bdp-dev-lake",
            "mart_cal": "adidas-da-bdp-dev-mart-cal",
            "metadata": "das-lab3-management-dev",
            "application": "adidas-da-landing-application",
            "log": "io.3stripes.factory.dev.ireland.infrastructure-logs"
          }},
          "s3_deployment_dir_base": "/m3d-dev/"
        }}
      }},

      "aws_api_credentials": "{aws_api_credentials}",
      "aws_s3_put_credentials": "{aws_s3_put_credentials}",
      "aws_s3_del_credentials": "{aws_s3_del_credentials}",

      "api_action_polling_interval_seconds": 3,
      "api_action_timeout_seconds": 120,
      "api_long_timeout_seconds": 300,
      "aws_region": "eu-west-1",
      "packages_to_deploy": [
        "hadoop"
      ],
      "configs_to_deploy": [
            "test_config_1",
            "test_config_2"
      ],

      "s3_dir_base": "/dir_base",

      "subdir": {{
        "data": "test_data/",
        "error": "test_error/",
        "log": "test_log/",
        "work": "test_work/",
        "archive": "test_archive/",
        "header": "test_header/",
        "config": "test_config/",
        "apps": "test_apps/",
        "m3d_engine": "test_m3d_engine/",
        "loading": "test_loading/",
        "full_load": "test_full_load/",
        "delta_load": "test_delta_load/",
        "append_load": "test_append_load/",
        "black_whole": "test_black_whole/",
        "credentials": "test_credentials/",
        "keytab": "test_keytab/",
        "tmp": "test_tmp/",
        "m3d": "m3d",
        "metadata" : "metadata"
      }},
      "spark": {{
        "jar_name": "test_jar.jar",
        "main_class": "com.adidas.analytics.AlgorithmFactory"
      }},
      "emr": {{
        "default_emr_version": "emr-5.17.0",
        "default_ebs_size": "128"
      }}
    }}
    '''

    test_emr_system_arguments = [
        'test_m3d_config_file',
        'test_source_system',
        'test_database',
        'test_environment'
    ]

    @pytest.mark.emr
    @patch('m3d.system.abstract_system.AbstractSystem.__init__')
    @patch('m3d.system.metadata_system.MetadataSystem')
    @patch.object(EMRSystem, 'config_service', new=MockConfigService, spec=None, create=True)
    def test_parses_basic_attributes_from_system_config_file(self, _, __):
        """
        Test case checks that all relevant key-values are extracted from sconx file and assigned to correct member
        variables of EMRSystem object.
        """
        aws_api_credentials = AWSCredentials("fake_aws_api_access_key", "fake_aws_api_secret_key")
        aws_api_credentials_file = self.local_run_dir.join("aws-credentials-emr-api.json")
        self.dump_aws_credentials(aws_api_credentials, str(aws_api_credentials_file))

        aws_s3_put_credentials = AWSCredentials("fake_aws_s3_put_access_key", "fake_aws_s3_put_secret_key")
        aws_s3_put_credentials_file = self.local_run_dir.join("aws-credentials-emr-s3_put.json")
        self.dump_aws_credentials(aws_s3_put_credentials, str(aws_s3_put_credentials_file))

        aws_s3_del_credentials = AWSCredentials("fake_aws_s3_del_access_key", "fake_aws_s3_del_secret_key")
        aws_s3_del_credentials_file = self.local_run_dir.join("aws-credentials-emr-s3_del.json")
        self.dump_aws_credentials(aws_s3_del_credentials, str(aws_s3_del_credentials_file))

        test_scon_json = TestEMRSystem.test_scon_json_template.format(
            aws_api_credentials=str(aws_api_credentials_file),
            aws_s3_put_credentials=str(aws_s3_put_credentials_file),
            aws_s3_del_credentials=str(aws_s3_del_credentials_file)
        )

        s3_scon_file = self.local_run_dir.join("scon-emr-emr-test.json")
        s3_scon_file.write(test_scon_json)

        MockConfigService.scon_path = str(s3_scon_file)

        emr_system = EMRSystem(*self.test_emr_system_arguments)

        expected_system_params = {
            "bucket_landing": "adidas-da-bdp-test-landing",
            "bucket_lake": "adidas-da-bdp-test-lake",
            "bucket_mart_cal": "adidas-da-bdp-test-mart-cal",
            "bucket_log": "io.3stripes.factory.test.ireland.infrastructure-logs",

            "default_ebs_size": "128",
            "default_emr_version": "emr-5.17.0",

            "aws_api_credentials": aws_api_credentials,
            "aws_s3_put_credentials": aws_s3_put_credentials,
            "aws_s3_del_credentials": aws_s3_del_credentials,

            "api_action_timeout_seconds": 120,
            "api_action_polling_interval_seconds": 3,
            "api_long_timeout_seconds": 300,
            "aws_region": "eu-west-1",
            "packages_to_deploy": ["hadoop"],
            "configs_to_deploy": ["test_config_1", "test_config_2"],

            "subdir_archive": "test_archive/",
            "subdir_header": "test_header/",
            "subdir_config": "test_config/",
            "subdir_data": "test_data/",
            "subdir_data_backup": "data_backup/",
            "subdir_error": "test_error/",
            "subdir_work": "test_work/",
            "subdir_log": "test_log/",
            "subdir_apps": "test_apps/",
            "subdir_m3d_engine": "test_m3d_engine/",
            "subdir_loading": "test_loading/",
            "subdir_full_load": "test_full_load/",
            "subdir_delta_load": "test_delta_load/",
            "subdir_append_load": "test_append_load/",
            "subdir_black_whole": "test_black_whole/",
            "subdir_credentials": "test_credentials/",
            "subdir_keytab": "test_keytab/",
            "subdir_tmp": "test_tmp/",

            "subdir_code": "m3d",
            "subdir_metadata": "metadata",

            "spark_jar_name": "test_jar.jar",

            "dir_apps": "s3://adidas-da-landing-application/m3d-test/test_environment/test_apps/",
            "dir_apps_algorithm": "s3://adidas-da-landing-application/m3d-test/"
                                  "test_environment/test_apps/test_m3d_engine/",
            "dir_apps_loading": "s3://adidas-da-landing-application/m3d-test/test_environment/"
                                "test_apps/test_loading/",

            "dir_tmp_s3": "s3://adidas-da-landing-application/m3d-test/test_environment/test_tmp/",
            "dir_tmp_local": "/test_tmp/",
            "spark_jar_path":
                "s3://adidas-da-landing-application/m3d-test/test_environment/m3d/" +
                "test_subdir_projects_m3d_api/test_jar.jar",

            "dir_m3d_api_deployment":
                "s3://adidas-da-landing-application/m3d-test/test_environment/m3d/test_subdir_projects_m3d_api",
            "dir_metadata_deployment":
                "s3://adidas-da-landing-application/m3d-test/test_environment/metadata/test_subdir_projects_m3d_api"
        }

        for param in expected_system_params.keys():
            assert getattr(emr_system, param) == expected_system_params[param]

    @pytest.mark.emr
    def test_add_emr_cluster_tags(self):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        m3d_config_file, _, _, _, _ = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )

        emr_system = EMRSystem(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            self.emr_cluster_id
        )

        emr_system.add_cluster_tag("DataFormat", "csv")

        assert fake_cluster.tags == {"DataFormat": "csv"}

    @pytest.mark.emr
    def test_add_emr_cluster_tags_multiple_calls(self):
        destination_system = "bdp"
        destination_database = "emr_test"
        destination_environment = "dev"
        destination_table = "bi_test101"

        m3d_config_file, _, _, _, _ = self.env_setup(
            self.local_run_dir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        fake_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]
        emr_system = EMRSystem(
            m3d_config_file,
            destination_system,
            destination_database,
            destination_environment,
            self.emr_cluster_id
        )

        tags1 = {
            "DataFormat": "csv"
        }
        emr_system.add_cluster_tag("DataFormat", tags1["DataFormat"])

        tags2 = {
            "Database": "test_lake",
            "Table": destination_table
        }
        emr_system.add_cluster_tags(tags2)

        all_tags = tags1.copy()
        all_tags.update(tags2)
        assert fake_cluster.tags == all_tags
