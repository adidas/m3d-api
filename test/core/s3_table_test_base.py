import os

from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase
from test.core.tconx_helper import TconxHelper


class S3TableTestBase(EMRSystemUnitTestBase):

    default_tconx = \
        "test/resources/s3_table_test_base/tconx-bdp-emr_test-dev-bi_test101.json"

    multi_partition_tconx = \
        "test/resources/s3_table_test_base/tconx-bdp-emr_test-dev-bi_test102.json"

    single_partition_tconx = \
        "test/resources/s3_table_test_base/tconx-bdp-emr_test-dev-bi_test103.json"

    def env_setup(
            self,
            tmpdir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
    ):
        """
        This function builds on top of EMRSystemUnitTestBase.env_setup() and adds test-specific tconx file.

        :param tmpdir: test case specific temporary directory where configuration files will be created.
        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code
        :param destination_table: destination table code

        :return: Function will return several parameters:

                     m3d_config_path: paths of test-specific config.json. Should be passed to M3D API calls.
                     scon_emr_path: paths of test-specific scon_emr
                     tconx_path: paths of test-specific tconx
                     m3d_config_dict: contents of test-specific config.json as dict
                     scon_emr_dict: contents of test-specific scon_emr as dict
        """
        m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict = \
            super(S3TableTestBase, self).env_setup(
                tmpdir,
                destination_system,
                destination_database,
                destination_environment
            )

        # tconx specific part
        tconx_file = TconxHelper.setup_tconx_from_file(
            m3d_config_dict["tags"]["config"],
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            S3TableTestBase.default_tconx
        )

        return m3d_config_file, scon_emr_file, tconx_file, \
            m3d_config_dict, scon_emr_dict

    @staticmethod
    def assert_one_hql_sent(dump_dir, expected_hql):
        generated_files = map(lambda f: os.path.join(dump_dir, f), os.listdir(dump_dir))
        hql_files = list(filter(lambda f: os.path.isfile(f) and f.endswith(".hql"), generated_files))
        assert len(hql_files) == 1

        hql_file = hql_files[0]

        with open(hql_file, 'r') as hql_f:
            generated_hql = hql_f.read()

        generated_hql_processed = generated_hql.strip().lower()
        expected_hql_processed = expected_hql.strip().lower()

        assert generated_hql_processed == expected_hql_processed
