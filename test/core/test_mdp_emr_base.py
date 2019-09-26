import py

from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase
from test.core.oracle_helper import OracleHelper


class TestMDPEMRBase(EMRSystemUnitTestBase):

    def env_setup(
            self,
            tmpdir,
            destination_system,
            destination_database,
            destination_environment,
            metadatapackage_system,
            mdp_database
    ):
        m3d_config_file, scon_bdp_file, m3d_config_dict, scon_bdp_dict = \
            super(TestMDPEMRBase, self).env_setup(
                tmpdir,
                destination_system,
                destination_database,
                destination_environment
            )

        config_dir = py.path.local(m3d_config_dict["tags"]["config"])

        OracleHelper.setup_oracle_scon(
            config_dir,
            metadatapackage_system,
            mdp_database,
            OracleHelper.default_scon_mdp_test
        )

        return m3d_config_file, scon_bdp_file, \
            m3d_config_dict, scon_bdp_dict
