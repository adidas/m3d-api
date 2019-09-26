import pytest
from mock import patch, mock_open
from m3d.system.metadata_system import MetadataSystem
from test.core.unit_test_base import UnitTestBase


class MockConfigService:
    def __init__(self):
        pass

    subdir_projects_m3d_api = 'test_subdir_projects_m3d_api'
    subdir_projects_m3d_engine = 'test_subdir_projects_m3d_engine'

    @staticmethod
    def get_scon_path(_, source_system, database):
        assert source_system == 'test_source_system'
        assert database == 'test_database'
        return 'test_data_file_path'


class TestMetadataSystem(UnitTestBase):
    test_metadatasystem_arguments = ['test_m3d_config_file',
                                     'test_cluster_mode',
                                     'test_source_system',
                                     'test_database',
                                     'test_schema']
    test_scon_json = '''
        {
         "credentials": "test_config/test_credentials/credentials-bdp-bdp_test.json",
         "view_oracle_scn": "SCN_LAKE_OUT_NAMES",
         "view_oracle_mdp": "M3D_API.V_API_M3D_MDP"
        }
    '''
    test_credentials_json = '{"oracle_conn_string": {"test_schema": ""}}'

    @pytest.mark.bdp
    @patch('m3d.system.abstract_system.AbstractSystem.__init__')
    @patch('builtins.open', new_callable=mock_open,
           read_data=test_scon_json)
    @patch.object(MetadataSystem, 'config_service', new=MockConfigService, spec=None, create=True)
    def test_init_calls_the_superclass_constructor(self, __, mock_super_init):
        metadata_system = MetadataSystem(*self.test_metadatasystem_arguments)
        mock_super_init.assert_called_once_with(metadata_system, 'test_m3d_config_file',
                                                'test_cluster_mode',
                                                'test_source_system',
                                                'test_database')
