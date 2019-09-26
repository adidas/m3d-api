import json

from test.core.acon_helper import AconHelper
from test.core.emr_system_unit_test_base import EMRSystemUnitTestBase


class EMRAlgorithmTestBase(EMRSystemUnitTestBase):

    def env_setup(
            self,
            tmpdir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            algorithm_instance
    ):
        m3d_config_file, scon_emr_file, m3d_config_dict, scon_emr_dict = super(EMRAlgorithmTestBase, self).env_setup(
            tmpdir,
            destination_system,
            destination_database,
            destination_environment
        )

        AconHelper.setup_acon_from_file(
            m3d_config_dict["tags"]["config"],
            destination_database,
            destination_environment,
            algorithm_instance,
            self.get_default_acon()
        )

        return m3d_config_file

    def get_default_acon(self):
        raise NotImplementedError

    def upload_json_file_to_s3(self, local_uri, s3_uri):
        with open(local_uri) as result_file:
            tmp_dict = json.load(result_file)
            self.dump_data_to_s3(s3_uri, json.dumps(tmp_dict))
