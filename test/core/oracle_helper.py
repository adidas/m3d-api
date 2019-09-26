import os
import json
import py

from m3d.util.util import Util


class OracleHelper(object):

    default_scon_m3d_test = "config/system/scon-m3d-m3d_test.json"
    default_scon_mdp_test = "config/system/scon-mdp-mdp_test.json"

    @staticmethod
    def setup_oracle_scon(
            config_dir,
            source_system,
            db_cd,
            base_scon_path,
            database_type=None
    ):
        # Making sure that we can accept both strings as well as py.path.local objects.
        config_dir = py.path.local(str(config_dir))

        config_system_dir = config_dir.join("system")
        config_credentials_dir = config_dir.join("credentials")

        if not config_system_dir.check():
            config_system_dir.mkdir()

        if not config_credentials_dir.check():
            config_credentials_dir.mkdir()

        oracle_docker_ip = os.getenv("ORACLE_DOCKER_IP", "")
        credentials_data = {
            "oracle_conn_string": {
                "lake": "LAKE/test_lake_password@%s:1521/XE" % oracle_docker_ip,
                "lake_out": "LAKE_OUT/test_lake_out_password@%s:1521/XE" % oracle_docker_ip,
                "m3d": "M3D/test_m3d_password@%s:1521/XE" % oracle_docker_ip,
                "mart_mod": "MART_MOD/test_mart_mod_password@%s:1521/XE" % oracle_docker_ip,
                "mart_cal": "MART_CAL/test_mart_cal_password@%s:1521/XE" % oracle_docker_ip,
                "mart_out": "MART_OUT/test_mart_out_password@%s:1521/XE" % oracle_docker_ip,
                "test_lake": "TEST_LAKE/test_lake_password@%s:1521/XE" % oracle_docker_ip,
                "test_lake_out": "TEST_LAKE_OUT/test_lake_out_password@%s:1521/XE" % oracle_docker_ip,
                "test_mart_mod": "TEST_MART_MOD/test_mart_mod_password@%s:1521/XE" % oracle_docker_ip,
                "test_mart_cal": "TEST_MART_CAL/test_mart_cal_password@%s:1521/XE" % oracle_docker_ip,
                "test_mart_out": "TEST_MART_OUT/test_mart_out_password@%s:1521/XE" % oracle_docker_ip,
                "dev_mart_mod": "DEV_MART_MOD/dev_mart_mod_password@%s:1521/XE" % oracle_docker_ip,
                "dev_mart_cal": "DEV_MART_CAL/dev_mart_cal_password@%s:1521/XE" % oracle_docker_ip,
                "dve_mart_out": "DEV_MART_OUT/dev_mart_out_password@%s:1521/XE" % oracle_docker_ip
            }
        }

        credentials_filename = "credentials-{}-{}.json".format(source_system, db_cd)
        credentials_file = config_credentials_dir.join(credentials_filename)
        credentials_file.write(json.dumps(credentials_data, indent=4))

        scon_dict = Util.load_dict(base_scon_path)
        scon_dict["credentials"] = str(credentials_file)

        if database_type:
            scon_dict["database_type"] = database_type

        scon_filename = "scon-{}-{}.json".format(source_system, db_cd)
        scon_file = config_system_dir.join(scon_filename)
        scon_file.write(json.dumps(scon_dict, indent=4))

        return str(scon_file), scon_dict
