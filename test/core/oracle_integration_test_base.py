import json
import logging
import os
import shutil

import py
from mock import patch

from m3d.util import util
from test.core.oracle_helper import OracleHelper
from test.core.tconx_helper import TconxHelper
from test.test_util.oracle_util import OracleConnectionMock, OracleCursorMock


class OracleIntegrationTestBase(object):
    """
    Base class for tests preparing the configuration by providing a helper function which constructs test
    case specific config.json, scon_bdp and scon_mdp configuration files.
    """

    local_global_test_home = os.path.abspath("test/runs/")

    local_suite_home = None
    local_case_home = None
    local_run_dir = None  # py.path.local object pointing to local_case_home

    default_m3d_config = "config/m3d/config.json"
    default_scon_m3d_test = "config/system/scon-m3d-m3d_test.json"
    default_scon_mdp_test = "config/system/scon-mdp-mdp_test.json"

    patches = [
        patch('cx_Oracle.connect', new=OracleConnectionMock, create=True)
    ]

    @classmethod
    def setup_class(cls):
        for p in OracleIntegrationTestBase.patches:
            p.start()

        cls.local_suite_home = os.path.join(cls.local_global_test_home, cls.__name__)

        logging.info("Performing cleanup of local directory: \"%s\"" % cls.local_suite_home)
        if not os.path.exists(cls.local_global_test_home):
            os.mkdir(cls.local_global_test_home)

        if os.path.exists(cls.local_suite_home):
            shutil.rmtree(cls.local_suite_home)

        os.mkdir(cls.local_suite_home)

    def setup_method(self, method):
        logging.info("Performing setup_method() for \"%s\" method." % method.__name__)
        self.local_case_home = os.path.join(self.local_suite_home, method.__name__)

        os.mkdir(self.local_case_home)

        self.local_run_dir = py.path.local(self.local_case_home)

        OracleCursorMock.reset()

    @classmethod
    def teardown_class(cls):
        logging.info("Performing teardown_class() for  \"%s\" class." % cls.__name__)
        for p in OracleIntegrationTestBase.patches:
            p.stop()

    def env_setup(
            self,
            tmpdir,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            scon_database_type=None,
            tconx_content=None
    ):
        m3d_config_dict = util.Util.load_dict(OracleIntegrationTestBase.default_m3d_config)

        config_dir = tmpdir.mkdir(m3d_config_dict["tags"]["config"])
        config_m3d_dir = config_dir.mkdir("m3d")

        m3d_config_dict["tags"]["config"] = str(config_dir)
        m3d_config_dict["dir_exec"] = self.local_case_home
        m3d_config_file = config_m3d_dir.join("config.json")
        m3d_config_file.write(json.dumps(m3d_config_dict, indent=4))

        scon_file, scon_dict = OracleHelper.setup_oracle_scon(
            config_dir,
            destination_system,
            destination_database,
            OracleHelper.default_scon_m3d_test,
            database_type=scon_database_type
        )

        logging.info("Test case configuration is saved in \"%s\" directory" % str(config_dir))

        # tconx specific part
        if tconx_content is None:
            tconx_file = TconxHelper.setup_tconx_from_file(
                m3d_config_dict["tags"]["config"],
                destination_system,
                destination_database,
                destination_environment,
                destination_table,
                TconxHelper.default_tconx_emr_bi_test101
            )
        else:
            tconx_file = TconxHelper.setup_tconx_from_content(
                m3d_config_dict["tags"]["config"],
                destination_system,
                destination_database,
                destination_environment,
                destination_table,
                tconx_content
            )

        return str(m3d_config_file), scon_file, tconx_file, m3d_config_dict, scon_dict
