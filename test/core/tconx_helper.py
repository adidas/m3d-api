import os
import py
import json


class TconxHelper(object):

    class Tags(object):
        CONFIG = "config"
        TABLE = "table"
        PUSHDOWN = "pushdown"

    default_tconx_emr_bi_test101 = \
        "test/resources/s3_table_test_base/tconx-bdp-emr_test-dev-bi_test101.json"

    @staticmethod
    def setup_tconx_from_file(
            config_dir_path,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            base_tconx_path
    ):
        tconx_file_path = TconxHelper.get_tconx_file_path(
            config_dir_path,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )

        if not os.path.isdir(os.path.dirname(tconx_file_path)):
            os.makedirs(os.path.dirname(tconx_file_path))

        base_tconx_content = py.path.local(base_tconx_path).read()
        py.path.local(tconx_file_path).write(base_tconx_content)

        return tconx_file_path

    @staticmethod
    def setup_tconx_from_content(
            config_dir_path,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            tconx_content
    ):
        tconx_file_path = \
            TconxHelper.get_tconx_file_path(
                config_dir_path,
                destination_system,
                destination_database,
                destination_environment,
                destination_table
            )

        if not os.path.isdir(os.path.dirname(tconx_file_path)):
            os.makedirs(os.path.dirname(tconx_file_path))

        with open(tconx_file_path, 'w') as out_file:
            json.dump(tconx_content, out_file)

        return tconx_file_path

    @staticmethod
    def get_tconx_file_name(
            destination_system,
            destination_database,
            destination_environment,
            destination_table
    ):
        tconx_file_name = "tconx-{}-{}-{}-{}.json".format(
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        return tconx_file_name

    @staticmethod
    def get_tconx_file_path(
            config_dir_path,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
    ):
        tconx_file_name = TconxHelper.get_tconx_file_name(
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )

        tconx_file = py.path.local(config_dir_path) \
            .join(TconxHelper.Tags.TABLE) \
            .join(TconxHelper.Tags.PUSHDOWN) \
            .join(destination_system) \
            .join(destination_database) \
            .join(destination_environment) \
            .join(tconx_file_name)

        return str(tconx_file)

    @staticmethod
    def get_tconx_dir(
            config_dir_path,
            destination_system,
            destination_database,
            destination_environment
    ):
        tconx_dir = py.path.local(config_dir_path) \
            .join(TconxHelper.Tags.TABLE) \
            .join(TconxHelper.Tags.PUSHDOWN) \
            .join(destination_system) \
            .join(destination_database) \
            .join(destination_environment)

        return str(tconx_dir)

    @staticmethod
    def get_tconx_dir_hdfs(
            m3d_config_dict,
            scon_bdp_dict,
            destination_system,
            destination_database,
            destination_environment
    ):
        txonc_dir_hdfs = os.path.join(
            scon_bdp_dict["hdfs_dir_oozie"],
            m3d_config_dict["subdir_projects"]["m3d_api"],
            TconxHelper.Tags.CONFIG,
            TconxHelper.Tags.TABLE,
            TconxHelper.Tags.PUSHDOWN,
            destination_system,
            destination_database,
            destination_environment
        )

        return txonc_dir_hdfs
