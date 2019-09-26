import py


class HQLHelper(object):

    class Tags(object):
        CONFIG = "config"
        VIEW = "view"
        PUSHDOWN = "pushdown"

    @staticmethod
    def get_hql_file_path(
            config_subdir,
            destination_system,
            destination_database,
            destination_environment,
            view
    ):
        hql_file_name = HQLHelper.get_hql_file_name(
            destination_system,
            destination_database,
            destination_environment,
            view
        )

        hql_file = py.path.local(config_subdir) \
            .join(HQLHelper.Tags.VIEW) \
            .join(HQLHelper.Tags.PUSHDOWN) \
            .join(destination_system) \
            .join(destination_database) \
            .join(destination_environment) \
            .join(hql_file_name)

        return str(hql_file)

    @staticmethod
    def get_hql_file_name(destination_system, destination_database, destination_environment, view):
        return "{}-{}-{}-{}.hql".format(destination_system, destination_database, destination_environment, view)
