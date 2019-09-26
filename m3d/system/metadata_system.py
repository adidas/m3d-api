import json

from m3d.system import abstract_system


class MetadataSystem(abstract_system.AbstractSystem):

    def __init__(self, config, cluster_mode, source_system, database, schema):
        """
        Initialize Metadata Config

        :param config: config file
        :param cluster_mode: flag for cluster mode
        :param source_system: metadata system code
        :param database: metadata database code
        :param schema: metadata schema code
        """

        # call super constructor
        abstract_system.AbstractSystem.__init__(self, config, cluster_mode, source_system, database)

        # store parameters
        self.schema = schema

        with open(self.config_service.get_scon_path(cluster_mode, source_system, database)) as data_file:
            self.params_system = json.load(data_file)

        self.view_oracle_scn = self.params_system["view_oracle_scn"]
        self.view_oracle_mdp = self.params_system["view_oracle_mdp"]

    @staticmethod
    def execute_query(credentials, query):
        import cx_Oracle
        conn = cx_Oracle.connect(credentials)
        curs = conn.cursor()
        curs.execute(query)
        rows = curs.fetchall()
        description = curs.description
        return rows, description

    @staticmethod
    def get_conn_string_with_credentials(params_system, schema):
        # get credentials
        credentials_file_oracle = params_system["credentials"]

        with open(credentials_file_oracle) as f_credentials:
            credentials_oracle = json.load(f_credentials)

        # connection String for schema
        return credentials_oracle["oracle_conn_string"][schema]
