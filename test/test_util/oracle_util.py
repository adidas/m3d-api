import logging


# Mock of the Oracle cursor class, the purpose is to capture the executed queries and called procedures,
# and also to provide mock data in the form of the description field and the fetchall() method
class OracleCursorMock(object):
    description = ''
    fetchall_data = []
    fetchone_data = []

    queries = []
    query_to_description_map = {}
    query_to_fetchall_data_map = {}

    called_procs = []

    @staticmethod
    def execute(query):
        OracleCursorMock.queries.append(query)
        if query not in OracleCursorMock.query_to_fetchall_data_map:
            logging.warning("Unexpected Oracle Cursor query: %s" % query)
            OracleCursorMock.fetchall_data = []
        else:
            OracleCursorMock.fetchall_data = OracleCursorMock.query_to_fetchall_data_map[query]

        if query not in OracleCursorMock.query_to_description_map:
            logging.warning("Unexpected Oracle Cursor query: %s" % query)
            OracleCursorMock.description = ""
        else:
            OracleCursorMock.description = OracleCursorMock.query_to_description_map[query]

    @staticmethod
    def callproc(proc_name, proc_arg):
        OracleCursorMock.called_procs.append(" ".join([proc_name] + proc_arg))

    @staticmethod
    def fetchall():
        return OracleCursorMock.fetchall_data

    @staticmethod
    def fetchone():
        return OracleCursorMock.fetchone_data

    @staticmethod
    def expect_query(query, description, fetchall_data):
        OracleCursorMock.query_to_description_map[query] = description
        OracleCursorMock.query_to_fetchall_data_map[query] = fetchall_data

    @staticmethod
    def reset():
        OracleCursorMock.description = ''
        OracleCursorMock.fetchall_data = []
        OracleCursorMock.fetchone_data = []

        OracleCursorMock.queries = []
        OracleCursorMock.query_to_description_map = {}
        OracleCursorMock.query_to_fetchall_data_map = {}

        OracleCursorMock.called_procs = []


# Mock of the Oracle Connection, the purpose is to provide an OracleCursorMock as cursor
class OracleConnectionMock:
    def __init__(self, _):
        self.cursor_instance = OracleCursorMock()

    def cursor(self):
        return self.cursor_instance


# Mock the readable lob object
class BLOB (object):
    ddl = ""

    def __init__(self, ddl):
        self.ddl = ddl.upper()

    def read(self):
        return self.ddl


class OracleTestUtil:

    def __init__(self, schema_name_lake, schema_name_lake_out, schema_name_mart_mod,
                 schema_name_mart_cal, schema_name_mart_out):
        self.schema_name_mart_out = schema_name_mart_out
        self.schema_name_mart_cal = schema_name_mart_cal
        self.schema_name_mart_mod = schema_name_mart_mod
        self.schema_name_lake_out = schema_name_lake_out
        self.schema_name_lake = schema_name_lake

    def assert_grant_permissions_to_higher_layers(self, query_list, permission_object):
        assert query_list[0] == 'grant select on %s to %s with grant option' % (
            permission_object, self.schema_name_lake
        )
        assert query_list[1] == 'grant select on %s to %s with grant option' % (
            permission_object, self.schema_name_lake_out
        )
        assert query_list[2] == 'grant select on %s to %s with grant option' % (
            permission_object, self.schema_name_mart_mod
        )
        assert query_list[3] == 'grant select on %s to %s with grant option' % (
            permission_object, self.schema_name_mart_cal
        )
        assert query_list[4] == 'grant select on %s to %s with grant option' % (
            permission_object, self.schema_name_mart_out
        )

    def assert_call_compile_schema_procedures(self, called_procs):
        assert len(called_procs) == 4
        assert called_procs[0] == 'DBMS_UTILITY.compile_schema ' + self.schema_name_lake_out.upper()
        assert called_procs[1] == 'DBMS_UTILITY.compile_schema ' + self.schema_name_mart_mod.upper()
        assert called_procs[2] == 'DBMS_UTILITY.compile_schema ' + self.schema_name_mart_cal.upper()
        assert called_procs[3] == 'DBMS_UTILITY.compile_schema ' + self.schema_name_mart_out.upper()
