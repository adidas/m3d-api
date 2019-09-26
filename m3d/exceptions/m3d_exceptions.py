class M3DException(Exception):
    """
    Base class for all custom exceptions.
    """

    def __init__(self, message=None):
        if message is None:
            message = "An M3D application level exception occurred."
        super(M3DException, self).__init__(message)


class M3DIOException(M3DException):
    """
    This class is the general class of exceptions produced by failed or interrupted I/O operations.
    Raised when an I/O operation fails for an I/O-related reason, e.g., file not found or disk full.
    """

    def __init__(self, message=None):
        if message is None:
            message = "An IO exception occurred."
        super(M3DIOException, self).__init__(message)


class M3DIllegalArgumentException(M3DException):
    """
    Thrown to indicate that a method has been passed an illegal or inappropriate argument.
    For example, invalid char exception.
    """

    def __init__(self, message):
        super(M3DIllegalArgumentException, self).__init__(message)


class M3DUnsupportedDataTypeException(M3DIOException):
    """
    A general exception thrown when the requested operation does not support the requested data type.
    """

    def __init__(self, message=None):
        if message is None:
            message = "An unsupported data type is requested for the operation."
        self.message = message
        super(M3DUnsupportedDataTypeException, self).__init__(message)


class M3DUnsupportedAlgorithmException(M3DUnsupportedDataTypeException):
    """
    This exception is thrown when the requested service does not support the algorithm.
    For example, some supported algorithms from algorithm factory are PartitionMaterialization,
    GzipDecompressor, etc.
    """

    def __init__(self, algorithm, message=None):
        if message is None:
            message = "The algorithm %s is not supported for the requested action." % algorithm
        self.algorithm = algorithm
        self.message = message
        super(M3DUnsupportedAlgorithmException, self).__init__(message)


class M3DUnsupportedLoadTypeException(M3DUnsupportedDataTypeException):
    """
    This exception is thrown when the requested service does not support the load type.
    The supported loads full load, append load and delta load.
    """

    def __init__(self, load_type, message=None):
        if message is None:
            message = "The load type %s is not supported for the requested action." % load_type
        self.load_type = load_type
        self.message = message
        super(M3DUnsupportedLoadTypeException, self).__init__(message)


class M3DUnsupportedSystemException(M3DUnsupportedDataTypeException):
    """
    This exception is thrown when the requested service does not support the system/technology.
    For example, some supported systems/technologies are Hive, Oracle, Exasol, etc.
    """

    def __init__(self, system, message=None):
        if message is None:
            message = "The system %s is not supported for the requested action." % system
        self.system = system
        self.message = message
        super(M3DUnsupportedSystemException, self).__init__(message)


class M3DUnsupportedStorageException(M3DUnsupportedDataTypeException):
    """
    This exception is thrown when the requested service does not support the system/technology.
    For example, it can be raised while calling the hdfs_table.HDFSTable().drop_tables()
    over a non HDFS storage.
    """

    def __init__(self, storage, message=None):
        if message is None:
            message = "Unsupported storage type: %s" % storage
        self.storage = storage
        self.message = message
        super(M3DUnsupportedStorageException, self).__init__(message)


class M3DUnsupportedDestinationSystemException(M3DUnsupportedSystemException):
    """
    This exception is thrown when the requested service does not support the destination system/technology.
    """

    def __init__(self, system, message=None):
        if message is None:
            message = "The destination system %s is not supported for the requested action" % system
        self.system = system
        self.message = message
        super(M3DUnsupportedDestinationSystemException, self).__init__(system, message)


class M3DUnsupportedDatabaseTypeException(M3DUnsupportedSystemException):
    """
    This exception is thrown when the requested service does not support the database type.
    """

    def __init__(self, system, message=None):
        if message is None:
            message = "The database type %s is not supported for the requested action" % system
        self.system = system
        self.message = message
        super(M3DUnsupportedDatabaseTypeException, self).__init__(system, message)


class M3DEmptyTconxException(M3DIOException):

    def __init__(self, message=None):
        if message is None:
            message = "The tconx file is empty."
        self.message = message
        super(M3DEmptyTconxException, self).__init__(message)


class M3DDatabaseException(M3DException):
    """
    A class indicating generic database exception.
    """

    def __init__(self, message):
        super(M3DDatabaseException, self).__init__(message)


class M3DExecutionException(M3DException):
    """
    An exception to be thrown if cli execution fails (e.g.: execute_subprocess)
    """

    def __init__(self, command, code, output):
        """
        Constructs M3DExecutionException

        :param command: failed command
        :param code: status code of the command
        :param output: output of the command
        """

        super(M3DExecutionException, self).__init__("Command \"{cmd}\" returned status code {code}:\n{output}".format(
            cmd=command,
            output=output,
            code=code
        ))
        self.output = output
        self.command = command
        self.code = code


class M3DEMRException(M3DException):
    """
    This class is the general class of exceptions produced by failed or interrupted operations with EMR/S3.
    """

    def __init__(self, message=None):
        if message is None:
            message = "An EMR exception occurred."
        super(M3DEMRException, self).__init__(message)


class M3DEMRApiException(M3DEMRException):
    """
    Signals an error occurred during the submission a call of the API Gateway.
    """

    def __init__(self, message=None):
        if message is None:
            message = "Error integration with EMR API"
        self.message = message
        super(M3DEMRApiException, self).__init__(message)


class M3DReconciliationDeviationException(M3DIOException):
    """
    A general exception to be thrown if at least one reconciliation task for a table
    returned a deviation from the expected result
    The programmer is responsible to throw an appropriate message.
    """

    def __init__(self, message=None):
        if message is None:
            message = "Reconciliation failed."
        self.message = message
        super(M3DReconciliationDeviationException, self).__init__(message)
