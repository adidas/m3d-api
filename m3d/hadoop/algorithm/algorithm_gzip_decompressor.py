from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop
from m3d.hadoop.algorithm.scala_classes import ScalaClasses
from m3d.hadoop.emr.s3_table import S3Table


class AlgorithmGzipDecompressor(AlgorithmHadoop):

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize Algorithm GzipDecompressor

        :param execution_system: an instance of EMRSystem object
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmGzipDecompressor, self).__init__(execution_system, algorithm_instance, algorithm_params)

        destination_table_name = algorithm_params["destination_table"]
        self._table = S3Table(execution_system, destination_table_name)

        if 'format' in algorithm_params:
            self._format = algorithm_params["format"]
        else:
            self._format = "csv"

        self._thread_pool_size = self._parameters["thread_pool_size"]

    def get_scala_class(self):
        return ScalaClasses.GZIP_DECOMPRESSOR

    def build_params(self):
        return GzipDecompressorParams(self._table.dir_landing_final, self._format, self._thread_pool_size).__dict__


class GzipDecompressorParams(object):
    """
    Class resembling the contents of the algorithms parameter file
    """

    def __init__(self, directory, format, thread_pool_size):
        self.format = format
        self.directory = directory
        self.thread_pool_size = thread_pool_size
