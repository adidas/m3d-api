from m3d.hadoop.algorithm.algorithm_hadoop import AlgorithmHadoop


class AlgorithmScalaRunner(AlgorithmHadoop):

    SCALA_CLASS_PARAM = "scala_class"

    def __init__(self, execution_system, algorithm_instance, algorithm_params):
        """
        Initialize AlgorithmScalaRunner which will run scala class specified
        in algorithm configuration file and pass parameters specified there
        as well (merging them with parameters passed from command line).

        :param execution_system: emr system
        :param algorithm_instance: name of the algorithm instance
        :param algorithm_params: algorithm configuration
        """

        super(AlgorithmScalaRunner, self).__init__(execution_system, algorithm_instance, algorithm_params)
        self._scala_class = self._parameters[AlgorithmScalaRunner.SCALA_CLASS_PARAM]

    def get_scala_class(self):
        return self._scala_class

    def build_params(self):
        return self._parameters
