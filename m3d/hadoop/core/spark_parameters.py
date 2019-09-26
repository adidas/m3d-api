import json
import copy

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException


# TODO: possibly the cleanup is required
class SparkParameters(object):

    EXTERNAL_PARAMS_KEY = "spark"

    class MandatoryKeys(object):
        DriverMemory = "spark.driver.memory"
        NumberOfExecutors = "spark.executor.instances"
        ExecutorMemory = "spark.executor.memory"
        Queue = "spark.yarn.queue"

    def __init__(self, parameters):
        self.parameters = parameters

    def __eq__(self, other):
        """ Equality comparison operator """
        if isinstance(other, SparkParameters):
            if (len(self.parameters)) != len(other.parameters):
                return False

            for key in self.parameters:
                if key not in other.parameters or self.parameters[key] != other.parameters[key]:
                    return False

            return True

        return False

    def __ne__(self, other):
        """ Inequality comparison operator """
        return not self == other

    def copy(self):
        return copy.deepcopy(self)

    def merge_spark_params_str(self, spark_params):
        # skip if None or empty string
        if not spark_params:
            return

        spark_params_dict = json.loads(spark_params)
        self.merge_spark_params_dict(spark_params_dict)

    def merge_spark_params_dict(self, spark_params_dict):
        # merge values from spark_params_dict to self
        for key in spark_params_dict:
            if self.parameters is None:
                self.parameters = {}
            self.parameters[key] = spark_params_dict[key]

    def validate(self, mandatory_keys):
        for key in mandatory_keys:
            if key not in self.parameters.keys():
                raise M3DIllegalArgumentException("'{}' mandatory key of SparkParameters missing".format(key))
            if not self.parameters[key]:
                raise M3DIllegalArgumentException("'{}' property of SparkParameters should have an assigned value".
                                                  format(key))
