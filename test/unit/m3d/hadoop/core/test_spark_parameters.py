import pytest
from pytest import raises

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException
from m3d.hadoop.core.spark_parameters import SparkParameters
from test.core.unit_test_base import UnitTestBase


class TestSparkParameters(UnitTestBase):
    driver_memory = "5G"
    new_driver_memory = "6G"
    number_executors = "5"
    executor_memory = "2G"
    queue = "test_queue"
    executor_cores = 1
    scheduler_mode = "FAIR"

    def create_spark_params(self):
        spark_params_dict = {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.instances": self.number_executors,
            "spark.executor.memory": self.executor_memory
        }
        spark_params = SparkParameters(spark_params_dict)
        return spark_params

    def create_spark_params_with_queue(self):
        spark_params_dict = {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.instances": self.number_executors,
            "spark.executor.memory": self.executor_memory,
            "spark.yarn.queue": self.queue
        }
        spark_params = SparkParameters(spark_params_dict)
        return spark_params

    def create_spark_params_with_queue_and_optional(self):
        spark_params_dict = {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.instances": self.number_executors,
            "spark.executor.memory": self.executor_memory,
            "spark.yarn.queue": self.queue,
            "spark.executor.cores": self.executor_cores,
            "spark.scheduler.mode": self.scheduler_mode
        }
        spark_params = SparkParameters(spark_params_dict)
        return spark_params

    @pytest.mark.algo
    def test_from_dict(self):
        spark_params = self.create_spark_params()

        assert spark_params.parameters["spark.driver.memory"] == self.driver_memory
        assert spark_params.parameters["spark.executor.instances"] == self.number_executors
        assert spark_params.parameters["spark.executor.memory"] == self.executor_memory

    @pytest.mark.algo
    def test_from_dict_with_queue_and_optional(self):
        spark_params = self.create_spark_params_with_queue_and_optional()

        assert spark_params.parameters["spark.driver.memory"] == self.driver_memory
        assert spark_params.parameters["spark.executor.cores"] == self.executor_cores
        assert spark_params.parameters["spark.executor.instances"] == self.number_executors
        assert spark_params.parameters["spark.scheduler.mode"] == self.scheduler_mode

    @pytest.mark.algo
    def test_equal(self):
        spark_params = self.create_spark_params()
        spark_params_2 = self.create_spark_params()

        assert spark_params == spark_params_2

    @pytest.mark.algo
    def test_not_equal(self):
        spark_params_dict_2 = {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.instances": self.number_executors,
            "spark.executor.memory": self.executor_memory,
            "spark.yarn.queue": "not_test_queue"
        }

        spark_params = self.create_spark_params_with_queue()
        spark_params_2 = SparkParameters(spark_params_dict_2)

        assert not spark_params == spark_params_2

    @pytest.mark.algo
    def test_not_equal_with_different_keys(self):
        spark_params_dict_2 = {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.instances": self.number_executors,
            "spark.executor.memory": self.executor_memory,
            "wrong_key_queue": "not_test_queue"
        }

        spark_params = self.create_spark_params_with_queue()
        spark_params_2 = SparkParameters(spark_params_dict_2)

        assert not spark_params == spark_params_2

    @pytest.mark.algo
    def test_not_equal_without_queue(self):
        spark_params_big = self.create_spark_params_with_queue()
        spark_params_small = self.create_spark_params()

        assert not spark_params_big == spark_params_small

    @pytest.mark.algo
    def test_merge_spark_params_dict(self):

        spark_params_dict_new = {
            "spark.driver.memory": self.new_driver_memory
        }

        spark_params = self.create_spark_params_with_queue()
        spark_params.merge_spark_params_dict(spark_params_dict_new)

        assert not spark_params.parameters["spark.driver.memory"] == self.driver_memory
        assert spark_params.parameters["spark.driver.memory"] == self.new_driver_memory
        assert spark_params.parameters["spark.executor.instances"] == self.number_executors
        assert spark_params.parameters["spark.executor.memory"] == self.executor_memory
        assert spark_params.parameters["spark.yarn.queue"] == self.queue

    @pytest.mark.algo
    def test_merge_spark_params_dict_without_queue(self):
        spark_queue = "queue_spark"
        executor_memory = "25G"
        spark_params = self.create_spark_params()

        spark_params_dict_new = {
            "spark.driver.memory": self.new_driver_memory,
            "spark.executor.memory": executor_memory,
            "spark.yarn.queue": spark_queue
        }

        spark_params.merge_spark_params_dict(spark_params_dict_new)

        assert not spark_params.parameters["spark.driver.memory"] == self.driver_memory
        assert spark_params.parameters["spark.driver.memory"] == self.new_driver_memory
        assert spark_params.parameters["spark.executor.instances"] == self.number_executors
        assert spark_params.parameters["spark.executor.memory"] == executor_memory
        assert spark_params.parameters["spark.yarn.queue"] == spark_queue

    @pytest.mark.algo
    def test_merge_spark_params_str(self):
        spark_params = self.create_spark_params()

        spark_params_str = """
            {
                "spark.driver.memory": "6G",
                "spark.yarn.queue": "test_queue"
            }
        """

        spark_params.merge_spark_params_str(spark_params_str)

        assert not spark_params.parameters["spark.driver.memory"] == self.driver_memory
        assert spark_params.parameters["spark.driver.memory"] == self.new_driver_memory
        assert spark_params.parameters["spark.executor.instances"] == self.number_executors
        assert spark_params.parameters["spark.executor.memory"] == self.executor_memory
        assert spark_params.parameters["spark.yarn.queue"] == self.queue

    @pytest.mark.algo
    def test_validate_ok(self):
        spark_params = self.create_spark_params()
        mandatory_keys = [
            SparkParameters.MandatoryKeys.DriverMemory,
            SparkParameters.MandatoryKeys.NumberOfExecutors,
            SparkParameters.MandatoryKeys.ExecutorMemory
        ]

        assert spark_params.validate(mandatory_keys) is None

    @pytest.mark.algo
    def test_validate_missing_key(self):
        assigned_value_string = "mandatory key of SparkParameters missing"
        spark_params = self.create_spark_params()

        mandatory_keys = [
            SparkParameters.MandatoryKeys.DriverMemory,
            SparkParameters.MandatoryKeys.NumberOfExecutors,
            SparkParameters.MandatoryKeys.ExecutorMemory,
            SparkParameters.MandatoryKeys.Queue
        ]

        with raises(M3DIllegalArgumentException) as exc_info:
            spark_params.validate(mandatory_keys)

        assert assigned_value_string in str(exc_info.value)

    @pytest.mark.algo
    def test_validate_missing_value(self):
        assigned_value_string = "should have an assigned value"
        spark_params = self.create_spark_params_with_queue()
        spark_params.parameters["spark.yarn.queue"] = None

        mandatory_keys = [
            SparkParameters.MandatoryKeys.DriverMemory,
            SparkParameters.MandatoryKeys.NumberOfExecutors,
            SparkParameters.MandatoryKeys.ExecutorMemory,
            SparkParameters.MandatoryKeys.Queue
        ]

        with raises(M3DIllegalArgumentException) as exc_info:
            spark_params.validate(mandatory_keys)

        assert assigned_value_string in str(exc_info.value)
