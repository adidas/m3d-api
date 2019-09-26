import json
import logging
import os
import traceback

from m3d.util.util import Util


class SparkExecutor(object):

    def __init__(self, execution_system):
        self._execution_system = execution_system

    def run(self):
        """
        Run Spark application.

        :raises Exception: in case the selected algorithm is not supported
        """

        self._run_steps()

    def _upload_parameter_json(self, output_dictionary, local_file_path, cluster_file_path):
        """
        Upload spark application parameter dictionary to S3 as JSON file

        :param output_dictionary: parameter dictionary for load
        """

        parameters_json_content = json.dumps(output_dictionary, sort_keys=True, default=lambda o: o.__dict__)
        logging.info("Uploading parameter JSON.\n Content:\n {}".format(parameters_json_content))

        # store dictionary in JSON file
        with open(local_file_path, "w") as f_out:
            f_out.write(parameters_json_content)

        # upload JSON file to S3
        self._execution_system.s3_util.upload_object(
            local_file_path,
            cluster_file_path
        )

        # delete local tmp file
        logging.info("Deleting temporary local parameters file \"{}\"".format(local_file_path))
        os.remove(local_file_path)

    def _spark_submit(self, application_class):
        """
        Execute spark-submit command for algorithm to be executed
        """

        # get spark submit String
        spark_str = self._get_spark_submit_str()

        logging.info("Executing Spark: {}".format(spark_str))
        # Add step to EMR cluster.
        step_name = "EMR Step: Running Spark Application Class {}".format(application_class)

        # execute Spark submit
        self._execution_system.run_command_in_cluster(spark_str, step_name)

    def _remove_parameter_json(self, parameter_file_path):
        """
        Removes the spark application parameter file from S3
        """

        self._execution_system.s3_util.delete_object(parameter_file_path)

    def _report_error(self, name):
        error_subject = "Error for " + name
        exec_tb = traceback.format_exc()
        message = "Error in executing {}. \n Stacktrace: \n {}".format(name, exec_tb)

        logging.error(error_subject)
        Util.send_email(self._execution_system.config_service.emails, error_subject, message)

    def _report_success(self, name):
        success_subject = "Success for " + name
        logging.info(success_subject)
        Util.send_email(self._execution_system.config_service.emails, success_subject, success_subject)

    def _get_spark_submit_str(self):
        raise NotImplementedError("There is currently no implementation for {}._get_spark_submit_str()."
                                  .format(self.__class__.__name__))

    def _run_steps(self):
        raise NotImplementedError("There is currently no implementation for {}._run_steps()."
                                  .format(self.__class__.__name__))
