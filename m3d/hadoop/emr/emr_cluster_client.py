import collections
import logging
import os
import sys
import traceback

import boto3
import botocore

from m3d.exceptions.m3d_exceptions import M3DIllegalArgumentException, M3DEMRApiException
from m3d.hadoop.emr.emr_exceptions import M3DAWSAPIException, M3DEMRStepException, M3DEMRStepTimeout
from m3d.util.exception_logger import exception_logger
from m3d.util.s3_util import S3Util


class EMRClusterClient(object):

    class EMRStepState(object):
        PENDING = "PENDING"
        CANCEL_PENDING = "CANCEL_PENDING"
        RUNNING = "RUNNING"
        COMPLETED = "COMPLETED"
        CANCELLED = "CANCELLED"
        FAILED = "FAILED"
        INTERRUPTED = "INTERRUPTED"

    class EMRClusterState(object):
        STARTING = "STARTING"
        BOOTSTRAPPING = "BOOTSTRAPPING"
        RUNNING = "RUNNING"
        WAITING = "WAITING"
        TERMINATING = "TERMINATING"
        TERMINATED = "TERMINATED"
        TERMINATED_WITH_ERRORS = "TERMINATED_WITH_ERRORS"

    class RequestActionOnFailure(object):
        TERMINATE_JOB_FLOW = "TERMINATE_JOB_FLOW"
        CANCEL_AND_WAIT = "CANCEL_AND_WAIT"
        CONTINUE = "CONTINUE"

    class EMRCustomJar(object):
        COMMAND_RUNNER = "command-runner.jar"

    class EMRResponseFields(object):
        CLUSTER = "Cluster"
        STATUS = "Status"
        STATE = "State"
        STEP = "Step"
        STEPS = "Steps"
        FAILURE_DETAILS = "FailureDetails"
        REASON = "Reason"
        MESSAGE = "Message"
        LOG_FILE = "LogFile"
        ID = "Id"
        STEP_IDS = "StepIds"
        LOG_URI = "LogUri"

    class AWSConstants(object):
        S3_FILE_AVAILABILITY_TIMEOUT_SECONDS = 300
        EMR_STEP_OUTPUT_FILE_NAME = "{}/steps/{}/stdout.gz"
        EMR_STEP_ERROR_FILE_NAME = "stderr.gz"

    # definition of FailureDetails class
    FailureDetails = collections.namedtuple("FailureDetails", ["reason", "message", "log_file"])

    HEADER_CONTENT_TYPE_JSON = {
        "Content-Type": "application/json"
    }

    def __init__(
            self,
            emr_cluster_id,
            aws_region,
            aws_credentials,
            timeout_seconds,
            polling_interval_seconds,
            long_timeout_seconds,
    ):
        """
        Initialize EMR Cluster Client

        :param emr_cluster_id: id of the EMR Cluster
        :param aws_region: aws region where the cluster is running
        :param aws_credentials: AWSCredentials object holding access_key_id and secret_access_key
        :param timeout_seconds: request timeout in seconds
        :param polling_interval_seconds: polling interval time in seconds
        :param long_timeout_seconds: timeout for spark steps in seconds
        """
        self.emr_cluster_id = emr_cluster_id
        self.aws_region = aws_region

        self.timeout_seconds = self._validate_float("timeout_seconds", timeout_seconds)
        self.polling_interval_seconds = self._validate_float("polling_interval_seconds", polling_interval_seconds)
        self.long_timeout_seconds = self._validate_float("long_timeout_seconds", long_timeout_seconds)

        self.client = boto3.client(
            'emr',
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            region_name=aws_region
        )

        self.s3_util = S3Util(aws_credentials)

    @exception_logger(Exception)
    def _validate_float(self, parameter_name, parameter_value):
        """
        Validate whether the parameter was assigned a valid float value and cast it
        :param parameter_name: name of the parameter
        :param parameter_value: received value for the parameter
        :return: casted float value
        """

        float_value = float(parameter_value)

        if float_value <= 0:
            raise M3DIllegalArgumentException("Negative value passed for {} argument.".format(parameter_name))

        return float_value

    def get_cluster_state(self):
        response = self.client.describe_cluster(ClusterId=self.emr_cluster_id)
        cluster_state = (
            response[EMRClusterClient.EMRResponseFields.CLUSTER]
                    [EMRClusterClient.EMRResponseFields.STATUS]
                    [EMRClusterClient.EMRResponseFields.STATE]
        )
        return cluster_state

    def describe_cluster(self):
        response = self.client.describe_cluster(ClusterId=self.emr_cluster_id)
        return response

    def wait_for_cluster_startup(self):
        max_attempts = 1 + self.timeout_seconds / self.polling_interval_seconds
        waiter = self.client.get_waiter('cluster_running')

        try:
            waiter.wait(
                ClusterId=self.emr_cluster_id,
                WaiterConfig={
                    'Delay': self.polling_interval_seconds,
                    'MaxAttempts': max_attempts
                }
            )

        except botocore.exceptions.WaiterError as e:
            if "Max attempts exceeded" in str(e):
                raise M3DAWSAPIException(
                    "Cluster {} failed to start after {} seconds.".format(self.emr_cluster_id, self.timeout_seconds)
                )
            else:
                raise

        cluster_state = self.get_cluster_state()
        return cluster_state

    @staticmethod
    def _get_add_step_request_body(name, action_on_failure, jar, args):
        """
        Create the request body for EMR step
        :param name: name of the request to submit
        :param action_on_failure: specify action for failure
        :param jar: path of the jar file
        :param args: string of args to provide
        :return: request body as a string
        """
        return {
            "Name": name,
            "ActionOnFailure": action_on_failure,
            "HadoopJarStep": {
                "Jar": jar,
                "Args": args.split()
            }
        }

    def add_step(self, step_name, command_str):
        """
        Add step in the EMR Cluster
        :param step_name: name of the step
        :param command_str: command for the step
        :return: assigned step id
        """
        # create EMR Step request body
        step_body = self._get_add_step_request_body(
            step_name,
            EMRClusterClient.RequestActionOnFailure.CONTINUE,
            EMRClusterClient.EMRCustomJar.COMMAND_RUNNER,
            command_str
        )

        # Submit and execute EMR Step
        response = self.client.add_job_flow_steps(
            JobFlowId=self.emr_cluster_id,
            Steps=[step_body]
        )

        emr_step_id = response[EMRClusterClient.EMRResponseFields.STEP_IDS][0]
        logging.info("Started EMR Step '{}' to execute command '{}'.".format(emr_step_id, command_str))

        return emr_step_id

    def get_step_status(self, emr_step_id):
        """
        Get the status for a specific step in the EMR Cluster
        :param emr_step_id: id of the step
        :return: status of the step together with the failure details, if any
        """
        response = self.client.describe_step(
            ClusterId=self.emr_cluster_id,
            StepId=emr_step_id
        )

        response_step_status = (
            response[EMRClusterClient.EMRResponseFields.STEP]
                    [EMRClusterClient.EMRResponseFields.STATUS]
        )

        emr_step_status = response_step_status[EMRClusterClient.EMRResponseFields.STATE]

        if emr_step_status == EMRClusterClient.EMRStepState.FAILED:
            failure_details_dict = response_step_status[EMRClusterClient.EMRResponseFields.FAILURE_DETAILS]

            # We need to use get() method here since not all fields are always present.
            emr_step_failure_details = EMRClusterClient.FailureDetails(
                failure_details_dict.get(EMRClusterClient.EMRResponseFields.REASON, ""),
                failure_details_dict.get(EMRClusterClient.EMRResponseFields.MESSAGE, ""),
                failure_details_dict.get(EMRClusterClient.EMRResponseFields.LOG_FILE, "")
            )
        else:
            emr_step_failure_details = None

        return emr_step_status, emr_step_failure_details

    def wait_for_step_completion(self, emr_step_id, timeout_seconds=None):
        """
        Continuously poll for the status of the step till it is completed or it times out
        :param emr_step_id: id of the step
        :param timeout_seconds: timeout in seconds
        :return: nothing if the step completes successfully, otherwise raises
        """
        timeout_seconds = timeout_seconds or self.timeout_seconds

        max_attempts = 1 + timeout_seconds / self.polling_interval_seconds
        waiter = self.client.get_waiter('step_complete')

        try:
            waiter.wait(
                ClusterId=self.emr_cluster_id,
                StepId=emr_step_id,
                WaiterConfig={
                    'Delay': self.polling_interval_seconds,
                    'MaxAttempts': max_attempts
                }
            )

        except botocore.exceptions.WaiterError:
            _, e_value, _ = sys.exc_info()
            if "Max attempts exceeded" in str(e_value):
                raise M3DEMRStepTimeout(self.emr_cluster_id, emr_step_id)
            else:
                logging.error("WaiterError in step execution: {}".format(e_value))
                logging.error(traceback.format_exc())

        emr_step_status, emr_step_failure_details = self.get_step_status(emr_step_id)

        if emr_step_status == EMRClusterClient.EMRStepState.COMPLETED:
            logging.info("EMR Step \"{}\" finished successfully.".format(emr_step_id))
            return

        msg = "EMR Step with cluster_id='{}' and step_id='{}' failed to complete: " \
              "status: '{}', failure details: '{}'" \
            .format(self.emr_cluster_id, emr_step_id, emr_step_status, emr_step_failure_details)

        logging.error(msg)

        if emr_step_status == EMRClusterClient.EMRStepState.FAILED:
            err_path = self.get_step_err_path(emr_step_id)
            if err_path:
                self.s3_util.wait_for_file_availability(
                    err_path,
                    self.polling_interval_seconds,
                    EMRClusterClient.AWSConstants.S3_FILE_AVAILABILITY_TIMEOUT_SECONDS
                )

                stderr_content = self.s3_util.read_gzip_file_content(err_path)

                logging.error("EMR Step failed with error output: {}".format(stderr_content))
                msg += ", stderr: '{}'".format(stderr_content)

        raise M3DEMRStepException(self.emr_cluster_id, emr_step_id, msg)

    def wait_for_spark_step_completion(self, emr_step_id):
        """
        Wait for completion of a spark step. The timeout in this case is going to be the
        long_timeout_seconds.
        :param emr_step_id:  id of step
        :return: nothing if the step completes successfully, otherwise raises
        """
        self.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

    def get_list_of_steps(self):
        """
        Get list of steps in the EMR Cluster
        :return: list of steps in the order they were accepted by EMR Cluster
        """
        response = self.client.list_steps(ClusterId=self.emr_cluster_id)

        step_ids = list(map(
            lambda step_def: step_def[EMRClusterClient.EMRResponseFields.ID],
            response[EMRClusterClient.EMRResponseFields.STEPS]
        ))

        # step ids will be in reverse order. fixing that bellow
        return step_ids[::-1]

    def get_step_output_path(self, emr_step_id):
        """
        Get location in s3 of stdout file for a particular step
        :param emr_step_id: emr step id
        :return: s3 location of the file
        """
        response = self.client.describe_cluster(
            ClusterId=self.emr_cluster_id
        )

        cluster_log_location = \
            response[EMRClusterClient.EMRResponseFields.CLUSTER][EMRClusterClient.EMRResponseFields.LOG_URI]

        out_file_path = cluster_log_location + \
            EMRClusterClient.AWSConstants.EMR_STEP_OUTPUT_FILE_NAME.format(
                self.emr_cluster_id,
                emr_step_id
            )

        # out_file_path can have 's3n://' protocol instead of 's3://'. So we are changing it.
        out_file = S3Util.normalize_s3_path(out_file_path)

        return out_file

    def get_step_err_path(self, emr_step_id):
        """
        Get location in s3 of error log file for a particular failed step
        :param emr_step_id: emr step id
        :return: s3 location of the file
        """
        _, emr_step_failure_details = self.get_step_status(emr_step_id)

        if emr_step_failure_details is None or emr_step_failure_details.log_file is None:
            return None

        error_file_path = emr_step_failure_details.log_file

        if not error_file_path.endswith(EMRClusterClient.AWSConstants.EMR_STEP_ERROR_FILE_NAME):
            error_file_path = os.path.join(
                error_file_path,
                EMRClusterClient.AWSConstants.EMR_STEP_ERROR_FILE_NAME
            )

        # error_file_path can have 's3n://' protocol instead of 's3://'. So we are changing it.
        error_file = S3Util.normalize_s3_path(error_file_path)

        return error_file

    def add_cluster_tag(self, key, value):
        """
        Add a single tag to EMR cluster
        :param key: tag key
        :param value: tag value
        """

        tag = self._create_tag(key, value)
        if tag is not None:
            self._do_add_emr_cluster_tags([tag])

    def add_cluster_tags(self, tags_dict):
        """
        Add multiple tags to a cluster
        :param tags_dict: dict with tags key/value pairs
        """

        tags = list(filter(lambda t: t is not None, map(lambda p: self._create_tag(p[0], p[1]), tags_dict.items())))
        self._do_add_emr_cluster_tags(tags)

    def _do_add_emr_cluster_tags(self, tags):
        try:
            self.client.add_tags(ResourceId=self.emr_cluster_id, Tags=tags)
        except botocore.exceptions.ClientError as e:
            raise M3DEMRApiException("Unable to add cluster tags: " + str(e))

    @staticmethod
    def _create_tag(key, value):
        if key is not None and value is not None:
            logging.info("Adding a tag with key=\"{}\", value=\"{}\" to cluster.".format(key, value))
            return {"Key": key, "Value": value}
        else:
            logging.info("Unable to add a tag with key=\"{}\", value=\"{}\" to cluster.".format(key, value))
            return None
