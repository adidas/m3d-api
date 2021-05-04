import datetime
import gzip
import json
import logging
import time

import moto

from m3d.config.config_service import ConfigService
from m3d.util import util
from m3d.util.aws_credentials import AWSCredentials
from m3d.util.s3_util import S3Util
from test.core.unit_test_base import UnitTestBase
from test.test_util.boto3_util import Boto3Util


class EMRSystemUnitTestBase(UnitTestBase):
    """
    Base class for EMRSystem tests . It prepares the configuration by providing a helper function which constructs test
    case specific config.json, scon_emr configuration.
    """

    default_m3d_config = "config/m3d/config.json"
    default_scon_emr = "config/system/scon-bdp-emr_test.json"

    default_server_host = "localhost"
    default_server_port = 52334
    default_server_url = "http://{}:{}".format(default_server_host, default_server_port)

    default_aws_region = "eu-west-1"
    default_log_bucket = "log-bucket"
    default_dev_landing_bucket = "m3d-dev-landing"
    default_dev_lake_bucket = "m3d-dev-lake"
    default_dev_mart_cal_bucket = "m3d-dev-mart_cal"
    default_dev_metadata_bucket = "m3d-dev-metadata"
    default_dev_inbound_bucket = "m3d-dev-inbound"
    default_dev_application_bucket = "m3d-dev-application"

    @staticmethod
    def dump_aws_credentials(aws_credentials, aws_credentials_file_path):
        """
        Dump AWS credentials to a file.

        :param aws_credentials: object of type AWSCredentials holding AWS access key id and AWS secret access key.
        :param aws_credentials_file_path: path of file where credentials should be saved.
        """
        with open(aws_credentials_file_path, 'w') as outfile:
            json.dump(
                {
                    AWSCredentials.Keys.ACCESS_KEY_ID: aws_credentials.access_key_id,
                    AWSCredentials.Keys.SECRET_ACCESS_KEY: aws_credentials.secret_access_key
                },
                outfile,
                indent=4
            )

    def env_setup(self, tmpdir, destination_system, destination_database, destination_environment):
        """
        This functions creates test specific config.json, scon_emr configuration file in provided tmpdir.
        Directory structure will resemble that of config directory found in root folder of this repository.

            - config.json will be changed to point to a test specific configuration files.
            - scon_emr will be changed to point to a test case specific root directory in HDFS and connect to HTTP
              server on localhost.

        :param tmpdir: test case specific temporary directory where configuration files will be created.
        :param destination_system: destination system code
        :param destination_database: destination database code
        :param destination_environment: destination environment code

        :return: Function will return several parameters:

                     m3d_config_path: paths of test-specific config.json. Should be passed to M3D API calls.
                     scon_emr_path: paths of test-specific scon_emr
                     m3d_config_dict: contents of test-specific config.json as dict
                     scon_emr_dict: contents of test-specific scon_emr as dict
        """
        m3d_config_dict = util.Util.load_dict(self.default_m3d_config)
        tag_config = m3d_config_dict["tags"]["config"]
        tag_system = m3d_config_dict["tags"]["system"]

        tag_credentials = "credentials"

        config_dir = tmpdir.mkdir(tag_config)
        config_system_dir = config_dir.mkdir(tag_system)
        config_credentials_dir = config_dir.mkdir(tag_credentials)

        m3d_config_dict["tags"]["config"] = str(config_dir)
        m3d_config_dict["dir_exec"] = str(self.local_run_dir.mkdir("tmp"))
        m3d_config_file = config_dir.mkdir("m3d").join("config.json")
        m3d_config_file.write(json.dumps(m3d_config_dict, indent=4))

        aws_api_credentials_file = config_credentials_dir.join(
            "credentials-{}-{}-api.json".format(destination_system, destination_database)
        )

        aws_s3_put_credentials_file = config_credentials_dir.join(
            "credentials-{}-{}-s3_put.json".format(destination_system, destination_database)
        )

        aws_s3_del_credentials_file = config_credentials_dir.join(
            "credentials-{}-{}-s3_del.json".format(destination_system, destination_database)
        )

        self.dump_aws_credentials(
            AWSCredentials("test-aws-access-key-api", "test-aws-secret-key-api"),
            str(aws_api_credentials_file)
        )

        self.dump_aws_credentials(
            AWSCredentials("test-aws-access-key-s3_put", "test-aws-secret-key-s3_put"),
            str(aws_s3_put_credentials_file)
        )

        self.dump_aws_credentials(
            AWSCredentials("test-aws-access-key-s3_del", "test-aws-secret-key-s3_del"),
            str(aws_s3_del_credentials_file)
        )

        scon_emr_filename = \
            ConfigService.Prefixes.SCON + "-" + destination_system + "-" + destination_database + ".json"
        scon_emr_file = config_system_dir.join(scon_emr_filename)
        scon_emr_dict = util.Util.load_dict(self.default_scon_emr)

        scon_emr_dict["name_service"] = "localhost:9000"
        scon_emr_dict["credentials"] = "<placeholder_for_AWS_credentials_file>"
        scon_emr_dict["aws_region"] = self.default_aws_region

        scon_emr_dict["aws_api_credentials"] = str(aws_api_credentials_file)
        scon_emr_dict["aws_s3_put_credentials"] = str(aws_s3_put_credentials_file)
        scon_emr_dict["aws_s3_del_credentials"] = str(aws_s3_del_credentials_file)

        scon_emr_dict["api_gateway"] = self.default_server_url
        scon_emr_dict["api_action_timeout_seconds"] = 10
        scon_emr_dict["api_action_polling_interval_seconds"] = 0.2
        scon_emr_dict["api_long_timeout_seconds"] = 20

        scon_emr_dict["emr"]["default_emr_version"] = "emr-6.2.0"
        scon_emr_dict["emr"]["default_ebs_size"] = "128"

        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["landing"] = \
            self.default_dev_landing_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["lake"] = \
            self.default_dev_lake_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["mart_cal"] = \
            self.default_dev_mart_cal_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["metadata"] = \
            self.default_dev_metadata_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["inbound"] = \
            self.default_dev_inbound_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["application"] = \
            self.default_dev_application_bucket
        scon_emr_dict["environments"][destination_environment]["s3_buckets"]["log"] = self.default_log_bucket

        scon_emr_file.write(json.dumps(scon_emr_dict, indent=4))

        logging.debug("test case configuration is saved in \"{}\" directory".format(str(config_dir)))

        return str(m3d_config_file), str(scon_emr_file), m3d_config_dict, scon_emr_dict

    def get_object_content_from_s3(self, file_path_s3, s3_resource=None):
        # Use self.s3_resource if s3_resource is not specified.
        if s3_resource is None:
            s3_resource = self.s3_resource

        bucket, key = S3Util.get_bucket_and_key(file_path_s3)
        return s3_resource.Object(bucket, key).get()["Body"].read().decode("utf-8")

    def get_child_objects(self, dir_path_s3, s3_resource=None):
        # Use self.s3_resource if s3_resource is not specified.
        if s3_resource is None:
            s3_resource = self.s3_resource

        bucket_name, key = S3Util.get_bucket_and_key(dir_path_s3)
        child_objects = s3_resource.Bucket(bucket_name).objects.filter(Prefix=key)
        child_files = ["s3://" + bucket_name + "/" + obj.key for obj in child_objects]
        return child_files

    def dump_data_to_s3(self, object_key, data, s3_resource=None):
        # Use self.s3_resource if s3_resource is not specified.
        if s3_resource is None:
            s3_resource = self.s3_resource

        bucket_name, object_path = S3Util.get_bucket_and_key(object_key)
        s3_resource.Bucket(bucket_name).put_object(Key=object_path, Body=data)

    @staticmethod
    def trim(content):
        return content.strip().replace("\r", "")

    def setup_method(self, method):
        super(EMRSystemUnitTestBase, self).setup_method(method)

        # Setup EMR mock
        self.mock_emr = moto.mock_emr()
        self.mock_emr.start()

        self.emr_cluster_name = "test clustester for unit and integration tests"

        run_job_flow_args = dict(
            Instances={
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType': 'c3.medium',
                'Placement': {'AvailabilityZone': 'test_zone'},
                'SlaveInstanceType': 'c3.xlarge',
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            LogUri='s3://{}/log/'.format(self.default_log_bucket),
            Name=self.emr_cluster_name,
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True
        )

        emr_client = Boto3Util.create_emr_client(self.default_aws_region)
        self.emr_cluster_id = emr_client.run_job_flow(**run_job_flow_args)['JobFlowId']
        logging.debug("Test case specific EMR cluster id is {}".format(self.emr_cluster_id))

        # Setup S3 mock
        self.mock_s3 = moto.mock_s3()
        self.mock_s3.start()

        self.s3_resource = Boto3Util.create_s3_resource()
        self.s3_resource.create_bucket(Bucket=self.default_dev_landing_bucket)
        self.s3_resource.create_bucket(Bucket=self.default_dev_lake_bucket)
        self.s3_resource.create_bucket(Bucket=self.default_dev_mart_cal_bucket)
        self.s3_resource.create_bucket(Bucket=self.default_dev_application_bucket)
        self.s3_resource.create_bucket(Bucket=self.default_log_bucket)

    def teardown_method(self):
        if self.mock_emr is not None:
            self.mock_emr.stop()

        if self.mock_s3 is not None:
            self.mock_s3.stop()

    def create_emr_steps_completer(self, expected_steps_count, timeout_seconds):
        """
        Create and return callable function which will move EMR steps to completed state.
        :param expected_steps_count: number of expected EMR steps
        :param timeout_seconds: timeout in seconds
        :return: callable function
        """
        mock_cluster = self.mock_emr.backends[self.default_aws_region].clusters[self.emr_cluster_id]

        def emr_steps_completer():
            timeout_datetime = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)
            completed_steps = 0

            while datetime.datetime.utcnow() <= timeout_datetime:
                steps_count = len(mock_cluster.steps)

                while completed_steps < steps_count:
                    mock_cluster.steps[completed_steps].state = 'COMPLETED'
                    completed_steps += 1

                if completed_steps >= expected_steps_count:
                    return

                time.sleep(0.2)

            if completed_steps < expected_steps_count:
                err_msg = "Were expecting {} EMR steps to complete. Received: {}".format(
                    expected_steps_count, completed_steps)

                raise Exception(err_msg)

        return emr_steps_completer

    def complete_all_steps(self, fake_cluster, expected_step_count, timeout_seconds, dummy_text):
        end_datetime = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)
        completed_steps = 0

        while datetime.datetime.utcnow() < end_datetime:
            steps_count = len(fake_cluster.steps)
            logging.info("Number of steps so far: {}".format(steps_count))

            while completed_steps < steps_count:
                if completed_steps == 0:
                    self.create_output_file(fake_cluster.steps[completed_steps], dummy_text)

                fake_cluster.steps[completed_steps].state = 'COMPLETED'
                completed_steps += 1

            if completed_steps >= expected_step_count:
                break

            time.sleep(0.2)

    def create_output_file(self, step, dummy_text):
        logging.info("step={{id: {}, state: {}}}".format(step.id, step.state))
        step_id = step.id
        s3_log_file_location = "s3://{}/log/{}/steps/{}/stdout.gz" \
            .format(self.default_log_bucket, self.emr_cluster_id, step_id)

        local_log_file_location = self.local_run_dir.join("stdout.gz")

        logging.info("local_log_file_location={}".format(local_log_file_location))
        logging.info("s3_log_file_location={}".format(str(s3_log_file_location)))

        with gzip.open(str(local_log_file_location), 'wb') as f:
            f.write(dummy_text.encode("utf-8"))

        s3_util = S3Util(AWSCredentials("", ""))
        s3_util.upload_object(str(local_log_file_location), str(s3_log_file_location))
