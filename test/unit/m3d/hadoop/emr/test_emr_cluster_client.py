import gzip
import io
import logging
import time

import moto
import pytest
from mock import patch

from m3d.hadoop.emr.emr_cluster_client import EMRClusterClient
from m3d.hadoop.emr.emr_exceptions import M3DAWSAPIException
from m3d.util.aws_credentials import AWSCredentials
from test.core.unit_test_base import UnitTestBase
from test.test_util.boto3_util import Boto3Util
from test.test_util.concurrent_executor import ConcurrentExecutor


class MockedMethod(object):
    emr_cluster_id = None

    log_file_template = "s3://mybucket/log/{emr_cluster_id}/steps/{emr_step_id}/stderr.gz"

    @staticmethod
    def get_step_status_mocked(emr_step_id):
        return "FAILED", EMRClusterClient.FailureDetails(
                "test_reason",
                "test_message",
                MockedMethod.log_file_template.format(
                    emr_cluster_id=MockedMethod.emr_cluster_id,
                    emr_step_id=emr_step_id
                )
            )


class TestEMRClusterClient(UnitTestBase):
    emr_cluster_name = "test_cluster"
    aws_region = "us-east-1"
    aws_credentials = AWSCredentials("test_access_key", "test_secret_key")
    timeout_seconds = 0.5
    retry_seconds = 0.1
    long_timeout_seconds = 3.0

    @staticmethod
    def env_setup(
            emr_cluster_name,
            aws_region,
            aws_credentials,
            timeout_ms,
            retry_ms,
            long_timeout_ms
    ):
        run_job_flow_args = dict(
            Instances={
                'InstanceCount': 3,
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType': 'c3.medium',
                'Placement': {'AvailabilityZone': 'test_zone'},
                'SlaveInstanceType': 'c3.xlarge',
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            LogUri='s3://mybucket/log/',
            Name=emr_cluster_name,
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True
        )

        emr_client = Boto3Util.create_emr_client(aws_region)
        emr_cluster_id = emr_client.run_job_flow(**run_job_flow_args)['JobFlowId']

        emr_cluster_client = EMRClusterClient(
            emr_cluster_id,
            aws_region,
            aws_credentials,
            timeout_ms,
            retry_ms,
            long_timeout_ms
        )

        return emr_cluster_client, emr_cluster_id

    @staticmethod
    def _compress_string(s):
        out = io.BytesIO()
        with gzip.GzipFile(fileobj=out, mode="w") as gzip_s:
            gzip_s.write(s.encode())

        compressed_str = out.getvalue()
        return compressed_str

    @pytest.mark.emr
    @moto.mock_emr
    def test_get_cluster_state(self):
        emr_cluster_client, _ = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        cluster_state = emr_cluster_client.get_cluster_state()

        assert cluster_state == "WAITING"

    @pytest.mark.emr
    @moto.mock_emr
    def test_wait_for_cluster_startup(self):
        emr_cluster_client, _ = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        cluster_state = emr_cluster_client.wait_for_cluster_startup()

        assert cluster_state == "WAITING"

    @pytest.mark.emr
    @moto.mock_emr
    def test_wait_for_bootstrapping_cluster(self):
        mock_emr_obj = moto.mock_emr()
        with mock_emr_obj:
            emr_cluster_client, emr_cluster_id = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            # Change step state to BOOTSTRAPPING so that wait times out
            emr_backend = mock_emr_obj.backends[self.aws_region]
            fake_cluster = emr_backend.clusters[emr_cluster_id]
            fake_cluster.state = "BOOTSTRAPPING"

            err_msg = "Cluster {} failed to start after {} seconds.".format(emr_cluster_id, self.timeout_seconds)

            with pytest.raises(M3DAWSAPIException, match=err_msg):
                emr_cluster_client.wait_for_cluster_startup()

    @pytest.mark.emr
    @moto.mock_emr
    def test_add_step(self):
        # Expected response is of the format s-XXXXXXXXXXXXX
        emr_cluster_client, _ = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        step_name = "Test_Step"
        command_str = "/usr/bin/spark-submit --class spark.job.main.class"

        emr_step_id = emr_cluster_client.add_step(step_name, command_str)

        assert str(emr_step_id).startswith("s-")
        assert len(emr_step_id) == 15

    @pytest.mark.emr
    @moto.mock_emr
    def test_get_step_status(self):
        emr_cluster_client, _ = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        step_name = "Test_Step"
        command_str = "/usr/bin/spark-submit --class spark.job.main.class"

        emr_step_id = emr_cluster_client.add_step(step_name, command_str)
        emr_step_status, emr_step_failure_details = emr_cluster_client.get_step_status(emr_step_id)

        assert str(emr_step_id).startswith("s-")
        assert emr_step_status == "STARTING"
        assert emr_step_failure_details is None

    @pytest.mark.emr
    @moto.mock_emr
    def test_wait_for_step_completion_without_state_change(self):
        with pytest.raises(M3DAWSAPIException):
            # In this test we expect exception because the state of the step will be STARTING
            emr_cluster_client, _ = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            emr_cluster_client.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

    @pytest.mark.emr
    def test_add_step_to_cluster_with_state_change(self):
        mock_emr_obj = moto.mock_emr()
        with mock_emr_obj:
            emr_cluster_client, emr_cluster_id = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            logging.info(str(emr_step_id))

            cluster_steps = emr_cluster_client.get_list_of_steps()
            assert 1 == len(cluster_steps)
            assert cluster_steps[0] == emr_step_id

            emr_step_status, _ = emr_cluster_client.get_step_status(emr_step_id)
            assert emr_step_status == "STARTING"

            # "STARTING" is not a valid EMR Step state, so we will change it to "RUNNING"
            emr_backend = mock_emr_obj.backends[self.aws_region]
            fake_cluster = emr_backend.clusters[emr_cluster_id]
            fake_cluster.steps[0].state = "RUNNING"

            def complete_step():
                # Wait for some time to let EMRClusterClient poll a few times.
                fake_cluster.steps[0].state = "COMPLETED"

            with ConcurrentExecutor(complete_step, 0.2):
                emr_cluster_client.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

    @pytest.mark.emr
    @moto.mock_s3
    def test_add_step_to_cluster_fail_without_output(self):
        mock_emr_obj = moto.mock_emr()
        with mock_emr_obj:
            emr_cluster_client, emr_cluster_id = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            s3_resource = Boto3Util.create_s3_resource()
            s3_resource.create_bucket(Bucket="mybucket")

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            cluster_steps = emr_cluster_client.get_list_of_steps()
            assert 1 == len(cluster_steps)
            assert cluster_steps[0] == emr_step_id

            emr_step_status, _ = emr_cluster_client.get_step_status(emr_step_id)
            assert emr_step_status == "STARTING"

            # "STARTING" is not a valid EMR Step state, so we will change it to "RUNNING"
            emr_backend = mock_emr_obj.backends[self.aws_region]
            fake_cluster = emr_backend.clusters[emr_cluster_id]
            fake_step = fake_cluster.steps[0]
            fake_step.state = "RUNNING"

            def fail_step():
                fake_step.state = "FAILED"

            # Make sure that we do not wait for 300 seconds for gz file to be available.
            EMRClusterClient.AWSConstants.S3_FILE_AVAILABILITY_TIMEOUT_SECONDS = self.timeout_seconds

            # Required for correct log path generation in MockedMethod.
            MockedMethod.emr_cluster_id = emr_cluster_id

            stderr_gz_path = MockedMethod.log_file_template.format(
                emr_cluster_id=emr_cluster_id,
                emr_step_id=emr_step_id
            )

            err_msg = "File {} failed to be available after {} seconds.".\
                format(stderr_gz_path, self.timeout_seconds)

            with pytest.raises(M3DAWSAPIException, match=err_msg):
                # Wait for some time to let EMRClusterClient poll a few times.
                with ConcurrentExecutor(fail_step, 0.4):
                    with patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient.get_step_status",
                               side_effect=MockedMethod.get_step_status_mocked):
                        emr_cluster_client.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

    @pytest.mark.emr
    @moto.mock_s3
    def test_add_step_to_cluster_fail_with_output(self):
        mock_emr_obj = moto.mock_emr()
        with mock_emr_obj:
            emr_cluster_client, emr_cluster_id = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            s3_resource = Boto3Util.create_s3_resource()
            s3_resource.create_bucket(Bucket="mybucket")

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            cluster_steps = emr_cluster_client.get_list_of_steps()
            assert 1 == len(cluster_steps)
            assert cluster_steps[0] == emr_step_id

            emr_step_status, _ = emr_cluster_client.get_step_status(emr_step_id)
            assert emr_step_status == "STARTING"

            # "STARTING" is not a valid EMR Step state, so we will change it to "RUNNING"
            emr_backend = mock_emr_obj.backends[self.aws_region]
            fake_cluster = emr_backend.clusters[emr_cluster_id]
            fake_step = fake_cluster.steps[0]
            fake_step.state = "RUNNING"

            # Make sure that we do not wait for 300 seconds for gz file to be available.
            EMRClusterClient.AWSConstants.S3_FILE_AVAILABILITY_TIMEOUT_SECONDS = self.timeout_seconds

            # Required for correct log path generation in MockedMethod.
            MockedMethod.emr_cluster_id = emr_cluster_id

            stderr_gz_path = MockedMethod.log_file_template.format(
                emr_cluster_id=emr_cluster_id,
                emr_step_id=emr_step_id
            )

            expected_content = "Lots of content here!!!"

            def fail_step_and_write_output():
                fake_step.state = "FAILED"

                time.sleep(0.3)

                compressed_content = TestEMRClusterClient._compress_string(expected_content)

                bucket, key = emr_cluster_client.s3_util.get_bucket_and_key(stderr_gz_path)
                s3_resource.Bucket(bucket).put_object(Key=key, Body=compressed_content)

            with pytest.raises(M3DAWSAPIException) as exc:
                # Wait for some time to let EMRClusterClient poll a few times.
                with ConcurrentExecutor(fail_step_and_write_output, 0.3):
                    with patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient.get_step_status",
                               side_effect=MockedMethod.get_step_status_mocked):
                        emr_cluster_client.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

            err_msg = "EMR Step with cluster_id='{}' and step_id='{}' failed to complete".\
                format(emr_cluster_id, emr_step_id)

            assert err_msg in str(exc.value)
            assert stderr_gz_path in str(exc.value)

            resulting_content = emr_cluster_client.s3_util.read_gzip_file_content(stderr_gz_path)
            assert expected_content == resulting_content

    @pytest.mark.emr
    @moto.mock_emr
    def test_wait_for_spark_step_completion(self):
        with pytest.raises(M3DAWSAPIException):
            # In this test we expect exception because the state of the step will be starting
            emr_cluster_client, _ = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            emr_cluster_client.wait_for_spark_step_completion(emr_step_id)

    @pytest.mark.emr
    @moto.mock_emr
    def test_get_list_of_steps(self):
        emr_cluster_client, _ = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        step_name = "Test_Step_{}"
        command_str_0 = "/usr/bin/spark-submit --class spark.job.main.class"
        command_str_1 = "spark-submit --class totally.different.main.class config.json"
        command_str_2 = "hive --silent -f s3://app.bucket/path/to/query.hql"

        emr_step_id_0 = emr_cluster_client.add_step(step_name.format(0), command_str_0)
        emr_step_id_1 = emr_cluster_client.add_step(step_name.format(1), command_str_1)
        emr_step_id_2 = emr_cluster_client.add_step(step_name.format(2), command_str_2)

        step_ids = emr_cluster_client.get_list_of_steps()

        assert len(step_ids) == 3

        assert emr_step_id_0 == step_ids[2]
        assert emr_step_id_1 == step_ids[1]
        assert emr_step_id_2 == step_ids[0]

    @pytest.mark.emr
    def test_get_step_output_path(self):
        mock_emr_obj = moto.mock_emr()
        with mock_emr_obj:
            emr_cluster_client, emr_cluster_id = self.env_setup(
                self.emr_cluster_name,
                self.aws_region,
                self.aws_credentials,
                self.timeout_seconds,
                self.retry_seconds,
                self.long_timeout_seconds
            )

            step_name = "Test_Step"
            command_str = "/usr/bin/spark-submit --class spark.job.main.class"

            emr_step_id = emr_cluster_client.add_step(step_name, command_str)

            # Change step state to COMPLETED
            emr_backend = mock_emr_obj.backends[self.aws_region]
            fake_cluster = emr_backend.clusters[emr_cluster_id]
            fake_cluster.steps[0].state = "COMPLETED"

            emr_cluster_client.wait_for_step_completion(emr_step_id, self.long_timeout_seconds)

            output_file = emr_cluster_client.get_step_output_path(emr_step_id)

            expected_output_file = "s3://mybucket/log/{}/steps/{}/stdout.gz".format(
                emr_cluster_client.emr_cluster_id, emr_step_id
            )

            assert output_file == expected_output_file

    @pytest.mark.emr
    @moto.mock_emr
    def test_get_step_err_path(self):
        emr_cluster_client, emr_cluster_id = self.env_setup(
            self.emr_cluster_name,
            self.aws_region,
            self.aws_credentials,
            self.timeout_seconds,
            self.retry_seconds,
            self.long_timeout_seconds
        )

        step_name = "Test_Step"
        command_str = "/usr/bin/spark-submit --class spark.job.main.class"

        emr_step_id = emr_cluster_client.add_step(step_name, command_str)

        MockedMethod.emr_cluster_id = emr_cluster_id
        with patch("m3d.hadoop.emr.emr_cluster_client.EMRClusterClient.get_step_status",
                   side_effect=MockedMethod.get_step_status_mocked):
            err_file = emr_cluster_client.get_step_err_path(emr_step_id)

        expected_err_file = "s3://mybucket/log/{}/steps/{}/stderr.gz".format(
            emr_cluster_client.emr_cluster_id, emr_step_id
        )

        assert err_file == expected_err_file
