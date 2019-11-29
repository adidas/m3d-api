import datetime
import json
import logging
import os
import random
import string

from m3d.config.config_service import ConfigService
from m3d.exceptions.m3d_exceptions import M3DEMRException, M3DEMRApiException, M3DIllegalArgumentException
from m3d.hadoop.emr.aws_s3_credentials_wrapper import AWSS3CredentialsWrapper
from m3d.hadoop.emr.emr_cluster_client import EMRClusterClient
from m3d.system.data_system import DataSystem
from m3d.util.aws_credentials import AWSCredentials
from m3d.util.exception_logger import exception_logger
from m3d.util.util import Util


def check_emr_cluster_client(func):
    """
    This decorator is used to check that EMRSystem object has an initialized emr_cluster_client member.
    This will make sure that decorated function using it can proceed.

    If the emr_cluster_client member is not initialized then it will throw an error of M3DEMRException type with
    message about passing emr_cluster_id to constructor of EMRSystem.

    :param func: member function of EMRSystem for which it will do the check
    :return: None. Throws M3DEMRException if emr_cluster_client member of EMRSystem is None
    """
    def wrapper(*args, **kwargs):
        emr_system = args[0]  # this is self

        if emr_system.emr_cluster_client is None:
            raise M3DEMRException(
                "EMRClusterClient is not initiated. EMRSystem.{}() method cannot run without a valid emr_cluster_id "
                "passed to the constructor of EMRSystem.".format(func.__name__))

        return func(*args, **kwargs)

    return wrapper


class EMRSystem(DataSystem):
    DEFAULT_ID_LENGTH = 10
    DATETIME_FORMAT = "%Y%m%dT%H%M%S"

    class EMRClusterTag(object):
        API_METHOD = "ApiMethod"
        SYSTEM = "System"
        ENVIRONMENT = "Environment"
        ALGORITHM_INSTANCE = "AlgorithmInstance"
        ALGORITHM_CLASS = "AlgorithmClass"
        SOURCE_TABLE = "SourceTable"
        TARGET_TABLE = "TargetTable"
        TARGET_DATASET = "TargetDataset"
        SOURCE_VIEW = "SourceView"
        TARGET_VIEW = "TargetView"
        LOAD_TYPE = "LoadType"

    SPARK_MANDATORY_KEYS = {
        # "spark.driver.memory",
        # "spark.executor.instances",
        # "spark.executor.memory",
        # "spark.yarn.queue"
    }

    def __init__(
            self,
            config,
            source_system,
            database,
            environment,
            emr_cluster_id=None,
            spark_params=None
    ):
        """
        Initialize Hadoop system

        :param config: system config file
        :param source_system: destination system code
        :param database: destination database code
        :param environment: destination schema code
        :param emr_cluster_id: id EMR cluster
        :param spark_params: spark specific parameters
        """

        # call super constructor
        super(EMRSystem, self).__init__(config, source_system, database, environment)

        self.scon_full_path = self.config_service.get_scon_path(source_system, database)

        # system config files
        with open(self.scon_full_path) as data_file:
            params_system = json.load(data_file)

        # S3 buckets
        self.bucket_landing = params_system["environments"][self.environment]["s3_buckets"]["landing"]
        self.bucket_lake = params_system["environments"][self.environment]["s3_buckets"]["lake"]
        self.bucket_mart_cal = params_system["environments"][self.environment]["s3_buckets"]["mart_cal"]
        self.bucket_application = params_system["environments"][self.environment]["s3_buckets"]["application"]
        self.bucket_log = params_system["environments"][self.environment]["s3_buckets"]["log"]

        # EMR default configuration
        self.default_emr_version = params_system["emr"]["default_emr_version"]
        self.default_ebs_size = params_system["emr"]["default_ebs_size"]

        # base M3D application deployment directory
        self.s3_deployment_dir_base = params_system["environments"][self.environment]["s3_deployment_dir_base"]

        # AWS credentials
        self.aws_api_credentials = AWSCredentials.from_file(params_system["aws_api_credentials"])
        self.aws_s3_put_credentials = AWSCredentials.from_file(params_system["aws_s3_put_credentials"])
        self.aws_s3_del_credentials = AWSCredentials.from_file(params_system["aws_s3_del_credentials"])

        # configurations
        self.api_action_timeout_seconds = params_system["api_action_timeout_seconds"]
        self.api_action_polling_interval_seconds = params_system["api_action_polling_interval_seconds"]
        self.api_long_timeout_seconds = params_system["api_long_timeout_seconds"]
        self.aws_region = params_system["aws_region"]
        self.packages_to_deploy = params_system["packages_to_deploy"]
        self.configs_to_deploy = params_system["configs_to_deploy"]

        # base directories
        self.s3_dir_base = params_system["s3_dir_base"]

        # defined sub-directories
        self.subdir_archive = params_system["subdir"]["archive"]
        self.subdir_header = params_system["subdir"]["header"]
        self.subdir_config = params_system["subdir"]["config"]
        self.subdir_data = params_system["subdir"]["data"]
        self.subdir_data_backup = DataSystem.DirectoryName.DATA_BACKUP
        self.subdir_error = params_system["subdir"]["error"]
        self.subdir_work = params_system["subdir"]["work"]
        self.subdir_log = params_system["subdir"]["log"]
        self.subdir_apps = params_system["subdir"]["apps"]
        self.subdir_m3d_engine = params_system["subdir"]["m3d_engine"]
        self.subdir_loading = params_system["subdir"]["loading"]
        self.subdir_full_load = params_system["subdir"]["full_load"]
        self.subdir_delta_load = params_system["subdir"]["delta_load"]
        self.subdir_append_load = params_system["subdir"]["append_load"]
        self.subdir_black_whole = params_system["subdir"]["black_whole"]
        self.subdir_credentials = params_system["subdir"]["credentials"]
        self.subdir_keytab = params_system["subdir"]["keytab"]
        self.subdir_tmp = params_system["subdir"]["tmp"]

        # deployment directories of M3D application and metadata (tconx)
        self.subdir_code = params_system["subdir"]["m3d"]
        self.subdir_metadata = params_system["subdir"]["metadata"]

        # spark arguments
        self.spark_main_class = params_system["spark"]["main_class"]
        self.spark_jar_name = params_system["spark"]["jar_name"]

        self.spark_params = spark_params

        self.version_filename = "version.txt"

        s3_deployment_dir = "{protocol}{bucket}{base_dir}".format(
            protocol=ConfigService.Protocols.S3,
            bucket=self.bucket_application,
            base_dir=self.s3_deployment_dir_base
        )

        # derived directories: apps
        self.dir_apps = os.path.join(s3_deployment_dir, self.environment, self.subdir_apps)
        self.dir_apps_algorithm = os.path.join(self.dir_apps, self.subdir_m3d_engine)
        self.dir_apps_loading = os.path.join(self.dir_apps, self.subdir_loading)

        # tmp directory in S3
        self.dir_tmp_s3 = os.path.join(s3_deployment_dir, self.environment, self.subdir_tmp)

        # tmp directory in local filesystem
        self.dir_tmp_local = os.path.join("/", self.subdir_tmp)

        self.dir_m3d_api_deployment = os.path.join(
            s3_deployment_dir,
            self.environment,
            self.subdir_code,
            self.config_service.subdir_projects_m3d_api
        )

        self.dir_metadata_deployment = os.path.join(
            s3_deployment_dir,
            self.environment,
            self.subdir_metadata,
            self.config_service.subdir_projects_m3d_api
        )

        self.spark_jar_path = os.path.join(
            self.dir_m3d_api_deployment,
            self.spark_jar_name
        )

        # AWSFactoryS3Wrapper will do the routing of methods to correct underlying S3Util object.
        self.s3_util = AWSS3CredentialsWrapper(
            [self.bucket_application, self.bucket_log],
            [self.bucket_landing, self.bucket_lake, self.bucket_mart_cal],
            self.aws_api_credentials,
            self.aws_s3_put_credentials,
            self.aws_s3_del_credentials
        )

        # Set up EMRClusterClient
        self.emr_cluster_id = emr_cluster_id

        if emr_cluster_id is not None:
            self.emr_cluster_client = self._create_emr_cluster_client(emr_cluster_id)
        else:
            self.emr_cluster_client = None

    def create_table(self, destination_table):
        from m3d.hadoop.emr.s3_table import S3Table
        full_table_name = "{}.{}".format(self.db_lake, destination_table)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_TABLE, full_table_name)
        S3Table(self, destination_table).create_tables()

    def drop_table(self, destination_table):
        from m3d.hadoop.emr.s3_table import S3Table
        full_table_name = "{}.{}".format(self.db_lake, destination_table)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_TABLE, full_table_name)
        S3Table(self, destination_table).drop_tables()

    def drop_dataset(self, destination_dataset):
        """
        Semi-structured datasets are not associated with any Hive metadata and therefore we cannot drop
        and truncate them like tables. So, this function drops a semi-structured dataset.
        All landing and lake data and folders associated with the dataset will be permanently deleted.
        :param destination_dataset: dataset to drop
        """
        from m3d.hadoop.dataset.semistructured_dataset import SemistructuredDataSet
        full_dataset_name = "{}.{}".format(self.db_lake, destination_dataset)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_DATASET, full_dataset_name)
        SemistructuredDataSet(self, destination_dataset).drop_datasets()

    def create_lake_out_view(self, destination_table):
        from m3d.hadoop.emr.s3_table import S3Table
        full_table_name = "{}.{}".format(self.db_lake_out, destination_table)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_VIEW, full_table_name)
        S3Table(self, destination_table).create_lake_out_view()

    def drop_lake_out_view(self, destination_table):
        from m3d.hadoop.emr.s3_table import S3Table
        full_table_name = "{}.{}".format(self.db_lake_out, destination_table)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_VIEW, full_table_name)
        S3Table(self, destination_table).drop_lake_out_view()

    def truncate_table(self, destination_table):
        from m3d.hadoop.emr.s3_table import S3Table
        full_table_name = "{}.{}".format(self.db_lake, destination_table)
        self.add_cluster_tag(self.EMRClusterTag.TARGET_TABLE, full_table_name)
        S3Table(self, destination_table).truncate_tables()

    # method to create emr cluster in adidas-linked-milkyway using boto3 calls (no factory api)
    def create_emr_cluster(self, core_instance_type, core_instance_count,
                           master_instance_type, emr_version, ebs_size):

        aws_region = 'us-east-2'
        log_uri = 's3://aws-logs-923130144402-us-east-2/elasticmapreduce/'
        credentials = self.aws_api_credentials

        if emr_version is None:
            emr_version = self.default_emr_version

        if ebs_size is None:
            ebs_size = self.default_ebs_size

        client = OpenSourceClient(aws_region, log_uri, credentials)

        # start cluster without job
        cluster_id = client.start_cluster(log_uri, emr_version, ebs_size, master_instance_type,
                                          core_instance_type, core_instance_count)

        emr_cluster_id = cluster_id['JobFlowId']

        self.emr_cluster_client = self._create_emr_cluster_client(emr_cluster_id)
        self.add_cluster_tags({
            EMRSystem.EMRClusterTag.SYSTEM: self.source_system,
            EMRSystem.EMRClusterTag.ENVIRONMENT: self.environment
        })

        logging.info("Creation of \"{}\" EMR cluster has been initialized.".format(
            emr_cluster_id
        ))
        return emr_cluster_id

    @check_emr_cluster_client
    def add_cluster_tag(self, key, value):
        try:
            self.emr_cluster_client.add_cluster_tag(key, value)
        except M3DEMRApiException:
            logging.warning("Unable to add tag to the cluster: {} -> {}".format(key, value))

    @check_emr_cluster_client
    def add_cluster_tags(self, tags_dict):
        try:
            self.emr_cluster_client.add_cluster_tags(tags_dict)
        except M3DEMRApiException:
            logging.warning("Unable to add tags to the cluster: {}".format(tags_dict))

    @check_emr_cluster_client
    @exception_logger(Exception)
    def execute_hive(self, hql, return_output=False):
        # Put HQL statement to a file since it can be longer than allowed length of EMR step parameter.
        datetime_str = Util.get_formatted_utc_now(EMRSystem.DATETIME_FORMAT)
        id_str = EMRSystem._generate_random_id()

        hql_filename = "{}.{}{}".format(datetime_str, id_str, ConfigService.Extensions.HQL)
        hql_path_local = os.path.join(self.dir_tmp_local, hql_filename)
        hql_path_s3 = os.path.join(self.dir_tmp_s3, hql_filename)

        with open(hql_path_local, "w") as hql_file:
            hql_file.write(hql)

        self.s3_util.upload_object(hql_path_local, hql_path_s3)

        # Create hive command line.
        hive_cmd = "hive --silent -f {}".format(hql_path_s3)

        # Add step to EMR cluster.
        step_name = "Hive EMR Step: datetime=\"{}\", id=\"{}\"".format(datetime_str, id_str)
        emr_step_id = self.emr_cluster_client.add_step(step_name, hive_cmd)

        self.emr_cluster_client.wait_for_step_completion(emr_step_id)

        if return_output:
            output_file = self.emr_cluster_client.get_step_output_path(emr_step_id)
            logging.info("Waiting for availability of output file: '{}'.".format(output_file))

            self.s3_util.wait_for_file_availability(
                output_file,
                self.emr_cluster_client.polling_interval_seconds,
                EMRClusterClient.AWSConstants.S3_FILE_AVAILABILITY_TIMEOUT_SECONDS
            )
            file_content = self.s3_util.read_gzip_file_content(output_file)

            return file_content

        return None

    @check_emr_cluster_client
    def run_command_in_cluster(self, cmd_str, emr_step_name, spark_timeout=True):
        """
        This function will execute given command as an EMR Step and wait for its completion.

        :param cmd_str: string representing command to run. This can be any shell command,
                        including hive and spark-submit calls.
        :param emr_step_name: name to be assigned to EMR Step
        :param spark_timeout: If set to true (default), timeout for EMR Step will be set to
                              long spark step timeout. Otherwise shorter timeout will be used.
        """
        # Add step to EMR cluster and wait for its completion.
        logging.info("The following command will be executed as EMR Step: '{}'".format(cmd_str))
        emr_step_id = self.emr_cluster_client.add_step(emr_step_name, cmd_str)

        if spark_timeout:
            self.emr_cluster_client.wait_for_spark_step_completion(emr_step_id)
        else:
            self.emr_cluster_client.wait_for_step_completion(emr_step_id)

    def create_spark_submit_str(self, spark_params, algorithm_class, file_json_s3):
        """
        Return spark-submit string

        :param spark_params: Spark parameters for spark-submit call
        :param algorithm_class: Spark algorithm to be executed
        :param file_json_s3: full path to JSON file with algorithm parameters
        :return: spark2-submit String to be executed in shell
        """

        EMRSystem.validate_spark_params(spark_params)

        spark_submit = "spark-submit "
        spark_submit += "--master yarn "
        spark_submit += "--deploy-mode cluster "

        for key, value in spark_params.items():
            spark_submit += "--conf {}={} ".format(key, value)

        spark_submit += "--class {} ".format(self.spark_main_class)
        spark_submit += self.spark_jar_path + " "
        spark_submit += algorithm_class + " "
        spark_submit += file_json_s3 + " "
        spark_submit += self.storage_type

        return spark_submit

    def spark_submit(self, spark_params, algorithm_class, file_json_s3, step_name):
        # Create spark-submit string
        spark_str = self.create_spark_submit_str(spark_params, algorithm_class, file_json_s3)
        self.run_command_in_cluster(spark_str, step_name)

    def _create_emr_cluster_client(self, emr_cluster_id):
        return EMRClusterClient(
            emr_cluster_id,
            self.aws_region,
            self.aws_api_credentials,
            self.api_action_timeout_seconds,
            self.api_action_polling_interval_seconds,
            self.api_long_timeout_seconds
        )

    def _generate_version_file(self):
        def read_deployment_history():
            if os.path.exists(self.version_filename):
                with open(self.version_filename, 'r') as version_file:
                    return version_file.read().strip()
            else:
                return None

        def update_deployment_history(current_line, history=None):
            with open(self.version_filename, 'w') as version_file:
                if history is None:
                    version_file.writelines([current_line + "\n"])
                else:
                    version_file.writelines([current_line + "\n", history + "\n"])

        deployment_history = read_deployment_history()
        current_time = datetime.datetime.now()
        branch_name = Util.execute_subprocess("git status | grep -E 'On branch .*' | tail -c +11")
        last_commit = Util.execute_subprocess("git log -1 | grep -E 'commit .*' | tail -c +8")

        current_deployment = "{date} {branch} ({commit})".format(
            date=current_time.strftime("%Y-%m-%d %H:%M:%S"),
            branch=branch_name.strip(),
            commit=last_commit.strip()
        )
        update_deployment_history(current_deployment, deployment_history)

    @staticmethod
    def _generate_random_id(id_length=DEFAULT_ID_LENGTH):
        random_id_str = ''.join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(id_length)
        )
        return random_id_str

    @staticmethod
    def from_data_system(data_system, emr_cluster_id):
        return EMRSystem(
            data_system.config,
            data_system.source_system,
            data_system.database,
            data_system.environment,
            emr_cluster_id,
        )

    @staticmethod
    def validate_spark_params(spark_params):
        for key in EMRSystem.SPARK_MANDATORY_KEYS:
            if key not in spark_params:
                raise M3DIllegalArgumentException("'{}' mandatory key of SparkParameters missing".format(key))
            if not spark_params[key]:
                raise M3DIllegalArgumentException("'{}' property of SparkParameters should have an assigned value".
                                                  format(key))

    def delete_emr_cluster(self, emr_cluster_id):

        aws_region = 'us-east-2'
        log_uri = 's3://aws-logs-923130144402-us-east-2/elasticmapreduce/'
        credentials = self.aws_api_credentials

        client = OpenSourceClient(aws_region, log_uri, credentials)
        client.terminate_cluster(emr_cluster_id)
        logging.info("Deletion of \"{}\" EMR cluster successful.".format(emr_cluster_id))


class OpenSourceClient(object):

    def __init__(
            self,
            aws_region,
            log_uri,
            aws_credentials,
    ):
        """

        :param aws_region: aws region where cluster is running
        :param log_uri: bucket where logs are saved to
        :param aws_credentials: tbd
        """
        import boto3

        self.aws_region = aws_region
        self.log_uri = log_uri
        self.client = boto3.client(
            'emr',
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            region_name=aws_region
        )

    def start_cluster(self, log_uri, emr_version, ebs_size,
                      master_instance_type, core_instance_type, core_instance_count):
        cluster_id = self.client.run_job_flow(
            Name='open source test cluster',
            LogUri=log_uri,
            ReleaseLabel=emr_version,
            Applications=[
                {
                    'Name': 'Ganglia',
                },
                {
                    'Name': 'Hive',
                },
                {
                    'Name': 'Hue',
                },
                {
                    'Name': 'Mahout',
                },
                {
                    'Name': 'Pig',
                },
                {
                    'Name': 'Tez',
                },
                {
                    'Name': 'Spark'
                }
             ],
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance_type,
                        'InstanceCount': 1,
                        'EbsConfiguration': {
                            'EbsBlockDeviceConfigs': [
                                {
                                    'VolumeSpecification': {
                                        'VolumeType': 'gp2',
                                        'SizeInGB': int(ebs_size)
                                    },
                                }
                            ]
                        }
                    },
                    {
                        'Name': "Slave",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': core_instance_type,
                        'InstanceCount': int(core_instance_count),
                    }
                ],
                'Ec2KeyName': '',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': '',
            },
            Steps=[

            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
            ],
        )
        return cluster_id

    def terminate_cluster(self, emr_cluster_id):
        self.client.terminate_job_flows(
            JobFlowIds=[
                emr_cluster_id,
            ]
        )
