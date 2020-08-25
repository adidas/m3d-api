import logging

from m3d.exceptions import m3d_exceptions
from m3d.system.data_system import DataSystem
from m3d.system.table import Table


class M3D(object):

    @staticmethod
    def create_emr_cluster(
            config,
            destination_system,
            destination_database,
            destination_environment,
            core_instance_count,
            core_instance_type,
            master_instance_type,
            emr_version=None,
            ebs_size=None
    ):
        from m3d.hadoop.emr import emr_system
        emr = emr_system.EMRSystem(
            config,
            destination_system,
            destination_database,
            destination_environment
        )

        emr_cluster_id = emr.create_emr_cluster(
            core_instance_type=core_instance_type,
            core_instance_count=core_instance_count,
            master_instance_type=master_instance_type,
            emr_version=emr_version,
            ebs_size=ebs_size
        )

        logging.info("EMR Cluster ID: {}".format(emr_cluster_id))
        return emr_cluster_id

    @staticmethod
    def add_emr_cluster_tags(
            config,
            destination_system,
            destination_database,
            destination_environment,
            emr_cluster_id,
            cluster_tags
    ):
        from m3d.hadoop.emr.emr_system import EMRSystem
        emr_system = EMRSystem(
            config,
            destination_system,
            destination_database,
            destination_environment,
            emr_cluster_id
        )
        emr_system.add_cluster_tags(cluster_tags)

    @staticmethod
    def delete_emr_cluster(
            config,
            destination_system,
            destination_database,
            destination_environment,
            emr_cluster_id
    ):
        from m3d.hadoop.emr.emr_system import EMRSystem
        emr = EMRSystem(
            config,
            destination_system,
            destination_database,
            destination_environment
        )

        emr.delete_emr_cluster(emr_cluster_id)

    # create table (l0 and l1)
    @staticmethod
    def create_table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            destination_table_location_prefix=None,
            emr_cluster_id=None
    ):
        # create abstract table object to retrieve source technology
        abstract_table = Table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        destination_system_technology = abstract_table.get_destination_technology()

        # hadoop
        if destination_system_technology == DataSystem.SystemTechnology.HIVE:
            if abstract_table.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.emr.emr_system import EMRSystem
                emr_system = EMRSystem(
                    config,
                    destination_system,
                    destination_database,
                    destination_environment,
                    emr_cluster_id
                )
                emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.create_table.__name__)
                emr_system.create_table(destination_table, destination_table_location_prefix)
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(abstract_table.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDestinationSystemException(destination_system_technology)

    @staticmethod
    def drop_table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            emr_cluster_id=None
    ):
        # create abstract table object to retrieve source technology
        abstract_table = Table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        destination_system_technology = abstract_table.get_destination_technology()

        # hadoop
        if destination_system_technology == DataSystem.SystemTechnology.HIVE:
            if abstract_table.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.emr.emr_system import EMRSystem
                emr_system = EMRSystem(
                    config,
                    destination_system,
                    destination_database,
                    destination_environment,
                    emr_cluster_id
                )
                emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.drop_table.__name__)
                emr_system.drop_table(destination_table)
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(abstract_table.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDestinationSystemException(destination_system_technology)

    # load table from l0 to l1
    @staticmethod
    def load_table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            load_type,
            emr_cluster_id=None,
            spark_params=None
    ):
        ds = DataSystem(config, destination_system, destination_database, destination_environment)

        # hadoop
        if ds.database_type == DataSystem.DatabaseType.EMR:
            if ds.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.load.load_executor_hadoop import LoadExecutorHadoop
                LoadExecutorHadoop.create(
                    config_path=config,
                    destination_system=destination_system,
                    destination_database=destination_database,
                    destination_environment=destination_environment,
                    destination_table=destination_table,
                    load_type=load_type,
                    emr_cluster_id=emr_cluster_id,
                    spark_params_str=spark_params
                ).run()
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(ds.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDatabaseTypeException(ds.database_type)

    @staticmethod
    def truncate_table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            emr_cluster_id=None
    ):
        # create abstract table object to retrieve source technology
        abstract_table = Table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        destination_system_technology = abstract_table.get_destination_technology()

        if destination_system_technology == DataSystem.SystemTechnology.HIVE:
            if abstract_table.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.emr.emr_system import EMRSystem
                emr_system = EMRSystem(
                    config,
                    destination_system,
                    destination_database,
                    destination_environment,
                    emr_cluster_id
                )
                emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.truncate_table.__name__)
                emr_system.truncate_table(destination_table)
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(abstract_table.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDestinationSystemException(destination_system_technology)

    # create out_view (l2)
    @staticmethod
    def create_out_view(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            emr_cluster_id=None
    ):
        # create abstract table object to retrieve source technology
        abstract_table = Table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        destination_system_technology = abstract_table.get_destination_technology()

        # hadoop
        if destination_system_technology == DataSystem.SystemTechnology.HIVE:
            if abstract_table.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.emr.emr_system import EMRSystem
                emr_system = EMRSystem(
                    config,
                    destination_system,
                    destination_database,
                    destination_environment,
                    emr_cluster_id
                )
                emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.create_out_view.__name__)
                emr_system.create_out_view(destination_table)
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(abstract_table.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDestinationSystemException(destination_system_technology)

    @staticmethod
    def drop_out_view(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table,
            emr_cluster_id=None
    ):
        # create abstract table object to retrieve source technology
        abstract_table = Table(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_table
        )
        destination_system_technology = abstract_table.get_destination_technology()

        # hadoop
        if destination_system_technology == DataSystem.SystemTechnology.HIVE:
            if abstract_table.storage_type == DataSystem.StorageType.S3:
                from m3d.hadoop.emr.emr_system import EMRSystem
                emr_system = EMRSystem(
                    config,
                    destination_system,
                    destination_database,
                    destination_environment,
                    emr_cluster_id
                )
                emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.drop_out_view.__name__)
                emr_system.drop_out_view(destination_table)
            else:
                raise m3d_exceptions.M3DUnsupportedStorageException(abstract_table.storage_type)
        else:
            raise m3d_exceptions.M3DUnsupportedDestinationSystemException(destination_system_technology)

    # run algorithm
    @staticmethod
    def run_algorithm(
            config,
            destination_system,
            destination_database,
            destination_environment,
            algorithm_instance,
            emr_cluster_id=None,
            ext_params=None
    ):
        ds = DataSystem(config, destination_system, destination_database, None)
        if ds.database_type == DataSystem.DatabaseType.EMR:
            from m3d.hadoop.algorithm.algorithm_executor_hadoop import AlgorithmExecutorHadoop
            AlgorithmExecutorHadoop.create(
                config_path=config,
                destination_system=destination_system,
                destination_database=destination_database,
                destination_environment=destination_environment,
                algorithm_instance=algorithm_instance,
                emr_cluster_id=emr_cluster_id,
                ext_params_str=ext_params
            ).run()

        else:
            raise m3d_exceptions.M3DUnsupportedDatabaseTypeException(ds.database_type)

    @staticmethod
    def drop_dataset(
            config,
            destination_system,
            destination_database,
            destination_environment,
            destination_dataset,
            emr_cluster_id=None
    ):
        from m3d.hadoop.emr.emr_system import EMRSystem
        emr_system = EMRSystem(
            config,
            destination_system,
            destination_database,
            destination_environment,
            emr_cluster_id)

        emr_system.add_cluster_tag(EMRSystem.EMRClusterTag.API_METHOD, M3D.drop_dataset.__name__)
        emr_system.drop_dataset(destination_dataset)
