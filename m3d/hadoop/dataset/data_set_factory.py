from m3d.exceptions.m3d_exceptions import M3DUnsupportedLoadTypeException, M3DUnsupportedDataTypeException
from m3d.hadoop.core.hive_table import HiveTable
from m3d.hadoop.dataset.semistructured_data_set import SemistructuredDataSet
from m3d.hadoop.emr.s3_table import S3Table
from m3d.util.data_types import DataType


class DataSetFactory(object):

    @staticmethod
    def create_data_set(execution_system, load_type, data_type, data_set_name):

        if data_type == DataType.STRUCTURED:
            data_set = S3Table(
                emr_system=execution_system,
                destination_table=data_set_name
            )
        elif data_type == DataType.SEMISTRUCTURED:
            if load_type == HiveTable.TableLoadType.APPEND:
                data_set = SemistructuredDataSet(
                    emr_system=execution_system,
                    data_set_name=data_set_name
                )
            else:
                raise M3DUnsupportedLoadTypeException(
                    load_type=load_type,
                    message="Loading algorithm {} not support for data type {}.".format(load_type, data_type)
                )
        else:
            raise M3DUnsupportedDataTypeException(
                message="Data Type {} not available.".format(data_type)
            )

        return data_set
