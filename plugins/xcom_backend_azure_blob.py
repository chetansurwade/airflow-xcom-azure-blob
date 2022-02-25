from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import pandas as pd
import uuid

class XcomAzureBlobBackend(BaseXCom):
    PREFIX = "xcom-abfs://"
    CONTAINER_NAME = "xcom-data"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            hook        = WasbHook()
            key         = "data_csv_" + str(uuid.uuid4())
            filename    = f"{key}.csv"
            value.to_csv(filename,index=False)
            hook.load_file(
                file_path=filename,
                blob_name=key,
                container_name=XcomAzureBlobBackend.CONTAINER_NAME
            )
            value = XcomAzureBlobBackend.PREFIX + key
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(XcomAzureBlobBackend.PREFIX):
            hook    = WasbHook()
            key     = result.replace(XcomAzureBlobBackend.PREFIX, "")
            filename = f"{key}.csv"
            hook.get_file(
                file_path=filename,
                container_name=XcomAzureBlobBackend.CONTAINER_NAME,
                blob_name=key
            )
            hook.delete_file(
                container_name=XcomAzureBlobBackend.CONTAINER_NAME,
                blob_name=key,
                ignore_if_missing=True
            )
            result = pd.read_csv(filename)
        return result
