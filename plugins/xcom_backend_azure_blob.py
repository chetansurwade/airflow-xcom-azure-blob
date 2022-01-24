from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
import json
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
        if not isinstance(value, (str, dict, list, pd.DataFrame)):
            hook        = WasbHook()
            key         = "data_json_" + str(uuid.uuid4())
            filename    = f"{key}.json"

            with open(filename, 'w') as f:
                json.dump(json.loads(str(value)), f)

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
            file_format = key.split('_')[1]
            filename = f"{key}.{file_format}"
            hook.get_file(
                file_path=filename,
                container_name=XcomAzureBlobBackend.CONTAINER_NAME,
                blob_name=key
            )
            if file_format == 'csv':
                result = pd.read_csv(filename)
            else:
                result = json.load(filename)
            hook.delete_file(
                container_name=XcomAzureBlobBackend.CONTAINER_NAME,
                blob_name=key,
                ignore_if_missing=True
            )
            
        return result
