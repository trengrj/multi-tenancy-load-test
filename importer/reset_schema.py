import weaviate
import time
import random
import sys
import uuid
import os
import requests
from loguru import logger
from typing import Optional

host = os.getenv("HOST")
replication_factor = int(os.getenv("REPLICATION_FACTOR") or 1)
client = weaviate.Client(f"http://{host}", timeout_config=(20, 240))


def reset_schema(client: weaviate.Client):
    client.schema.delete_all()
    class_payload = {
        "class": "MultiTenancyTest",
        "description": "A class to test multi-tenancy with flat index type",
        "vectorizer": "none",
        "replicationConfig": {
            "factor": replication_factor,
        },
        "properties": [
            {
                "dataType": ["text"],
                "tokenization": "field",
                "name": "tenant_id",
            },
        ],
        "vectorIndexType": "flat",
        "vectorIndexConfig": {
            "distance": "dot",
            "fullyOnDisk": True,
            "quantization": False,
        },
        "multiTenancyConfig": {
            "enabled": True,
            "autoTenantCreation": True,
        },
    }
    res = requests.post(f"http://{host}/v1/schema", json=class_payload)
    print(res.status_code)
    if res.status_code > 299:
        print(res.json())
        sys.exit(1)


reset_schema(client)
