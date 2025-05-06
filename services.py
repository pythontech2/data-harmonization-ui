import os
import time
from typing import Any, Dict, List

import requests
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


class DataHarmonizationService:
    def __init__(self):
        # Initialize MongoDB connection
        self.client = MongoClient(os.getenv("MONGODB_CONNECTION_STRING"))

        # Get database instance
        self.db = self.client[os.getenv("MONGODB_DATABASE_NAME")]

        # Get collection instance
        self.schema_collection_data = self.db["Data"]

        self.schema_collection_keymap = self.db["KeyMaps"]

    def convert_objectid_to_str(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_objectid_to_str(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_objectid_to_str(i) for i in obj]
        elif isinstance(obj, ObjectId):
            return str(obj)
        else:
            return obj

    def get_schema_versions(self) -> List[str]:
        """
        Get available schema versions from MongoDB based on provider and domain
        """
        try:
            # Query to get all documents with schemaVersion field
            query = (
                {"schemaVersion": {"$exists": True}},
                {"schemaVersion": 1, "_id": 0},
            )
            documents = list(self.schema_collection_data.find(*query))

            # Convert documents to a list of schemaVersion values
            version_list = [doc["schemaVersion"] for doc in documents]

            return (
                sorted(version_list, reverse=False) if version_list else []
            )  # Return sorted versions, newest first
        except Exception as e:
            print(f"Error fetching schema versions: {str(e)}")
            return []

    def submit_harmonization_request(
        self, form_data: Dict[str, Any], input_file: Any,generate_missing_key:bool
    ) -> Dict[str, Any]:
        """
        Submit harmonization request to the webhook
        """
        api_url = os.getenv("N8N_WEBHOOK_URL")  # Get the webhook url
        api_key = os.getenv("N8N_API_KEY")  # Get the api key
        workflow_id = os.getenv("WORKFLOW_ID")  # Get the workflow id

        # Prepare files
        files = {"input_file": (input_file.name, input_file, "application/json")}

        # Make the POST request
        response = requests.post(api_url, data=form_data, files=files, timeout=240)

        # Get the execution id
        response_2 = requests.get(
            f"https://riskdatalab.app.n8n.cloud/api/v1/executions?workflowId={workflow_id}",
            headers={"X-N8N-API-KEY": api_key},
        )
        execution_id = int(response_2.json()["data"][0]["id"])
        print("execution_id::", execution_id)

        if generate_missing_key:
            execution_id = execution_id + 1
        else:
            execution_id = execution_id + 2
        while True:
            # Get the execution status
            response_3 = requests.get(
                f"https://riskdatalab.app.n8n.cloud/api/v1/executions/{execution_id}",
                headers={"X-N8N-API-KEY": api_key},
            )
            if response_3.json()["finished"] and response_3.json()["mode"] == "webhook":
                print("In the if")
                return {
                    "status_code": response.status_code,
                    "response_data": response.json()
                    if response.status_code == 200
                    else response.text,
                }
            elif response_3.json()["mode"] == "error":
                return False
            else:
                time.sleep(10)

    def fetch_keymap_data(self, provider_name: str):
        try:
            query = {provider_name: {"$exists": True}}
            documents = list(self.schema_collection_keymap.find(query))
            if documents:
                return documents
            else:
                return f"No keymap data found for {provider_name}"
        except Exception as e:
            return f"Error fetching keymap data: {str(e)}"

    def fetch_data_from_target_schema(self, target_schema_version: str):
        try:
            print("target_schema_version::", target_schema_version)
            query = {"schemaVersion": target_schema_version}
            documents = list(self.schema_collection_data.find(query))
            if documents:
                return documents
            else:
                return f"No data found for {target_schema_version}"
        except Exception as e:
            return f"Error fetching data from target schema: {str(e)}"

    def update_collections_data(
        self, data_id, updated_schema, keymap_id, updated_keymap, provider_name
    ):
        """
        Update both Data and keyMaps collections.
        """
        try:
            # Update Data collection
            data_result = self.schema_collection_data.update_one(
                {"_id": data_id}, {"$set": {"schema": updated_schema}}
            )
            # Update keyMaps collection
            keymap_result = self.schema_collection_keymap.update_one(
                {"_id": keymap_id}, {"$set": {provider_name: updated_keymap}}
            )
            return data_result.modified_count > 0 or keymap_result.modified_count > 0
        except Exception as e:
            print(f"Error updating collections: {str(e)}")
            return False

    def final_workflow(self, target_schema_id, provider_name, input_json):
        try:
            # input_json is already a dict
            input_json = self.convert_objectid_to_str(input_json)
            payload = {
                provider_name: input_json,
                "_id": str(target_schema_id),
            }
            webhook_url = os.getenv("N8N_FINAL_WEBHOOK_URL")
            response = requests.post(webhook_url, json=payload)
            if response.status_code == 200:
                return {"success": True, "data": response.json()}
            else:
                return {"success": False, "error": response.text}
        except Exception as e:
            print(f"Error in final workflow: {str(e)}")
            return {"success": False, "error": str(e)}
