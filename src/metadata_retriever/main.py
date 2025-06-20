import urllib.request
import json
import os
import boto3
from log_config import logger
from io import StringIO
import csv
import json
import base64

base_url = os.getenv("SERVICENOW_BASE_URL", None)
username = os.getenv("SERVICENOW_USER", None)
password = os.getenv("SERVICENOW_PASSWORD", None)
finops_bucket = os.getenv("FINOPS_BUCKET", "")
cloudintelligence_bucket = os.getenv("CLOUDINTELLIGENCE_BUCKET", finops_bucket)
cudos_filename = os.getenv(
    "CUDOS_FILENAME", "reference_data/cudos_account_metadata.txt"
)
finops_automation_filename = os.getenv(
    "FINOPS_AUTOMATION_FILENAME", "reference_data/lambda_automation_metadata.json"
)

s3_client = boto3.client("s3")

# URL and headers setup
data_format = "CSV"
url = f"{base_url}/cmdb_ci_cloud_service_account_list.do?{data_format}=&sysparm_fields=name%2Caccount_id%2Cenvironment%2Cassignment_group%2Cassignment_group.email"
credentials = f"{username}:{password}"
encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
headers = {"Authorization": f"Basic {encoded_credentials}"}


def decode_snow_data(response):
    data = response.decode("utf-8")
    logger.info("Decoded ServiceNow data")
    logger.debug(data[:100])

    # Use StringIO to convert the string data into a file-like object
    csv_file = StringIO(data)

    # Read the CSV data
    csv_reader = csv.DictReader(csv_file)
    dict_list = [row for row in csv_reader]
    return dict_list


def format_cudos_data(
    dict_list: list[dict], assignment_group_mapping: dict, email_mapping: dict
) -> list[dict]:
    def remove_snow_team(temp_dict: dict, workload: str):
        if workload == "Not found":
            temp_dict["assignment_group"] = "Not found"
            temp_dict["email"] = "Not found"
        return temp_dict

    def dict_format(row):
        workload = split_workload(
            account_id=row["account_id"], workload_env=row["name"]
        )
        workload_type = identify_platform(workload)

        temp_dict = {
            "account_id": row["account_id"],
            "name": row["name"],
            "workload": workload,
            "workload_type": workload_type,
            "environment": row["environment"],
            "assignment_group": assignment_group_mapping.get(workload, ""),
            "email": email_mapping.get(workload, ""),
        }

        temp_dict = remove_snow_team(temp_dict, workload)

        return temp_dict

    cudos_list = [dict_format(row) for row in dict_list]
    return cudos_list


def format_budget_data(
    dict_list: list[dict], assignment_group_mapping: dict, email_mapping: dict
) -> dict:
    def dict_format(row: dict):
        workload = split_workload(
            account_id=row["account_id"], workload_env=row["name"]
        )
        workload_type = identify_platform(workload)

        return {
            "name": row["name"],
            "workload": workload,
            "workload_type": workload_type,
            "environment": row["environment"],
            "assignment_group": assignment_group_mapping.get(workload, ""),
            "email": email_mapping.get(workload, ""),
        }

    account_dict = {row["account_id"]: dict_format(row) for row in dict_list}
    return account_dict


def split_workload(account_id: str, workload_env: str) -> str:
    if "-" in workload_env:
        return workload_env.split("-")[0]
    if account_id == workload_env:
        return "Not found"
    return workload_env


def identify_platform(workload: str) -> str:
    if workload.startswith("bsp"):
        workload_type = "bsp"
    elif (
        workload.startswith("dp")
        or "dataplatform" in workload
        or "marketingdata" in workload
        or "hrpoc" in workload
    ):
        workload_type = "dataplatform"
    else:
        workload_type = "NA"
    return workload_type


def augment_missing_data(key: str, data: list[dict]) -> list[dict]:
    # Create a mapping of (workload, environment) to email
    augmentation_mapping = {
        split_workload(account_id=row["account_id"], workload_env=row["name"]): row[key]
        for row in data
        if row[key]
    }
    return augmentation_mapping


def store_hive_json_objects_in_s3(
    s3_client, data: dict, filename: str, bucket: str = finops_bucket
) -> None:
    # Store the results in a string
    output_string = ""
    for item in data:
        output_string += json.dumps(item) + "\n"

    s3_client.put_object(
        Bucket=bucket,
        Key=filename,
        Body=output_string,
        ContentType="text/plain",
    )

    # USED FOR DEBUGGING
    # with open(filename, "w") as f:
    #     f.writelines(output_string)

    logger.info(f"Saved {filename} to {bucket}")


def store_json_in_s3(
    s3_client, data: dict, filename: str, bucket: str = finops_bucket
) -> None:
    json_formatted_string = json.dumps(data, indent=2)

    s3_client.put_object(
        Bucket=bucket,
        Key=filename,
        Body=json_formatted_string,
        ContentType="application/json",
    )

    # USED FOR DEBUGGING
    # with open(filename, "w") as f:
    #     f.writelines(json_formatted_string)

    logger.info(f"Saved {filename} to {bucket}")


def handler(event, context):
    """
    Lambda handler for the FinOps BudgetSetter.
    Triggered on a monthly EventBridge schedule.
    """

    # Making the request
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as response:
        response_data = response.read()
        logger.info("Succesfully requested data from ServiceNow")

    dict_list = decode_snow_data(response_data)

    assignment_group_augmentation = augment_missing_data("assignment_group", dict_list)
    email_augmentation = augment_missing_data("assignment_group.email", dict_list)

    cudos_data = format_cudos_data(
        dict_list, assignment_group_augmentation, email_augmentation
    )
    store_hive_json_objects_in_s3(
        s3_client, cudos_data, cudos_filename, cloudintelligence_bucket
    )

    budget_data = format_budget_data(
        dict_list, assignment_group_augmentation, email_augmentation
    )
    store_json_in_s3(s3_client, budget_data, finops_automation_filename, finops_bucket)

    return {"status": "success"}


if __name__ == "__main__":
    handler({}, {})
