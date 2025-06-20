from log_config import logger
import json


import boto3

s3_client = boto3.client("s3")

ignore_emails_for_workloads = ["finopsmanagement", "cloudintelligencedashboard"]


def load_metadata_from_s3(s3_client, bucket, filename):
    try:
        # Fetch the CSV file from S3
        response = s3_client.get_object(Bucket=bucket, Key=filename)

        logger.info("Workload email data read from S3")
    except Exception as e:
        logger.warning(f"Issue retrieving workload mapping from S3: {e}")
        return {}

    try:
        # Read the content of the object
        json_data = response["Body"].read().decode("utf-8")

        # Convert the JSON string back to a dictionary
        metadata_mapping = json.loads(json_data)
        return metadata_mapping

    except Exception as e:
        logger.warning(f"Issue decoding workload mapping file from S3: {e}")
        return {}


def retrieve_metadata_per_workload(
    workload: str, account_ids: list[str], metadata: dict
) -> str:
    """
    Looks up emails based on account IDs provided in a list.

    Parameters:
    - account_ids (list[str]): A list of account IDs to look up.
    - email_dict (dict): A dictionary mapping account IDs to emails.

    Returns:
    - dict: A mapping of account IDs to their corresponding emails.
    """
    if workload in ignore_emails_for_workloads:
        logger.info(
            f"Workload ({workload}) is owned by FinOps team. Ignoring SNOW address."
        )
        return None
    if metadata is None:
        logger.warning("Metadata not found. Is object succesfully retrieved?")
        return None
    if workload == "Not found":
        return None

    email = None
    for account_id in account_ids:
        email = metadata.get(account_id, {}).get("email", None)

        if email is not None:
            logger.debug(f"Email found for: {account_id} - email: {email}")
            return email

    if email == None:
        logger.info(f"No email found for workload: {workload}")

    return email
