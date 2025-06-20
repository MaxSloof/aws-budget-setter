import boto3
import datetime
import json
from collections import defaultdict
import os
from log_config import logger
from metadata_loader import load_metadata_from_s3, retrieve_metadata_per_workload


# Set Boto3 clients
sts_client = boto3.client("sts")
ce_client = boto3.client("ce")
budgets_client = boto3.client("budgets")
s3_client = boto3.client("s3")

# FinOps email
finops_email = os.getenv("FINOPS_EMAIL", "")
role_arn = os.getenv("ASSUME_ROLE_ARN", "")
account_metadata_filename = os.getenv(
    "ACCOUNT_METADATA_FILENAME", "reference_data/lambda_automation_metadata.json"
)
account_metadata_bucket = os.getenv(
    "ACCOUNT_METADATA_BUCKET", "artifacts-finops-dvb-sbx"
)


def assume_role(role_arn):
    """
    Assumes the specified role and returns temporary credentials.
    """
    try:
        response = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName="FinOpsBudgetSetterSession"
        )
        logger.info(f"Assumed role {role_arn} successfully.")
        return response["Credentials"]
    except Exception as e:
        logger.error(f"Failed to assume role {role_arn}: {e}")
        raise


def initialize_boto3_clients(credentials):
    """
    Initialize Boto3 clients using assumed role credentials.
    """
    assumed_session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    ce_client = assumed_session.client("ce")
    budgets_client = assumed_session.client("budgets")
    return ce_client, budgets_client


def get_all_accounts(ce_client, start_date: str, end_date: str):
    """
    Retrieves all accounts under the centralised organisation.
    """
    response = ce_client.get_dimension_values(
        Dimension="LINKED_ACCOUNT",
        TimePeriod={"Start": start_date, "End": end_date},
        MaxResults=2000,
    )
    accounts = [
        {
            "name": account.get("Attributes", {}).get("description", ""),
            "id": account["Value"],
        }
        for account in response["DimensionValues"]
    ]
    logger.debug("Accounts have been retrieved and stored in a dictionary")
    logger.debug(json.dumps(accounts, sort_keys=True))
    return accounts


def group_accounts_by_workload(accounts):
    """
    Groups accounts by workload based on the naming convention: {workloadname}-{env}.
    If an account name does not follow the naming convention then it is skipped.
    This function uses rsplit so that workloads containing hyphens are still supported.
    """
    workload_accounts = defaultdict(list)
    for account in accounts:
        account_name = account["name"]
        account_id = account["id"]
        # Expect account name to have at least one hyphen
        if "-" in account_name:
            # Split on the last hyphen to separate workload and env, e.g. "ecommerce-prod"
            parts = account_name.rsplit("-", 1)
            if len(parts) == 2:
                workload = parts[0]
                workload_accounts[workload].append(account_id)
            else:
                logger.warning(
                    f"Account {account_name} does not follow the expected naming convention."
                )
        else:
            logger.warning(
                f"Account {account_name} does not follow the expected naming convention."
            )
        logger.debug(
            "Accounts have been grouped based on workload (text before '-' in accountname)"
        )
        logger.debug(json.dumps(workload_accounts, sort_keys=True))
    return workload_accounts


def get_previous_month_date_range():
    """
    Determines the start and end dates for the previous fully completed month.
    For example, if today is 15 November then this returns 1 October to 31 October.
    """
    today = datetime.date.today()
    # Get first day of current month
    first_day_current_month = today.replace(day=1)

    # Last day of previous month = day before first day of current month
    last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)
    return first_day_previous_month.strftime(
        "%Y-%m-%d"
    ), first_day_current_month.strftime("%Y-%m-%d")


def get_cost_for_workload(ce_client, account_ids, start_date, end_date):
    """
    Retrieves the total cost for the provided account IDs for the period between
    start_date and end_date using Cost Explorer. End date is first day of current month!
    """
    cost_metric = "NetAmortizedCost"  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ce/client/get_cost_and_usage.html#:~:text=(string)%20%E2%80%93-,Metrics,-(list)%20%E2%80%93

    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={"Start": start_date, "End": end_date},
            Granularity="MONTHLY",
            Metrics=[cost_metric],
            Filter={"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": account_ids}},
        )
    except Exception as e:
        logger.error(f"Error fetching cost for accounts {account_ids}: {e}")
        return 0.0
    # Parse the returned cost amount (as a string) and convert to float
    try:
        results = response.get("ResultsByTime", [])
        if results:
            total_str = (
                results[0].get("Total", {}).get(cost_metric, {}).get("Amount", "0")
            )
            cost = float(total_str)
            return cost
    except Exception as e:
        logger.error(f"Error parsing cost response: {e}")
    return 0.0


def format_notification(
    budget_percentage: int, workload_email: str | None = None
) -> dict:
    if workload_email and workload_email != finops_email:
        return {
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": float(budget_percentage),
                "ThresholdType": "PERCENTAGE",
                "NotificationState": "ALARM",
            },
            "Subscribers": [
                {"SubscriptionType": "EMAIL", "Address": finops_email},
                {"SubscriptionType": "EMAIL", "Address": workload_email},
            ],
        }
    else:
        return {
            "Notification": {
                "NotificationType": "ACTUAL",
                "ComparisonOperator": "GREATER_THAN",
                "Threshold": float(budget_percentage),
                "ThresholdType": "PERCENTAGE",
                "NotificationState": "ALARM",
            },
            "Subscribers": [
                {"SubscriptionType": "EMAIL", "Address": finops_email},
            ],
        }


def create_or_update_budget(
    budgets_client,
    budget_name,
    budget_amount,
    root_account: str,
    account_ids: list[str],
    workload_email: str | None = None,
):
    """
    Checks if a budget exists (by name) and then creates or updates the cost budget for the workload.
    It also reconfigures notifications at thresholds 95% and 115% with the workload email and a finance team recipient.
    """

    # Construct the budget object. For monthly cost budgets, no explicit time period is given.
    budget_object = {
        "BudgetName": budget_name,
        "BudgetLimit": {"Amount": f"{budget_amount:.2f}", "Unit": "USD"},
        "CostFilters": {"LinkedAccount": account_ids},
        "TimeUnit": "MONTHLY",
        "BudgetType": "COST",
        "CostTypes": {
            "IncludeTax": True,
            "IncludeSubscription": True,
            "UseBlended": False,
            "IncludeRefund": False,
            "IncludeCredit": False,
            "IncludeUpfront": True,
            "IncludeRecurring": True,
            "IncludeOtherSubscription": True,
            "IncludeSupport": True,
            "IncludeDiscount": True,
            "UseAmortized": True,
        },
    }

    notifications_with_subscribers = [
        format_notification(105, workload_email),  # 105% Alert notification
        format_notification(120, workload_email),  # 120% Alert notification
    ]

    logger.debug(notifications_with_subscribers)

    # Check if the budget exists. Using the budgets' describe_budget call.
    exists = False
    try:
        _ = budgets_client.describe_budget(
            AccountId=root_account,  # account id of organization account
            BudgetName=budget_name,
        )
        exists = True
        logger.info(
            f"Budget '{budget_name}' exists. Deleting the current one before replacing it."
        )
    except budgets_client.exceptions.NotFoundException:
        logger.info(f"Budget '{budget_name}' does not exist. Creating it.")
    except Exception as e:
        logger.error(f"Error checking budget '{budget_name}': {e}")

    if exists:
        try:
            budgets_client.delete_budget(AccountId=root_account, BudgetName=budget_name)
            logger.info(f"Deleted budget '{budget_name}'")
        except Exception as e:
            logger.error(f"Failed to delete budget '{budget_name}': {e}")

    try:
        budgets_client.create_budget(
            AccountId=root_account,
            Budget=budget_object,
            NotificationsWithSubscribers=notifications_with_subscribers,
        )
        logger.info(
            f"Created budget '{budget_name}' with limit: {budget_amount:.2f} USD"
        )
    except Exception as e:
        logger.error(f"Failed to create budget '{budget_name}': {e}")


def handler(event, context):
    """
    Lambda handler for the FinOps BudgetSetter.
    Triggered on a monthly EventBridge schedule.
    """
    logger.info("FinOps BudgetSetter Lambda started")

    # Get the caller's account ID (this account is where the budgets are managed)
    root_account = sts_client.get_caller_identity().get("Account")
    logger.info(f"Operating in AWS Account: {root_account}")

    # Assume role in the target account
    # credentials = assume_role(role_arn)

    # Initialize Boto3 clients with assumed role credentials
    # ce_client, budgets_client = initialize_boto3_clients(credentials)

    # Calculate the date range for the fully completed previous month
    start_date, end_date = get_previous_month_date_range()
    logger.info(f"Cost-capture period: {start_date} to {end_date}")

    # Retrieve all accounts managed by the central organisation
    try:
        accounts = get_all_accounts(ce_client, start_date, end_date)
    except Exception as e:
        logger.error(f"Error listing accounts: {e}")
        return {"status": "error", "message": "Could not list accounts."}

    # Group accounts using the naming convention: {workload}-{env}
    workload_accounts = group_accounts_by_workload(accounts)
    if not workload_accounts:
        logger.info("No accounts found that match the naming convention. Exiting.")
        return {"status": "success", "message": "No matching accounts."}

    # Retrieve email of workloads
    metadata_mapping = load_metadata_from_s3(
        s3_client, account_metadata_bucket, account_metadata_filename
    )
    logger.info("Retrieved account metadata")

    # Process each workload group
    for workload, account_ids in workload_accounts.items():
        logger.info(f"Processing workload '{workload}' for account IDs: {account_ids}")

        # Query Cost Explorer for the workload cost during the previous month
        cost = get_cost_for_workload(ce_client, account_ids, start_date, end_date)

        # Minimum budget is 50 USD to avoid email spam
        if cost < 50:
            logger.info("Cost under 50 USD. Setting budget to 50 to avoid email spam")
            cost = 50

        logger.info(f"Workload '{workload}' - Set budget amount: {cost:.2f} USD")

        # Construct a budget name. For example: "Budget-ecommerce"
        budget_name = f"AUTO-workload-{workload}"

        # Get the workload email, defaulting to None if not found
        workload_email = retrieve_metadata_per_workload(
            workload, account_ids, metadata_mapping
        )

        # Create or update the budget and configure its notifications
        create_or_update_budget(
            budgets_client,
            budget_name,
            cost,
            root_account,
            account_ids,
            workload_email,
        )

    logger.info("FinOps BudgetSetter Lambda completed successfully")
    return {"status": "success"}


if __name__ == "__main__":
    handler({}, {})
