import boto3
import datetime
import logging
import json
from collections import defaultdict

# Configure logging 
logger = logging.getLogger()
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s',
    level=logging.INFO
)

def get_all_accounts(start_date:str, end_date:str):
    """
    Retrieves all accounts under the centralised organisation.
    """
    # Create an IAM client
    ce_client = boto3.client('ce')
    
    response = ce_client.get_dimension_values(
        Dimension='LINKED_ACCOUNT', 
        TimePeriod={
            "Start": start_date,
            "End": end_date
        },
        MaxResults=2000,
    )   
    accounts = [{"name":account["Attributes"]["description"], "id": account["Value"]} for account in response["DimensionValues"]]
    logger.debug("Accounts have been retrieved and stored in a dictionary")
    logger.debug(json.dumps(accounts,sort_keys=True))
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
        if '-' in account_name:
            # Split on the last hyphen to separate workload and env, e.g. "ecommerce-prod"
            parts = account_name.rsplit('-', 1)
            if len(parts) == 2:
                workload = parts[0]
                workload_accounts[workload].append(account_id)
            else:
                logger.warning(f"Account {account_name} does not follow the expected naming convention.")
        else:
            logger.warning(f"Account {account_name} does not follow the expected naming convention.")
        logger.debug("Accounts have been grouped based on workload (text before '-' in accountname)")
        logger.debug(json.dumps(workload_accounts,sort_keys=True))
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
    return first_day_previous_month.strftime('%Y-%m-%d'), first_day_current_month.strftime('%Y-%m-%d')


def get_cost_for_workload(account_ids, start_date, end_date):
    """
    Retrieves the total cost for the provided account IDs for the period between
    start_date and end_date using Cost Explorer. End date is first day of current month!
    """
    cost_metric = "NetAmortizedCost" # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ce/client/get_cost_and_usage.html#:~:text=(string)%20%E2%80%93-,Metrics,-(list)%20%E2%80%93

    ce_client = boto3.client('ce')
    try:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='MONTHLY',
            Metrics=[cost_metric],
            Filter={
                "Dimensions": {
                    "Key": "LINKED_ACCOUNT",
                    "Values": account_ids
                }
            }
        )
    except Exception as e:
        logger.error(f"Error fetching cost for accounts {account_ids}: {e}")
        return 0.0
    # Parse the returned cost amount (as a string) and convert to float
    try:
        results = response.get('ResultsByTime', [])
        if results: 
            total_str = results[0].get('Total', {}).get(cost_metric, {}).get('Amount', '0')
            cost = float(total_str)
            return cost
    except Exception as e:
        logger.error(f"Error parsing cost response: {e}")
    return 0.0


def create_or_update_budget(budget_name, budget_amount, root_account:str, account_ids:list[str], workload_email:str|None=None):
    """
    Checks if a budget exists (by name) and then creates or updates the cost budget for the workload.
    It also reconfigures notifications at thresholds 80% and 100% with the workload email and a finance team recipient.
    """
    budgets_client = boto3.client('budgets')

    # Construct the budget object. For monthly cost budgets, no explicit time period is given.
    budget_object = {
        'BudgetName': budget_name,
        'BudgetLimit': {
            'Amount': f"{budget_amount:.2f}",
            'Unit': 'USD'
        },
        'CostFilters': {
            'LinkedAccount': account_ids
        },
        'TimeUnit': 'MONTHLY',
        'BudgetType': 'COST',
        'CostTypes': {
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
            "UseAmortized": True
        },
    }

    # Check if the budget exists. Using the budgets' describe_budget call.
    exists = False
    try:
        _ = budgets_client.describe_budget(
            AccountId=root_account, # account id of organization account
            BudgetName=budget_name
        )
        exists = True
        logger.info(f"Budget '{budget_name}' exists. Updating it.")
    except budgets_client.exceptions.NotFoundException:
        logger.info(f"Budget '{budget_name}' does not exist. Creating it.")
    except Exception as e:
        logger.error(f"Error checking budget '{budget_name}': {e}")

    if exists:
        try:
            budgets_client.update_budget(AccountId=root_account, NewBudget=budget_object)
            logger.info(f"Updated budget '{budget_name}' with new limit: {budget_amount:.2f} USD")
        except Exception as e:
            logger.error(f"Failed to update budget '{budget_name}': {e}")
    else:
        try:
            budgets_client.create_budget(AccountId=root_account, Budget=budget_object)
            logger.info(f"Created budget '{budget_name}' with limit: {budget_amount:.2f} USD")
        except Exception as e:
            logger.error(f"Failed to create budget '{budget_name}': {e}")

    # Set up notifications at 80% and 100%
    for threshold in [80, 100]:
        notification = {
            'NotificationType': 'ACTUAL',
            'ComparisonOperator': 'GREATER_THAN',
            'Threshold': float(threshold),
            'ThresholdType': 'PERCENTAGE'
        }
        # The subscriber list includes the workload’s contact (hardcoded here) and the finance email
        subscribers = [
            # {
            #     'SubscriptionType': 'EMAIL',
            #     'Address': workload_email
            # },
            {
                'SubscriptionType': 'EMAIL',
                'Address': 'finops@asnbank.nl'
            }
        ]
        # Delete any existing notification for this threshold before re-creating it
        try:
            budgets_client.delete_notification(
                AccountId=root_account,
                BudgetName=budget_name,
                Notification=notification,
            )
            logger.info(f"Deleted existing notification for threshold {threshold}% in budget '{budget_name}'")
        except budgets_client.exceptions.NotFoundException:
            logger.info(f"No existing notification for threshold {threshold}% in budget '{budget_name}'")
        except Exception as e:
            logger.warning(f"Exception deleting notification for threshold {threshold}%: {e}")

        try:
            budgets_client.create_notification(
                AccountId=root_account,
                BudgetName=budget_name,
                Notification=notification,
                Subscribers=subscribers
            )
            logger.info(f"Created notification for threshold {threshold}% in budget '{budget_name}'")
        except Exception as e:
            logger.error(f"Failed to create notification for threshold {threshold}% in budget '{budget_name}': {e}")


def lambda_handler(event, context):
    """
    Lambda handler for the FinOps BudgetSetter.
    Triggered on a monthly EventBridge schedule.
    """
    logger.info("FinOps BudgetSetter Lambda started")

    # Get the caller's account ID (this account is where the budgets are managed)
    sts_client = boto3.client('sts')
    root_account = sts_client.get_caller_identity().get('Account')
    logger.info(f"Operating in AWS Account: {root_account}")

    # Calculate the date range for the fully completed previous month
    start_date, end_date = get_previous_month_date_range()
    logger.info(f"Cost-capture period: {start_date} to {end_date}")

    # Retrieve all accounts managed by the central organisation
    try:
        accounts = get_all_accounts(start_date, end_date)
    except Exception as e:
        logger.error(f"Error listing accounts: {e}")
        return {"status": "error", "message": "Could not list accounts."}

    # Group accounts using the naming convention: {workload}-{env}
    workload_accounts = group_accounts_by_workload(accounts)
    if not workload_accounts:
        logger.info("No accounts found that match the naming convention. Exiting.")
        return {"status": "success", "message": "No matching accounts."}

    # Process each workload group
    for workload, account_ids in workload_accounts.items():
        logger.info(f"Processing workload '{workload}' for account IDs: {account_ids}")

        # Query Cost Explorer for the workload cost during the previous month
        cost = get_cost_for_workload(account_ids, start_date, end_date)
        logger.info(f"Workload '{workload}' - Previous month cost: {cost:.2f} USD")

        # Calculate the budget amount by increasing last month's cost by 15%
        budget_amount = cost * 1.15
        logger.info(f"Workload '{workload}' - Set budget amount: {budget_amount:.2f} USD")

        # Construct a budget name. For example: "Budget-ecommerce"
        budget_name = f"AUTO-workload-{workload}"

        # Determine the email for alerting. This can be a mapping, or using a default pattern.
        # Here we use a default pattern (e.g. workload name in lowercase plus a fixed domain)
        # workload_email = f"{workload.lower()}@example.com"  # Modify as required.

        # logger.info(f"Alert notifications will be sent to: {workload_email} and finops@asnbank.nl")

        # Create or update the budget and configure its notifications
        create_or_update_budget(budget_name, budget_amount, root_account, account_ids)
        break

    logger.info("FinOps BudgetSetter Lambda completed successfully")
    return {"status": "success"}

if __name__ == "__main__":
    lambda_handler({},{})