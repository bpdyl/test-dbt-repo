"""############################################################
# Script      : utils.py
# Description : This script provides utility functions for the dbt project execution.
#               It handles Azure interactions, environment setup, data validation,
#               and email notifications for the ETL process.
#
# Key Functions:
# - Azure Integration:
#   - get_azure_credentials(): Retrieves Azure credentials
#   - create_blob_service_client(): Creates Azure Blob Storage client
#   - create_secret_client(): Creates Azure Key Vault client
#   - copy_file_to_blob_container(): Copies files to Azure Blob Storage
#   - copy_log_to_archive_container(): Archives log files to Azure Blob Storage
#
# - Environment & Configuration:
#   - prepare_env_vars(): Sets up environment variables from .env and Azure Key Vault
#   - get_business_date(): Retrieves current business date from Snowflake
#
# - Data Validation:
#   - get_stage_files_list(): Lists files in Snowflake stage
#   - get_lnd_tables_list(): Lists available landing tables
#   - validate_selected_stage_data(): Validates required data availability
#   - check_selected_stage_data_availability(): Monitors data availability with notifications
#
# - Email Notifications:
#   - send_email_selected_stage_data_availability(): Sends data availability status emails
#
# Modifications
# 3/28/2025    : Added header comments and improved inline documentation.
############################################################"""

import configparser
import os
import time
from datetime import datetime, date
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError
from azure.core.pipeline.policies import RetryPolicy
from lib.DBTSnowflakeConn import DbtSnowflakeConnectionManager
from lib.Variables import Variables

var = Variables("ENV.cfg")

ARCHIVE_CONTAINER = "archiveblobcontainer"

email_footer = (
    f"<br/><br/>"
    f"*** Please, please, pretty please do not reply to this message because it will end up in deep "
    f"abyss where it will never be read. This is an automated message sent from an unmonitored email "
    f"account that is used for outgoing communication only. ***"
    f"<br/><br/>"
    f"For support or data requests, please visit "
    f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> '
    f"or send a message to "
    f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.'
    f"<br/><br/><br/>"
    f'Best Regards,<br/><strong><label style="color:#00BCD4">'
    f"Team Robling</label></strong>"
    f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'
)


def get_azure_credentials():
    """
    Retrieves Azure credentials using the ClientSecretCredential class.

    This function performs the following steps:
    1. Retrieves the tenant ID, client ID, and client secret from environment variables.
    2. Uses these credentials to create an instance of the ClientSecretCredential class.

    Returns:
        ClientSecretCredential: An instance of the ClientSecretCredential class for authenticating with Azure.

    Raises:
        KeyError: If any required environment variables (e.g., TENANT_ID, CLIENT_ID, CLIENT_SECRET) are not set.

    Note:
        This function relies on the "azure-identity" library to create the ClientSecretCredential instance.
    """
    azure_credential = ClientSecretCredential(
        tenant_id=os.environ["TENANT_ID"],
        client_id=os.environ["CLIENT_ID"],
        client_secret=os.environ["CLIENT_SECRET"],
    )
    return azure_credential


def create_blob_service_client():
    """
    Creates a BlobServiceClient instance to interact with Azure Blob Storage.

    This function performs the following steps:
    1. Retrieves Azure credentials using the get_azure_credentials function.
    2. Attempts to create a BlobServiceClient instance using the provided storage account URL and Azure credentials.
    3. If the required Azure environment variables (e.g., AZURE_BLOB_STORAGE_URL) are not set, it prints an error message and raises a KeyError.

    Returns:
        BlobServiceClient: An instance of the BlobServiceClient class for interacting with Azure Blob Storage.

    Raises:
        KeyError: If any required Azure environment variables (e.g., AZURE_BLOB_STORAGE_URL) are not set.

    Note:
        This function relies on the "azure-identity" and "azure-storage-blob" libraries to interact with Azure Blob Storage.
    """
    azure_credential = get_azure_credentials()
    try:
        azure_blob_account_url = os.environ["AZURE_BLOB_STORAGE_URL"]
        return BlobServiceClient(
            account_url=azure_blob_account_url, credential=azure_credential
        )
    except KeyError as e:
        print("Azure environment variables not set. Exiting.")
        raise e


def create_secret_client():
    """
    Creates a SecretClient instance to interact with Azure Key Vault.

    This function performs the following steps:
    1. Retrieves Azure credentials using the get_azure_credentials function.
    2. Attempts to create a SecretClient instance using the provided vault URL and Azure credentials.
    3. If the required Azure environment variables (e.g., VAULT_URL) are not set, it prints an error message and raises a KeyError.

    Returns:
        SecretClient: An instance of the SecretClient class for interacting with Azure Key Vault.

    Raises:
        KeyError: If any required Azure environment variables (e.g., VAULT_URL) are not set.

    Note:
        This function relies on the "azure-identity" and "azure-keyvault-secrets" libraries to interact with Azure Key Vault.
    """
    azure_credential = get_azure_credentials()
    try:
        policy = RetryPolicy(retry_count=5)
        vault_url = os.environ["VAULT_URL"]
        return SecretClient(vault_url=vault_url, credential=azure_credential, retry_policy=policy)
    except KeyError as e:
        print("Azure environment variables not set. Exiting.")
        raise e


def prepare_env_vars():
    """
    Prepares environment variables by loading them from a .env file and Azure Key Vault.

    This function performs the following steps:
    1. Checks for the existence of a .env file in the root directory specified by the "ROBLING_DBT_DIR" environment variable.
       If the .env file is found, it loads the environment variables from the file, overriding any existing variables.
       If the .env file is not found, it prints a message and skips the loading process.
    2. Creates a SecretClient instance to interact with Azure Key Vault.
    3. Defines a mapping of environment variable names to secret names stored in Azure Key Vault.
    4. Iterates through the mapping and retrieves the secrets from Azure Key Vault.
       If an environment variable is not already set, it sets the variable with the retrieved secret value.

    Raises:
        KeyError: If any required Azure environment variables (e.g., VAULT_URL) are not set.

    Note:
        This function relies on the "dotenv" library to load environment variables from the .env file
        and the "azure-identity" and "azure-keyvault-secrets" libraries to interact with Azure Key Vault.
    """
    # check for .env file in the root directory
    # if not found skip silently
    from dotenv import load_dotenv

    env_path = os.environ.get("ROBLING_DBT_DIR") + "/.env"
    if os.path.exists(env_path):
        load_dotenv(env_path, override=True)
    else:
        print(
            "No .env file found in the root directory. Skipping environment variable loading."
        )
    if var.get("CONFIG_LOCATION") == "2":
        client = create_secret_client()
        var_secret_mappings = {
            "SNOWFLAKE_ACCOUNT": "sfhost",
            "SNOWFLAKE_DATABASE": "sfdatabase",
            "SNOWFLAKE_ROLE": "sfsysadmin",
            "SNOWFLAKE_SCHEMA": "sfschema",
            "SNOWFLAKE_USER": "sfusername",
            "SNOWFLAKE_WAREHOUSE": "sfwarehouse",
            "SNOWFLAKE_PVT_KEY": "sf-private-key",
            "SNOWFLAKE_PASSPHRASE": "sfpassphrase",
        }
        for var_name, secret_name in var_secret_mappings.items():
            try:
                os.environ[var_name] = client.get_secret(secret_name).value
            except ResourceNotFoundError:
                print(f"Secret '{secret_name}' not found in Key Vault. Setting default value to None")
                os.environ[var_name] = None
            except Exception as e:
                raise(f"Error retrieving secret '{secret_name}': {e}")


def copy_file_to_blob_container(
    blob_service_client: BlobServiceClient,
    container_name: str,
    blob_name: str,
    full_path_to_file: str,
):
    if blob_service_client is None:
        blob_service_client = create_blob_service_client()
    if container_name is None:
        raise Exception("Blob Container is Mandatory !")
    if blob_name is None:
        blob_name = os.path.basename(full_path_to_file)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    with open(full_path_to_file, "rb") as data:
        return blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {full_path_to_file} to Azure Blob Storage.")


def copy_log_to_archive_container(
    full_path_to_file: str,
    log_archive_container: str = None,
    blob_directory: str = None,
):
    block_blob_service = create_blob_service_client()
    if log_archive_container is None:
        log_archive_container = ARCHIVE_CONTAINER
    if blob_directory is None:
        blob_directory = "dbt_log_archive/"
    blob_name = (
        blob_directory + str(date.today()) + "/" + os.path.basename(full_path_to_file)
    )
    return copy_file_to_blob_container(
        block_blob_service, log_archive_container, blob_name, full_path_to_file
    )


def get_business_date(database: DbtSnowflakeConnectionManager):
    business_date_sql = """
        SELECT TO_DATE(PARAM_VALUE) FROM DW_DWH.DWH_C_PARAM
        WHERE PARAM_NAME='CURR_DAY'; 
        """
    business_date_result = database.get_data(business_date_sql)
    if business_date_result is not None:
        business_date = business_date_result[0][0]
    else:
        business_date = date.today()
    return business_date


def get_stage_files_list(database: DbtSnowflakeConnectionManager, stage_name: str):
    list_stage_files_sql = (
        "LIST @"
        + var.get("LANDING_DATABASE")
        + "."
        + var.get("LND_DB")
        + "."
        + stage_name
    )
    stage_file_list = database.get_data(list_stage_files_sql)
    stg_file_names = [stg_file[0].split("/")[-1] for stg_file in stage_file_list]
    # print('Available Stage Files :')
    # print(stg_file_names)
    return stg_file_names


def get_lnd_tables_list(database: DbtSnowflakeConnectionManager, curr_day: date):
    list_lnd_tables_sql = (
        f"SELECT TBL_NAME FROM  DW_CFG.CFG_EXT_C_BATCH_LOG "
        f"WHERE BUSINESS_DATE='{curr_day}' "
        f"AND STATUS='COMPLETE' AND TBL_NAME IS NOT NULL;"
    )
    lnd_table_list = database.get_data(list_lnd_tables_sql)
    lnd_tbl_names = [lnd_table[0] for lnd_table in lnd_table_list]
    # print('Available LND Tables :')
    # print(lnd_tbl_names)
    return lnd_tbl_names


def send_email_selected_stage_data_availability(
    curr_date,
    script_dict: dict,
    email_subject,
    required_data_dict: dict,
    available_data,
    missing_data,
    subject_area,
):
    """
    Sends an email notification about the availability status of required stage data files.

    This function generates and sends an HTML-formatted email containing a detailed table
    of required files/tables and their availability status for each script in the batch.

    Args:
        curr_date (date): The current business date for the batch run
        script_dict (dict): Dictionary mapping script names to their load data modes
        email_subject (str, optional): Custom email subject. If None, a default subject is generated
        required_data_dict (dict): Dictionary mapping scripts to their required data files/tables
        available_data (list): List of data files/tables that are currently available
        missing_data (set): Set of data files/tables that are missing
        subject_area (str): The subject area being processed (e.g., 'Sales', 'Organization')

    Example email format:
        Current Batch Date : 2023-12-27

        [Status Message based on missing files]

        | S/No. | Script Name | Snowflake Stage File(s) / LND Table(s) | Availability |
        |-------|-------------|---------------------------------------|--------------|
        | 1     | script1     | file1_20231227.csv                   | ✅           |
        | 2     | script2     | table1 (❌)                          | ❌           |

    Notes:
        - Uses SendGridEmailModule for sending emails
        - Includes standard email footer with support information
        - Marks missing files with ❌ and available files with ✅
        - Recipients are fetched from EXTRACT_STATUS_RECIPIENT environment variable
    """
    from lib.SendGridEmailModule import SendGridEmailModule

    if email_subject is None:
        email_subject = (
            f'INFO :: {var.get("EXEC_ENV")} :: {subject_area} :: {curr_date} : '
            f"Daily Batch Snowflake Stage Files Status"
        )
    email_message = f"<strong>Current Batch Date : {curr_date}</strong>" f"<br/><br/>"

    if missing_data.__len__() > 0:
        email_message = (
            email_message + "Waiting for missing files in Snowflake Stage to continue "
            "with Daily Batch Execution."
        )

    else:
        email_message = (
            email_message
            + "All files needed for executing selected scripts are available in Snowflake Stage."
            " Moving ahead with Daily Batch Execution."
        )

    email_message = (
        email_message + f"<br/><br/>"
        f'<table border="1px">'
        f"<tr>"
        f"<td><strong>S/No.</strong></td>"
        f"<td><strong>Script Name</strong></td>"
        f"<td><strong>Snowflake Stage File(s) / LND Table(s)</strong></td>"
        f"<td><strong>Availability</strong></td>"
        f"</tr>"
    )

    email_tbl_iterator = 0
    for script, script_data in script_dict.items():
        email_tbl_iterator = email_tbl_iterator + 1
        required_data = required_data_dict.get(script)
        email_message = (
            email_message + f"<tr>"
            f'<td style="text-align: right">{email_tbl_iterator}</td>'
            f"<td>{script}</td>"
        )

        if required_data is None:
            email_message = email_message + f"<td>Not Required</td>" + f"<td></td>"

        else:
            # formatted_required_data = (',').join(required_data)
            for table in required_data:
                if (
                    required_data.index(table) == 0
                    and required_data.index(table) == len(required_data) - 1
                ):
                    if table not in available_data:
                        email_message = (
                            email_message + f"<td> {table} ( &#x274C; )</td>"
                        )
                    else:
                        email_message = email_message + f"<td> {table}</td>"
                elif required_data.index(table) == 0:
                    if table not in available_data:
                        email_message = email_message + f"<td> {table} ( &#x274C; ),"
                    else:
                        email_message = email_message + f"<td> {table},"
                elif required_data.index(table) == len(required_data) - 1:
                    if table not in available_data:
                        email_message = email_message + f" {table} ( &#x274C; )</td>"
                    else:
                        email_message = email_message + f" {table}</td>"
                else:
                    if table not in available_data:
                        email_message = email_message + f" {table} ( &#x274C; ),"
                    else:
                        email_message = email_message + f" {table},"

            count_of_table = 0
            for single_table in required_data:
                if single_table in available_data:
                    count_of_table += 1
            if count_of_table == len(required_data):
                email_message = (
                    email_message + f'<td style="text-align: center">&#x2705;</td>'
                )
            else:
                email_message = (
                    email_message + f'<td style="text-align: center">&#x274C;</td>'
                )

        email_message = email_message + f"</tr>"

    email_message = email_message + f"</table>"

    email_message = email_message + email_footer

    SendGridEmailModule.send_email(
        email_subject, email_message, var.get("EXTRACT_STATUS_RECIPIENT").split(",")
    )


def validate_selected_stage_data(
    database: DbtSnowflakeConnectionManager,
    stage_name: str,
    file_ext: str,
    script_dict: dict,
):
    """
    Validates the selected stage data by comparing the required files and tables
    with the available files and tables in the Snowflake stage.

    Args:
        database (DbtSnowflakeConnectionManager): The Snowflake connection manager instance.
        stage_name (str): The name of the Snowflake stage.
        file_ext (str): The file extension to filter the files in the stage.
        script_dict (dict): A dictionary containing script names and their corresponding load data flags.

    Returns:
        tuple: A tuple containing the following elements:
            - required_data_dict (dict): A dictionary of required data files and tables.
            - available_data (list): A list of available data files and tables in the stage.
            - missing_data (set): A set of data files and tables that are missing.
            - missing_iteration_call_order (dict): A dictionary of missing data files and tables with their iteration call order.
    """
    # Get current business date and format it for file naming
    curr_date = get_business_date(database)
    curr_date_string = curr_date.strftime("%Y%m%d")

    # Load configuration files that map scripts to their required input files and tables
    # File configuration contains mappings for file-based dependencies
    file_config = configparser.ConfigParser()
    file_config.read(os.environ.get("ROBLING_DBT_DIR") + "/config/script_data_file.cfg")
    file_config_section = "script-file-map"
    reference_files_dict = dict(file_config.items(file_config_section))

    # Table configuration contains mappings for table-based dependencies
    table_config = configparser.ConfigParser()
    table_config.read(
        os.environ.get("ROBLING_DBT_DIR") + "/config/script_data_table.cfg"
    )
    table_config_section = "script-table-map"
    reference_table_dict = dict(table_config.items(table_config_section))

    # Load iteration order configuration for both files and tables
    # This determines the sequence in which dependencies should be processed
    iteration_config_section = "script-iteration-map"
    script_iteration_list = [
        dict(file_config.items(iteration_config_section)),
        dict(table_config.items(iteration_config_section)),
    ]

    # Extract list of required files based on script configuration
    # load_data_from values 2 and 4 indicate file-based data sources
    script_data_file_list = [
        file_config[file_config_section][script]
        for (script, load_data_from) in script_dict.items()
        if file_config.has_option(file_config_section, script)
        and load_data_from in (2, 4)
    ]

    # Extract list of required tables based on script configuration
    # load_data_from value 3 indicates table-based data source
    script_data_table_list = [
        table_config[table_config_section][script]
        for (script, load_data_from) in script_dict.items()
        if table_config.has_option(table_config_section, script) and load_data_from == 3
    ]

    # Handle multiple table dependencies (tables separated by '|' in config)
    all_script_data_table_list = []
    for tables in script_data_table_list:
        if "|" in tables:
            all_script_data_table_list.append(tables.split("|"))
        else:
            all_script_data_table_list.append(tables)

    # Flatten the list of required tables
    splitted_script_data_table_list = []
    for sublist in all_script_data_table_list:
        if type(sublist) == list:
            for item in sublist:
                splitted_script_data_table_list.append(item)
        else:
            splitted_script_data_table_list.append(sublist)

    # Create dictionary mapping scripts to their required tables
    multiple_table_dict = dict()
    for script, table_name in reference_table_dict.items():
        table_list = table_name.split("|")
        multiple_table_dict.update(dict([(script, table_list)]))

    # Initialize dictionaries to track required data
    required_data_dict = dict()
    required_files_dict = dict()
    required_tables_dict = dict()

    # Build dictionary of required files with proper naming convention
    if script_data_file_list:
        required_files_dict = dict(
            [
                (script, ref_file + "_" + curr_date_string + "." + file_ext)
                for (script, ref_file) in reference_files_dict.items()
                if ref_file in script_data_file_list
            ]
        )
        required_data_dict.update(required_files_dict)

    # Build dictionary of required tables
    # Only include tables where all dependencies are required
    if splitted_script_data_table_list:
        for script, table_name in multiple_table_dict.items():
            required_tables_count = 0
            for table in table_name:
                if table in splitted_script_data_table_list:
                    required_tables_count += 1
            if required_tables_count == len(table_name):
                required_tables_dict.update(dict([(script, table_name)]))

        required_data_dict.update(required_tables_dict)

    # Flatten the list of all required data items
    required_data_list = list(required_data_dict.values())
    required_data = []
    for table_list in required_data_list:
        for item in table_list:
            required_data.append(item)

    # Build dictionary of iteration order for required data
    if script_iteration_list:
        iteration_call_order = {}
        iteration_order = dict(
            [
                (script, call_iteration_order)
                for iteration_dictionary in script_iteration_list
                for (script, call_iteration_order) in iteration_dictionary.items()
                if script in script_dict.keys()
            ]
        )
        if iteration_order:
            for script in iteration_order.keys():
                if script in required_data_dict.keys():
                    iteration_call_order[tuple(required_data_dict[script])] = (
                        iteration_order[script]
                    )
    else:
        iteration_call_order = {}

    # Get lists of available files and tables from Snowflake
    all_stage_files = get_stage_files_list(database, stage_name)
    all_lnd_tables = get_lnd_tables_list(database, curr_date)

    # Filter available files to only those that are required
    available_files = [
        file for file in all_stage_files if file in required_files_dict.values()
    ]

    # Flatten the list of required tables and filter available tables
    required_tables_values_list = list(required_tables_dict.values())
    required_tables_list = []
    for table_list in required_tables_values_list:
        for item in table_list:
            required_tables_list.append(item)
    available_tables = [
        table for table in all_lnd_tables if table in required_tables_list
    ]

    # Combine available files and tables, then determine missing items
    available_data = available_files + available_tables
    missing_data = set(required_data) - set(available_data)

    # Create dictionary of missing items with their iteration order
    if iteration_call_order:
        missing_iteration_call_order = {
            table: call_order
            for (table, call_order) in iteration_call_order.items()
            for tbl in table
            if tbl in missing_data
        }
    else:
        missing_iteration_call_order = {}

    # Return all relevant information about required and available data
    return [
        required_data_dict,
        available_data,
        missing_data,
        missing_iteration_call_order,
    ]


def check_selected_stage_data_availability(
    database: DbtSnowflakeConnectionManager,
    stage_name,
    file_ext,
    script_dict: dict,
    poll_time,
    email_trigger_time,
    max_wait_time,
    subject_area,
):
    """
    Monitors the availability of required stage data files and tables in Snowflake.

    This function continuously checks the availability of required data files and tables
    in the specified Snowflake stage. It sends email notifications about the availability
    status at regular intervals and raises an alert if the required data is not available
    within the maximum wait time.

    Args:
        database (DbtSnowflakeConnectionManager): The Snowflake connection manager instance.
        stage_name (str): The name of the Snowflake stage.
        file_ext (str): The file extension to filter the files in the stage.
        script_dict (dict): A dictionary containing script names and their corresponding load data flags.
        poll_time (int): The interval (in seconds) between consecutive checks for data availability.
        email_trigger_time (float): The time (in minutes) after which an email notification is triggered if data is still missing.
        max_wait_time (float): The maximum time (in minutes) to wait for the required data before raising an alert.
        subject_area (str): The subject area being processed (e.g., 'Sales', 'Organization').

    Raises:
        RuntimeError: If the email trigger time is greater than the maximum wait time.

    Returns:
        None
    """
    # Check if stage exists in Snowflake
    stage_exists_sql = f"""
        SELECT COUNT(*) FROM {var.get("LANDING_DATABASE")}.INFORMATION_SCHEMA.STAGES
        WHERE stage_name = '{stage_name.upper()}'
    """
    try:
        stage_exists_result = database.get_data(stage_exists_sql)
        if not stage_exists_result or stage_exists_result[0][0] == 0:
            print(
                f"Stage '{stage_name}' not found in Snowflake. Skipping data availability check for subject area '{subject_area}'."
            )
            return
    except Exception as e:
        print(
            f"Error checking stage existence for '{stage_name}': {e}. Skipping data availability check."
        )
        return

    # Get current business date from Snowflake
    curr_date = get_business_date(database)

    # Convert time parameters to appropriate units
    poll_time = int(poll_time)  # Time between checks in seconds
    email_trigger_time = float(
        email_trigger_time
    )  # Time before sending notification emails in minutes
    max_wait_time = float(max_wait_time)  # Maximum time to wait for files in minutes

    # Validate that email trigger time is less than max wait time
    if email_trigger_time > max_wait_time:
        raise RuntimeError("Email Trigger Time cannot be greater than Max Wait Time.")

    # Initialize monitoring control variables
    circuit_breaker = False  # Controls the monitoring loop
    first_iteration = True  # Tracks first run for initial notification
    delay_iteration = 0  # Counts number of delay iterations

    # Convert time thresholds to seconds for comparison
    email_trigger_time_sec = email_trigger_time * 60
    max_wait_time_sec = max_wait_time * 60

    # Initialize timing trackers
    start = time.time()  # Start time of monitoring
    email_trigger_start = start  # Last email notification time

    # Main monitoring loop
    while not circuit_breaker:
        # Check current status of required files and tables
        validate_files_output = validate_selected_stage_data(
            database, stage_name, file_ext, script_dict
        )
        # Unpack validation results
        required_data_dict = dict(validate_files_output[0])  # Required files/tables
        available_data = validate_files_output[1]  # Available files/tables
        missing_data = validate_files_output[2]  # Missing files/tables
        missing_iteration_call_order = validate_files_output[
            3
        ]  # Order of missing items

        # Send initial status email on first iteration
        if first_iteration:
            send_email_selected_stage_data_availability(
                curr_date,
                script_dict,
                None,  # Use default subject for initial email
                required_data_dict,
                available_data,
                missing_data,
                subject_area,
            )

        # Check if all required data is available
        if missing_data.__len__() == 0:
            circuit_breaker = True  # Exit monitoring loop

            # Send completion notification if not first check
            if not first_iteration:
                email_subject = (
                    f'INFO :: {var.get("EXEC_ENV")} :: {subject_area} :: {curr_date} : '
                    f"Snowflake Stage All Files Available"
                )
                send_email_selected_stage_data_availability(
                    curr_date,
                    script_dict,
                    email_subject,
                    required_data_dict,
                    available_data,
                    missing_data,
                    subject_area,
                )
        else:
            # Calculate elapsed times
            time_taken = time.time() - start
            email_trigger_period = time.time() - email_trigger_start

            # Check if it's time to send another status email
            if email_trigger_period > email_trigger_time_sec:
                email_trigger_start = time.time()  # Reset email timer

                # Send status email about missing files
                email_subject = (
                    f'INFO :: {var.get("EXEC_ENV")} :: {subject_area} :: {curr_date} : '
                    f"{missing_data.__len__()}"
                    f" File(s) missing in Snowflake Stage"
                )
                send_email_selected_stage_data_availability(
                    curr_date,
                    script_dict,
                    email_subject,
                    required_data_dict,
                    available_data,
                    missing_data,
                    subject_area,
                )

                # Check if any missing files are past their iteration order
                if missing_iteration_call_order:
                    if any(
                        int(call_order) <= delay_iteration
                        for call_order in missing_iteration_call_order.values()
                    ):
                        # batch_execution_delay_call(database)
                        print("Delaying Batch Execution")

            # Check if maximum wait time has been exceeded
            if time_taken > max_wait_time_sec:
                # Send failure notification
                email_subject = (
                    f'FAILURE :: {var.get("EXEC_ENV")} :: {subject_area} :: {curr_date} : '
                    f"Daily Batch Terminated "
                    f"after Max Wait Time of {max_wait_time} min exceeded."
                )
                send_email_selected_stage_data_availability(
                    curr_date,
                    script_dict,
                    email_subject,
                    required_data_dict,
                    available_data,
                    missing_data,
                    subject_area,
                )
                # batch_execution_delay_call(database)
                raise RuntimeError(
                    "Daily Batch terminated after Max Wait Time exceeded."
                )

            # Increment delay counter and wait before next check
            delay_iteration = delay_iteration + 1
            time.sleep(poll_time)

        first_iteration = False  # Mark completion of first iteration


def batch_execution_failure_email(
    database: DbtSnowflakeConnectionManager,
    log_file,
    ex_str,
    failure_email_recipient_list=[],
):
    """
    Sends a failure notification email when a batch execution fails.

    This function composes and sends an HTML-formatted email to notify stakeholders about
    a failed pipeline execution. If a log file is provided, it is attached to the email
    to help with debugging and root cause analysis.

    Args:
        database (DbtSnowflakeConnectionManager):
            Active Snowflake database connection used to fetch the current business date.
        log_file (str or None):
            Absolute path to the log file generated during the failed batch execution.
            If None, the email is sent without attachment.
        ex_str (str):
            A descriptive error message or exception string that identifies the reason
            for the failure. This message is included in the email body.
        failure_email_recipient_list (list, optional):
            List of email addresses to notify. If not provided, the list is fetched
            from the environment variable `FAILURE_EMAIL_RECIPIENT`.
    """
    from lib.SendGridEmailModule import SendGridEmailModule

    if not failure_email_recipient_list:
        failure_email_recipient_list = var.get("FAILURE_EMAIL_RECIPIENT").split(",")

    curr_date = get_business_date(database)
    email_subject = f'FAILURE :: {var.get("EXEC_ENV")} : RoblingDaaS - Batch Execution Failed : {curr_date}'
    email_message = (
        f"Current Batch Date : <strong>{curr_date}</strong>"
        f"<br/>"
        f"<br/>Batch Status : <strong>FAILURE</strong>"
        f"<br/>"
    )

    if log_file:
        email_message = (
            email_message + f"<br/>Exception Message : Error executing {ex_str} script."
            f" Please find attached herewith the log for the same." + email_footer
        )
        SendGridEmailModule.send_email_with_attachment(
            email_subject,
            email_message,
            log_file,
            failure_email_recipient_list,
        )
    else:
        email_message = (
            email_message + f"<br/>Exception Message : {ex_str}" + email_footer
        )
        SendGridEmailModule.send_email(
            email_subject, email_message, failure_email_recipient_list
        )
