"""############################################################
# Script      : dbt_runner.py
# Description : This script orchestrates the execution of dbt models in a scheduled batch run.
#               It reads the execution sequence from a config file, filters the relevant jobs,
#               and executes the corresponding dbt commands (test and run). Additionally,
#               it captures reconciliation results and sends email notifications.
#
# Key Functions:
# - Reads job configurations from execution_sequence.cfg
# - Filters jobs based on execution arguments
# - Executes dbt test and run commands
# - Captures reconciliation results and sends email notifications if run via cloud (config_location = 2)
# - Handles logging and error reporting
#
# Modifications
# 3/28/2025    : Added header comments and improved inline documentation.
############################################################"""

import configparser
import datetime
import json
import os
import sys
import subprocess
from dbt.cli.main import dbtRunner
from dbt_common.events.base_types import EventMsg
import pandas as pd

from lib.Formatter import Formatter
from lib.IntegrityCheckEmail import IntegrityCheckEmail
from lib.ReconciliationEmail import ReconciliationEmail
from lib.utils import (
    copy_log_to_archive_container,
    prepare_env_vars,
    check_selected_stage_data_availability,
    batch_execution_failure_email
)
from lib.Variables import Variables
from lib.logging_config import DbtLogHandler
from lib.DBTSnowflakeConn import DbtSnowflakeConnectionManager

# Directory for storing logs
LOG_DIR = "logs"
CONFIG_FILE = "execution_sequence.cfg"
os.makedirs(LOG_DIR, exist_ok=True)

# setup variable values from ENV.cfg
var = Variables("ENV.cfg")


def reconciliation_callback(event: EventMsg):
    """A callback function to register to dbtRunner for Handling reconciliation results from dbt events"""
    if "RECON_JSON_RESULT:" in str(event.info):
        json_str = event.info.msg.replace("RECON_JSON_RESULT: ", "").strip()
        json_result = json.loads(json_str)
        # Extract source system names
        system1 = json_result["source_systems"]["system1"]  # LND
        system2 = json_result["source_systems"]["system2"]  # STG_V

        # Define the exact column order
        column_order = [
            f"{system1}_UNT",
            f"{system2}_UNT",
            "DIFF_UNT",
            "VAR_PERCENT_UNT",
            f"{system1}_CST",
            f"{system2}_CST",
            "DIFF_CST",
            "VAR_PERCENT_CST",
            f"{system1}_RTL",
            f"{system2}_RTL",
            "DIFF_RTL",
            "VAR_PERCENT_RTL",
        ]

        # Process results into a structured format
        data = {}
        for metric, values in json_result["results"].items():
            data[f"{system1}_{metric.upper()}"] = [values["system1_value"]]
            data[f"{system2}_{metric.upper()}"] = [values["system2_value"]]
            data[f"DIFF_{metric.upper()}"] = [values["difference"]]
            data[f"VAR_PERCENT_{metric.upper()}"] = [values["variance_percent"]]

        # Convert dictionary to DataFrame and enforce column order
        df = pd.DataFrame(data, columns=column_order)
        # Using the formatter library to format the result we parsed so that we can send the pretty formatted email
        formatted_json = Formatter().format_reconciliation_json(json_result)
        ReconciliationEmail(var).send_reconciliation_email(formatted_json, df)


def integrity_check_callback(event: EventMsg):
    """Callback for handling integrity check results and sending email."""
    if "INTEGRITY_JSON_RESULT:" in str(event.info):
        json_str = event.info.msg.replace("INTEGRITY_JSON_RESULT: ", "").strip()
        integrity_json = json.loads(json_str)
        IntegrityCheckEmail(var).send_integrity_email(integrity_json)

def create_log_callback(log_handler: DbtLogHandler):
    """Factory function that creates a callback with log_handler captured in closure"""
    def log_callback(event: EventMsg):
        """Callback function to Log dbt events to the log file via DbtLogHandler"""
        log_handler.log_event(event)
    return log_callback


def run_dbt_commands(
    cwd, tag_name, staging_view_name=None, module_name=None, load_type=None
):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # Create log file path
    file_name = f"{tag_name}_{timestamp}.log"
    log_file = os.path.join(LOG_DIR, file_name)
    integrity_log_file = None # log file path will be updated later in integrity section below
    # Create log handler
    log_handler = DbtLogHandler(log_file)

    # Create log callback for main log file of ETL
    main_log_callback = create_log_callback(log_handler)

    # callbacks to register to dbtRunner based on the CONFIG_LOCATION
    # we don't want to send reconciliation email when we run the script from local environment
    main_callbacks = (
        [main_log_callback, reconciliation_callback]
        if var.get("CONFIG_LOCATION") == "2"
        else [main_log_callback]
    )

    try:
        # Run dbt test (only if a staging view is provided)
        if staging_view_name:
            stg_run_command = [
                "-d",
                "--log-format=debug",
                "run",
                "--vars",
                f'{{"module_name": "{module_name}","load_type": "{load_type}","log_file_name":"{file_name}"}}',
                "--profiles-dir",
                cwd,
                "--select",
                staging_view_name,
            ]
            stg_test_command = [
                "-d",
                "--log-format=debug",
                "test",
                "--profiles-dir",
                cwd,
                "--select",
                staging_view_name,
            ]

            # Run dbt test command with logging and reconciliation callbacks to validate staging view
            # The callbacks will:
            # 1. Log all dbt events to the log file via DbtLogHandler
            # 2. Monitor for reconciliation results and trigger email notifications via reconciliation_callback
            stg_run_result = dbtRunner(callbacks=main_callbacks).invoke(stg_run_command)
            test_result = dbtRunner(callbacks=main_callbacks).invoke(stg_test_command)

            if not (stg_run_result.success and test_result.success):
                # BASE ERROR MESSAGE CONSTRUCTION
                error_msg = f"Operation failed for {staging_view_name}: \
                        {stg_run_result.exception if not stg_run_result.success else test_result.exception}"
                # EXCEPTION HANDLING PRIORITY:
                # 1. Check for direct exceptions first (critical failures)
                if stg_run_result.exception or test_result.exception:
                    # Combine exceptions from both operations, preferring run exceptions first
                    error_msg += f": {str(stg_run_result.exception) if stg_run_result.exception else str(test_result.exception)}"

                # RESULT-BASED ERROR HANDLING:
                # 2. If no exceptions, collect detailed error messages from both operations
                elif stg_run_result.result or test_result.result:
                    res_msgs = []
                    # PROCESS RUN ERRORS:
                    # - Extract messages from failed model runs
                    if stg_run_result.result:
                        # Get human-readable messages from each failed node
                        run_res_msgs = [
                            res.message
                            for res in stg_run_result.result.results
                            if res.message
                        ]
                        res_msgs.extend(run_res_msgs)

                    # PROCESS TEST ERRORS:
                    # - Extract messages from failed test assertions
                    if test_result.result:
                        # Get test failure descriptions from validation results
                        test_res_msgs = [
                            res.message
                            for res in test_result.result.results
                            if res.message
                        ]
                        res_msgs.extend(test_res_msgs)

                    # FORMAT COMBINED ERRORS:
                    # - Join all messages with line breaks for readability
                    if res_msgs:
                        error_msg += f": {', '.join(res_msgs)}"

                # FINAL ERROR PROPAGATION:
                # - Raise combined error with full context from both operations
                # - Includes either exception stacktrace OR business-level error messages
                raise RuntimeError(error_msg)
        # Run dbt run command excluding the staging view model as it has already run from above command
        # Form the exclude command for the staging view
        exclude_stg_view_cmd = ['--exclude',staging_view_name]
        dbt_run_command = [
            "-d",
            "--log-format=debug",
            "run",
            "--vars",
            f'{{"module_name": "{module_name}","load_type": "{load_type}","log_file_name":"{file_name}"}}',
            "--profiles-dir",
            cwd,
            "--select",
            f"tag:{tag_name}"
        ]
        # Add exclude command only if staging view is provided
        # This is to avoid dbt run command failing if staging view is not provided (in case of datamart scripts)
        if staging_view_name:
            dbt_run_command.extend(exclude_stg_view_cmd)
        run_result = dbtRunner(callbacks=main_callbacks).invoke(dbt_run_command)

        if not run_result.success:
            error_msg = f"Run failed for script {tag_name}"
            if run_result.exception:
                error_msg += f": {str(run_result.exception)}"
            elif run_result.result:
                # Get all error messages from result object
                res_msgs = [
                    res.message for res in run_result.result.results if res.message
                ]
                error_msg += f": {str(res_msgs)}"
            raise RuntimeError(error_msg)
        log_handler.logger.info(
            f"Successfully executed dbt commands for tag: {tag_name}"
        )
        # Run Data Integrity Checks for the DWH models/sources
        # This command runs after the main models are loaded.
        # If the tag name does not start with 'dm_', we run the model test command
        # This is to avoid running model test command for datamart scripts as they are not
        # expected to have model tests associated with them
        if 'dm_' not in tag_name:
            # Moving integrity handler after the dbt run has been completed for the tag/script name
            # because placing the below code block in initial stage creates two log files and write same events in both files
            # Integrity checks will be written to a separate log file
            # so creating different handler and callback for integrity check
            integrity_log_file = os.path.join(LOG_DIR, f"{tag_name}_integrity_chk_{timestamp}.log")
            integrity_log_handler = DbtLogHandler(integrity_log_file)
            integrity_log_callback = create_log_callback(integrity_log_handler)

            # Separate callbacks for integrity checks (using different log handler)
            # callbacks to register to dbtRunner based on the CONFIG_LOCATION
            # we don't want to send integrity email when we run the script from local environment
            integrity_callbacks = (
                [integrity_log_callback, integrity_check_callback]
                if var.get("CONFIG_LOCATION") == "2"
                else [integrity_log_callback,integrity_check_callback]
            )

            integrity_log_handler.logger.info(f"Running Data Integrity Checks for tag: {tag_name}")
            dbt_model_test_command = [
                "-d", "--log-format=debug", "test",
                "--profiles-dir", cwd,
                "--select", f"tag:{tag_name}",
            ]
            # Exclude staging view tests as they have already run
            if staging_view_name:
                dbt_model_test_command.extend(["--exclude", staging_view_name])
            model_test_result = dbtRunner(callbacks=integrity_callbacks).invoke(
                dbt_model_test_command
            )
            if not model_test_result.success:
                error_msg = f"Data Integrity Checks failed for script {tag_name}"
                if model_test_result.exception:
                    error_msg += f": {str(model_test_result.exception)}"
                elif model_test_result.result:
                    # Get all error messages from result object
                    res_msgs = [
                        res.message for res in model_test_result.result.results if res.message
                    ]
                    error_msg += f": {str(res_msgs)}"
                raise RuntimeError(error_msg)
            integrity_log_handler.logger.info(
                f"Successfully executed integrity check commands for tag: {tag_name}"
            )
        # Only copy logs to azure storage if run via cloud
        if var.get("CONFIG_LOCATION") == "2":
            copy_log_to_archive_container(log_file)
            if integrity_log_file and os.path.exists(integrity_log_file):
                copy_log_to_archive_container(integrity_log_file)

    except Exception as e:
        error_msg = f"Unexpected error occurred while running dbt commands: {str(e)}"
        log_handler.logger.error(error_msg)
        # Only copy logs to azure storage if run via cloud
        if var.get("CONFIG_LOCATION") == "2":
            files_to_attach = []
            if log_file and os.path.exists(log_file):
                copy_log_to_archive_container(log_file)
                files_to_attach.append(log_file)
            # If integrity check's log file exists then copy it to the azure storage
            if integrity_log_file and os.path.exists(integrity_log_file):
                copy_log_to_archive_container(integrity_log_file)
                files_to_attach.append(integrity_log_file)
            with DbtSnowflakeConnectionManager(
                project_dir=cwd, profiles_dir=cwd, target="dev"
            ) as db:
                batch_execution_failure_email(
                    db,
                    files_to_attach,
                    error_msg,
                    var.get("FAILURE_EMAIL_RECIPIENT").split(","),
                )
        raise RuntimeError(error_msg)


def read_config_file(config_path: str = CONFIG_FILE):
    """Reads the configuration file and extracts job details.

    The configuration file contains information about the daily batch scripts,
    including their ID, tag, and staging view. This function reads the file,
    parses the relevant section, and returns a list of job details.

    Args:
        config_path: The path to the configuration file.

    Returns:
        A list of tuples, where each tuple contains the job ID, tag, and staging view(if present else None).
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    job_list = []
    section = "daily-batch-scripts"
    if section not in config:
        raise ValueError(f"No '{section}' section found in config.")
    for item in config[section]:
        value = config[section][item]
        parts = list(
            map(lambda arg: int(arg) if arg.isdigit() else arg, value.split("|"))
        )
        # Ensure at least 3 parts by padding with None since we are unpacking the
        # parts into at least 3 variables (tag, stg_view, load_from)
        parts += [None] * (3 - len(parts)) if len(parts) < 3 else []
        # Unpacking the parts into variables
        tag, stg_view, load_from, *data_src = parts
        job_list.append((item, tag, stg_view, load_from))
    return job_list


def parse_execution_args(exec_arg_str):
    """Parses the execution argument string to determine the jobs to run.

    The execution argument string specifies the range of job IDs to execute.
    It can be a comma-separated list of single job IDs or ranges (e.g., "1,2,5-7").

    Args:
        exec_arg_str: The execution argument string.

    Returns:
        A list of tuples, where each tuple represents a range of job IDs.
    """
    ranges = []
    tokens = exec_arg_str.split(",")
    for token in tokens:
        token = token.strip()
        try:
            if "-" in token:
                parts = token.split("-")
                start = float(parts[0])
                end = float(parts[1])
                if start > end:
                    raise ValueError(f"Invalid range found: {start}>{end} ")
                ranges.append((start, end))
            else:
                val = float(token)
                ranges.append((val, val))

        except ValueError as e:
            raise ValueError(f"Error occurred {e}")
    return ranges


def filter_jobs(jobs, exec_arg_str: str):
    """Filters the list of jobs based on the execution argument string.

    The execution argument string specifies the range of job IDs to execute.
    This function filters the input job list and returns only the jobs
    whose IDs fall within the specified ranges.

    Args:
        jobs: A list of tuples, where each tuple contains job ID, tag, and staging view.
        exec_arg_str: The execution argument string.

    Returns:
        A list of tuples, where each tuple contains the tag and staging view
        of the filtered jobs.
    """
    if not exec_arg_str:
        return jobs

    ranges = parse_execution_args(exec_arg_str)
    filtered = []
    for job_id, tag, staging_view, load_data_from in jobs:
        try:
            job_id_num = float(job_id)
        except ValueError:
            continue
        for start, end in ranges:
            if start <= job_id_num <= end:
                filtered.append((job_id_num, tag, staging_view, load_data_from))
                break
    return filtered


def main():
    """Main execution controller.

    Handles:
    - Command line argument parsing
    - Environment configuration
    - Data availability checks
    - Job sequence execution
    """
    print(f"sys.argv: {sys.argv}")
    if len(sys.argv) <= 2:
        raise ValueError(
            f"Invalid Arguments detected: {sys.argv[1:]}"
            "Usage: dbt_runner.py [<hourly_execution:0|1>,<subject_area_name>[,<load_type:FULL|DAILY>]] [exec_args]"
            "Example: dbt_runner.py [0,CO_ORD] [3201,3202] or dbt_runner.py [0,CO_ORD,FULL] [3203]"
        )
    # Extra Validation for the system arguments passed from user
    if not (sys.argv[1].startswith("[") and sys.argv[1].endswith("]")):
        raise ValueError(
            f"First argument must start with '[' and end with ']'. Got: {sys.argv[1]}"
        )
    if not (sys.argv[2].startswith("[") and sys.argv[2].endswith("]")):
        raise ValueError(
            f"Second argument must start with '[' and end with ']'. Got: {sys.argv[2]}"
        )

    # Parse arguments safely
    args = sys.argv[1].strip("[]").split(",")
    # Always extract the first two arguments
    hourly_execution_arg, subject_area = args[:2]
    # load type is optional and applicable to datamart models only
    load_type = args[2] if len(args) > 2 else None
    if hourly_execution_arg not in ("0", "1"):
        raise ValueError("hourly_execution must be 0 or 1")
    is_hourly = hourly_execution_arg == "1"
    # set module name which then is passed as variable in dbt command for logging into batch log table
    module_name = "HRLY" if is_hourly else "NTLY"
    exec_args = sys.argv[2].strip("[]")
    
    # get the absolute path of current working directory(root directory of the project)
    cwd = os.path.dirname(os.path.abspath(__file__))
    # Set root directory as environment variable so that we can use it whenever required
    os.environ["ROBLING_DBT_DIR"] = cwd
    config_file_abs_path = os.path.join(cwd, CONFIG_FILE)
    # validate that the execution sequence config file exists
    if not os.path.exists(config_file_abs_path):
        raise FileNotFoundError(f"Config file not found: {config_file_abs_path}")
    # Read all jobs from the config file
    jobs = read_config_file(config_file_abs_path)
    # Filter jobs based on the execution arguments (if provided)
    jobs_to_execute = filter_jobs(jobs, exec_args)

    if not jobs_to_execute:
        raise ValueError(
            "No jobs to execute based on the provided execution arguments."
        )
    print(f"Jobs: {jobs_to_execute}")
    load_data_from = 1
    if var.exists("LOAD_DATA_FROM"):
        load_data_from = int(var.get("LOAD_DATA_FROM"))
    # Create a dictionary of script load mode for each job/tag
    # job[0] is the job ID, job[1] is the tag, job[2] is the staging view, job[3] is the load_from
    script_load_mode_dict = dict(
        [
            (
                job[1],
                (load_data_from if int(job[3]) == 0 else int(job[3])),
            )
            for job in jobs_to_execute
            if job[3] is not None
        ]
    )
    stage_name = (
        var.get("AZURE_STAGE") if load_data_from == 2 else var.get("FILE_STAGE")
    )
    # prepare environment variables
    prepare_env_vars()
    if var.get("CONFIG_LOCATION") == "2":
        # install dbt dependencies
        subprocess.run(["dbt", "deps"], check=True, cwd=cwd)
        # Validate data availability in stage or some form of handshake by monitoring the EXT_C_BATCH_LOG table at specified
        # time interval
        with DbtSnowflakeConnectionManager(
            project_dir=cwd, profiles_dir=cwd, target="dev"
        ) as db:
            check_selected_stage_data_availability(
                db,
                stage_name,
                var.get("AZURE_FILE_FORMAT"),
                script_load_mode_dict,
                var.get("POLL_TIME"),
                var.get("EMAIL_TRIGGER_TIME"),
                var.get("MAX_WAIT_TIME"),
                subject_area,
            )
    for _, tag, staging_view, _ in jobs_to_execute:
        print(f"Executing script: {tag}, staging view: {staging_view}")
        run_dbt_commands(cwd, tag, staging_view, module_name, load_type)


if __name__ == "__main__":
    main()
