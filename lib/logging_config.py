import logging
from dbt_common.events.base_types import EventMsg
from datetime import datetime


EVENTS_TO_LOG = [
    "AdapterRegistered",
    "AdapterEventDebug",
    "CommandCompleted",
    "ConcurrencyLine",
    "EndOfRunSummary",
    "FinishedRunningStats",
    "FoundStats",
    "HooksRunning",
    "LogHookEndLine",
    "LogHookStartLine",
    "LogModelResult",
    "LogStartLine",
    "LogTestResult",
    "MainReportVersion",
    "NodeStart",
    "NodeFinished",
    "RunResultError",
    "SkippingDetails",
    "StatsLine",
    "UnusedResourceConfigPath",
    "SQLQuery",
    "SQLQueryStatus",
    "JinjaLogInfo"
]

class DbtLogHandler:
    def __init__(self, log_file: str):
        self.logger = logging.getLogger(f"dbt_run_{datetime.now().strftime('%Y%m%d%H%M%S')}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False  # Prevents duplicate logs from root logger
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)

    def log_event(self, event: EventMsg) -> None:
        """Log dbt events based on their level and type"""
        event_info = event.info
        
        if (name := event_info.name) not in EVENTS_TO_LOG:
            return None
        level = event_info.level
        msg = f"{level}: {event_info.msg}"

        if level == "error":
            self.logger.error(msg)
        elif level == "warn":
            self.logger.warning(msg)
        elif level == "info" or event_info.name in ["JinjaLogInfo", "SQLQuery", "SQLQueryStatus","AdapterEventDebug","CommandCompleted"]:
            self.logger.info(msg)
        else:
            self.logger.debug(msg) 