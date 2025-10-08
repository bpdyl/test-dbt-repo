from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.config.runtime import RuntimeConfig
from dbt.mp_context import get_mp_context
from argparse import Namespace
import copy
import os
from dbt.flags import set_flags, set_from_args
from dbt_common.context import set_invocation_context


class DbtSnowflakeConnectionManager:
    def __init__(self, project_dir, profiles_dir, target="dev"):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.config = None
        self.adapter = None
        self._env = copy.copy(os.environ)

    def __enter__(self):
        # Set up invocation context with current environment
        set_invocation_context(self._env)

        # Configure flags
        args = Namespace(
            profiles_dir=self.profiles_dir,
            project_dir=self.project_dir,
            target=self.target,
            profile=None,
            threads=1,
            single_threaded=True,
            vars={},
        )
        set_flags(args)
        set_from_args(args, None)

        self.config = RuntimeConfig.from_args(
            Namespace(
                profiles_dir=self.profiles_dir,
                project_dir=self.project_dir,
                target=self.target,
                profile=None,
                threads=1,
                single_threaded=True,
            )
        )
        # Initialize adapter
        register_adapter(self.config, get_mp_context())
        self.adapter = get_adapter(self.config)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.adapter:
            connection = self.adapter.connections.get_thread_connection()
            if connection:
                self.adapter.release_connection()
            self.adapter.connections.cleanup_all()
            self.adapter.connections.clear_transaction()
            reset_adapters()

    def get_connection(self):
        """Get raw Snowflake connection object"""
        with self.adapter.connection_named("dbt_runner_custom_connection"):
            conn = self.adapter.connections.get_thread_connection()
            return conn.handle

    def execute_agate_sql(self, sql, fetch=False):
        """Execute raw SQL using dbt's adapter that returns agate table"""
        with self.adapter.connection_named("custom_connection"):
            _, table = self.adapter.execute(sql, auto_begin=False, fetch=fetch)
            return table

    def get_data(self, sql):
        """Get data from Snowflake using snowflake raw connection"""
        snowflake_conn = self.get_connection()
        cursor = snowflake_conn.cursor()
        cursor.execute(sql)
        print(f"Snowflake Query id: {cursor.sfqid}")
        print(f"Number of rows: {cursor.rowcount}")
        return cursor.fetchall()


##usage examples
# import os
# with DbtSnowflakeConnectionManager(
#         project_dir=os.getcwd(),
#         profiles_dir=os.getcwd(),  # Assuming profiles.yml is in project dir
#         target='dev'
#     ) as conn_mgr:

#         # Example 1: Access raw Snowflake connection
#         data = conn_mgr.get_data("SELECT PARAM_VALUE FROM DW_DWH.DWH_C_PARAM")
#         print(data)

# Example 2: Use dbt's execute method
# result_table = conn_mgr.execute_agate_sql(
#     "SELECT * FROM DW_DWH.DWH_F_RECON_LD LIMIT 10",
#     fetch=True
# )
# print(result_table.rows)

