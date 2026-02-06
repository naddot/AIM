from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 7: Dashboard Tables...")
    run_sql_query(ctx, "aim_dashboard_update.sql", "Dashboard Tables")
    logging.info("✅ Stage 7 completed.")
