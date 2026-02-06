from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 9: Dashboard Output SQL...")
    run_sql_query(ctx, "aim_merchandising_update.sql", "Dashboard Output")
    logging.info("✅ Stage 9 completed.")
