from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 8: Output Core SQL...")
    run_sql_query(ctx, "aim_insights_update.sql", "Output Core")
    logging.info("✅ Stage 8 completed.")
