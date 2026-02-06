from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 6: Core Tables...")
    run_sql_query(ctx, "aim_analysis_update.sql", "Core Tables")
    logging.info("✅ Stage 6 completed.")
