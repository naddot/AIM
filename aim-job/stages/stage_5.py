from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 5: Validation Tables...")
    run_sql_query(ctx, "validation_tables.sql", "Validation Tables")
    logging.info("✅ Stage 5 completed.")
