from context import Context
from stages.sql import run_sql_query
import logging

def run(ctx: Context):
    logging.info("Starting Stage 3: TyreScore Algorithm...")
    run_sql_query(ctx, "tyrescore_algorithm.sql", "TyreScore Algorithm")
    logging.info("✅ Stage 3 completed successfully.")
