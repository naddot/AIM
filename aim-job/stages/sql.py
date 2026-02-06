import hashlib
from context import Context
from bq import execute_query_from_file

def run_sql_query(ctx: Context, file_path: str, description: str):
    """
    Executes a SQL file using the BQ client in Context.
    Also records the execution in the Manifest.
    """
    # 1. Calculate Hash for Manifest
    try:
        if ctx.io.exists(file_path):
            # Read bytes for hashing
            content_bytes = ctx.io.read_bytes(file_path)
            sha256 = hashlib.sha256(content_bytes).hexdigest()
            size = len(content_bytes)
            
            # Record in Tracker
            ctx.tracker.record_sql_execution(file_path, size, sha256)
        else:
            # File missing? Let execute_query handle the crash
            pass
    except Exception as e:
        # Don't fail execution just because hashing failed, let BQ fail if file is bad
        # But logging it is good
        pass

    # 2. Execute via BQ wrapper
    # Using dry_run flag from config
    execute_query_from_file(ctx.bq, file_path, ctx.config.dry_run)
