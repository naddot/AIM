import sys
import logging
import asyncio
import traceback

# Import our new modules
from config import load_config
from context import Context
from status import StatusTracker
from io_manager import get_io_backend
from bq import get_bq_client
from clients.waves import WavesClient

# Import Stages
from stages import stage_1, stage_2, stage_3, stage_4
from stages import stage_5, stage_6, stage_7, stage_8, stage_9

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

async def main():
    # 1. Load Config
    try:
        config = load_config()
    except Exception as e:
        logging.critical(f"❌ Failed to load config: {e}")
        sys.exit(1)

    # 2. Init Core Infrastructure
    try:
        logging.info(f"🚀 Starting AIM Job in {config.aim_mode} Mode (Dry Run: {config.dry_run})")
        
        # Init Subsystems
        tracker = StatusTracker(config)
        io_backend = get_io_backend(config)
        bq_client = get_bq_client(config)
        waves_client = WavesClient(config)
        
        # Create Context
        ctx = Context(
            config=config,
            tracker=tracker,
            io=io_backend,
            bq=bq_client,
            waves=waves_client
        )
        ctx.tracker.update(state="running", last_log_line="Job Initialized")

    except Exception as e:
        logging.critical(f"❌ Initialization Failed: {e}")
        traceback.print_exc()
        if 'tracker' in locals():
             tracker.update(state="failed", error_summary=str(e))
        sys.exit(1)

    # 3. Define Stages
    # Tuple: (Display Name, Function, [Async?])
    pipeline = [
        ("Stage 1: Load Data", stage_1.run, False),
        ("Stage 2: Placeholder", stage_2.run, False),
        ("Stage 3: TyreScore", stage_3.run, False),
        ("Stage 4: Batch Recommender", stage_4.run, True),
        ("Stage 5: Validation", stage_5.run, False),
        ("Stage 6: Core Tables", stage_6.run, False),
        ("Stage 7: Dashboard Tables", stage_7.run, False),
        ("Stage 8: Output Core", stage_8.run, False),
        ("Stage 9: Dashboard Output", stage_9.run, False),
    ]

    # Shared State logic (passed via args or return?)
    # Stage 1 returns 'known_makes'. Stage 4 needs it.
    # While Context is static, we can pass ad-hoc data.
    known_makes = set()

    # 4. Execute Pipeline
    try:
        for name, func, is_async in pipeline:
            # Skip check using config (Local Mode Skips)
            # Original Logic:
            # if LOCAL: Skip 1, 3, 5-9 unless modified?
            # Original main.py:
            # if LOCAL: Skip 1, 3. runs 4. Skip 5-9.
            # We enforce this logic here explicitly or inside stages?
            # Better to enforce here to match golden flow manifest.
            
            skip = False
            if config.aim_mode == "local":
                 if "Stage 1" in name: skip = True
                 if "Stage 3" in name: skip = True
                 if "Stage 5" in name or "Stage 6" in name or "Stage 7" in name or "Stage 8" in name or "Stage 9" in name:
                     skip = True
            
            if skip:
                logging.info(f"⏭ Skip {name} in Local Mode.")
                continue

            ctx.tracker.update(last_log_line=f"Starting {name}")
            ctx.tracker.record_stage_start(name)
            
            logging.info(f"▶️ Executing {name}...")
            
            if "Stage 1" in name:
                known_makes = func(ctx)
            elif "Stage 4" in name:
                await func(ctx, known_makes)
            elif is_async:
                await func(ctx)
            else:
                func(ctx)
                
            logging.info(f"✅ {name} Complete.")

        ctx.tracker.update(state="success", last_log_line="Job Completed Successfully")
        logging.info("🎉 AIM Job Finished Successfully.")
        
    except Exception as e:
        logging.error(f"❌ Job Failed at {name}: {e}")
        traceback.print_exc()
        ctx.tracker.update(state="failed", error_summary=str(e), last_log_line=f"Failed at {name}")
        sys.exit(1)
    finally:
        # Validate / Save Manifest
        ctx.tracker.save_manifest()
        
if __name__ == "__main__":
    asyncio.run(main())
