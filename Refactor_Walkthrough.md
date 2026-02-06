# Refactoring aim-job/main.py - Walkthrough

## Overview
The monolithic `aim-job/main.py` has been refactored into a modular, testable architecture. The logic is now distributed across specialized components while maintaining identical behavior to the original script (verified via "Golden" outputs).

### New Architecture
The project structure is now:

- **`aim-job/main.py`**: Lightweight orchestrator. Initializes Context and runs Stages.
- **`aim-job/config.py`**: Type-safe configuration loader (Env + Optional GCS/Local JSON).
- **`aim-job/context.py`**: Dependency Injection container (`config`, `io`, `bq`, `waves`, `tracker`).
- **`aim-job/status.py`**: Manages `job_status.json` and internal Verification Manifest.
- **`aim-job/io_manager.py`** & **`file_io/`**: I/O abstraction layer.
    - `backend.py`: Interface implementing strict path sanitation.
    - `local_backend.py`: Filesystem operations.
    - `gcs_backend.py`: Google Cloud Storage operations.
- **`aim-job/bq.py`**: Explicit wrappers for BigQuery operations.
- **`aim-job/clients/waves.py`**: API client for AIM Waves/Engine service with explicit error handling.
- **`aim-job/stages/`**: Domain logic and Stage Runners.
    - `processing.py`, `sizes.py`: logical transformations.
    - `sql.py`: Shared SQL runner (hashing & execution).
    - `stage_1.py` ... `stage_9.py`: Individual stage entry points.

## Verification
A rigorous verification process was used to ensure zero regression.

### Methodology
1.  **Golden Capture** (`capture_golden.py`):
    - Ran the *legacy* code in a mocked Local environment (mocking Network/BQ to avoid side effects).
    - Captured `job_status.json` and generated a `run_manifest.json` (execution flow).
2.  **Refactor Execution**:
    - Ran the *new* modular code in the exact same mocked environment.
    - The new code *natively* generates `run_manifest.json` via `StatusTracker`.
3.  **Comparison** (`verify_refactor.py`):
    - Uses **hand-written mocks** (via `unittest.mock`) to simulate Network (Waves API) and BigQuery interactions, ensuring deterministic runs without external dependencies.
    - Compared Manifests: Confirmed Stage execution order and logical flow match.
    - Compared Status: Confirmed schema integrity.
    - **Result**: `VERIFICATION SUCCESSFUL`.
        - **Behavior Change**: The Status Tracker now correctly reports `success` on completion. The legacy code left the status as `idle` (a known bug explicitly fixed in this refactor).

### Key Improvements
-   **Dependency Injection**: No more global state. Components receive explicit dependencies.
-   **Testability**: Stages and clients can now be unit-tested independently using mocked Context and IO backends.
-   **IO Abstraction**: Local/Cloud logic is handled by the backend, not `if/else` sprawl.
-   **Manifest Verification**: The code now self-reports its execution path (stages run, SQL files executed with hashes), enabling drift detection.
-   **Safety**: Imports are improved, and retry policies are explicit.

## Usage
The refactor is **backwards compatible** at the CLI and environment-variable level. Ops workflows do not need to change.

The entry point remains:
```bash
python aim-job/main.py
```
Environment variables (e.g., `AIM_MODE`, `DRY_RUN`) continue to control behavior as before.
