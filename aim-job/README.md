# AIM-Growth-Job: The Orchestrator

The Growth Job is the central orchestrator responsible for executing the inventory ranking pipeline. It manages data ingestion, batch pushes to the compute engine, and result persistence.

## 🏗 Run Modes

### 1. Global Priority Mode (`GLOBAL`) — *Recommended*
- Processes vehicles based on a global `PriorityRank` CSV.
- User-defined `TOTAL_OVERALL` limit (e.g., top 10,000).
- Uses the high-performance `Push Batch` architecture.

### 2. Per-Segment Mode (`PER_SEGMENT`) — *Legacy*
- Processes each vehicle segment (e.g., "Economy", "Performance") in parallel.
- User-defined `TOTAL_PER_SEGMENT` limit.

## 🛠 Features

- **Smart Batching**: Chunks large runlists into `BATCH_SIZE` requests for Waves.
- **Rich Recommendations**: Supports up to 24 suggestion slots (4 Hotboxes + 20 List items) with strict order preservation.
- **Robust Retries**: Attempts to recover failed CAMs in a specialized retry pass with internal formatting glitch protection.
- **Audit Manifests**: Writes detailed `run_id` audit logs including uri, row counts, and error breakdowns.
- **Local Mode Interop**: Detects `AIM_MODE=local` to swap Cloud services (GCS, BigQuery) for local directory I/O.

## ⚙️ Environment Configuration

| Variable | Description | Default |
| :--- | :--- | :--- |
| `AIM_WAVES_URL` | URL of the Waves compute engine. | Required |
| `AIM_RUN_MODE` | `GLOBAL` or `PER_SEGMENT`. | `PER_SEGMENT` |
| `AIM_BATCH_SIZE` | Rows per request to Waves. | `500` |
| `AIM_MODE` | `local` or `cloud`. | `cloud` |
| `AIM_LOCAL_ROOT` | Path to local demo assets. | `./demo` |

## 📁 Local I/O Simulation

In local mode, the job interacts with:
- `demo/config/`: Configuration overrides.
- `demo/runlist/`: Source CSV data.
- `demo/output/`: CSV results and `job_status.json`.
- `demo/logs/`: Request manifests and execution logs.

---
*The glue of the Autonomous Inventory Management system.*
