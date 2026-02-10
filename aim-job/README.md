# AIM Job (Runner)

The core orchestration service for the AIM Growth Job. It runs as a **Cloud Run Job** and orchestrates the data pipeline from ingestion to recommendation generation and storage.

## Architecture

The job consists of sequential stages:

1.  **Stage 1: Load Data**: Ingests `CarMakeModelSales.csv` and `TyreScore.csv` from GCS (bucket: `tyrescore`) into BigQuery.
2.  **Stage 2**: Placeholder.
3.  **Stage 3: TyreScore**: Runs SQL algorithms to calculate base Tyre Scores.
4.  **Stage 4: Batch Recommender**:
    -   Iterates through Vehicle/Size combinations.
    -   Calls `AIM-Waves` (Engine) to get recommendations.
    -   Stores results in BigQuery (`AIMData` and `CAM_SKU`).
5.  **Stages 5-9**: Validation, Core Tables, Dashboard Tables, Output Generation.

## Configuration

Configuration is managed via environment variables (in `.env`) and can be overridden by a GCS JSON config file.

### Key Environment Variables

-   `PROJECT_ID`: GCP Project ID.
-   `TYRESCORE_BUCKET`: Bucket containing source CSVs (default: `tyrescore`).
-   `AIM_WAVES_URL`: URL of the AIM Engine service.
-   `AIM_RUN_MODE`: `GLOBAL` (top X overall) or `PER_SEGMENT`.
-   `AIM_TOTAL_OVERALL`: Total items to process in GLOBAL mode.

## Local Development

1.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
2.  Run locally:
    ```bash
    python main.py
    ```

## Deployment

To deploy the job to Cloud Run:

```powershell
.\deploy.ps1
```

This script:
1.  Builds the Docker image (`aim-runner`).
2.  Pushes it to Google Container Registry (`gcr.io`).
3.  Updates (or creates) the Cloud Run Job.

## Recent Updates

-   **Stage 1 Fix**: Explicitly uses the `tyrescore` bucket for input files, resolving access issues in Cloud mode.
-   **SKU Expansion**: Now processes up to **20 SKUs** (plus 4 Hotboxes) to match the BigQuery schema.
-   **SQL Path Fix**: Correctly resolves paths for sequence templates in containerized environments.
-   **Cost Reporting**: Updated to reflect **GBP** pricing for Gemini 2.5 Flash-Lite.
-   **Search Re-enabled**: Verification mode active to compare token usage with/without Vertex AI Search.
-   **SQL Optimizations**:
    -   `aim_merchandising_update.sql`: Refactored to use a **TEMP TABLE** and unified scan, reducing redundant unnesting.
    -   `tyrescore_algorithm.sql`: Fixed logic to include "Hidden Gems" (high score, no sales) using `LEFT JOIN` and added robust `SAFE_CAST`.
-   **Authentication Robustness**: Implemented auto-refresh for OIDC tokens to prevent `401 Unauthorized` errors during long-running batch jobs.
-   **Rate Limit Handling**: Added exponential backoff retry logic for `429 Resource Exhausted` errors from Vertex AI.
