# AIM Waves (AIM Engine)

This is the AI Engine service for the AIM Growth Job. It is a Python Flask/Gunicorn application running on Google Cloud Run.

## Responsibilities

-   Receives batch requests of Vehicle/Size combinations from `aim-job`.
-   **Data Prioritization**: Fetches tyre data from the live BigQuery table `bqsqltesting.nexus_tyrescore.TyreScore_algorithm_output` with an automatic fallback to local `benchmark_final_balanced.csv` if BQ is unavailable.
-   **Smart Backfill**: Implements intelligent backfilling to ensure a complete set of **24 unique items** (4 Hotboxes + 20 SKUs) by filling gaps with unused high-ranking tyres from the source data.
-   Constructs prompts for Vertex AI (Gemini) using the **Gemini 2.5 Flash-Lite** model.
-   Parses and validates the AI response with robust retry-on-glitch logic.
-   Returns structured, deduplicated recommendations.

## Recent Updates

-   **Model Upgrade**: Switched to `gemini-2.5-flash-lite`.
-   **Output Fix**: Padded output to **24 items** (4 HB + 20 SKU) to prevent truncation.
-   **BigQuery Fix**: Added case-insensitive matching (`LOWER(...)`) for robust `TyreScore` lookups.
-   **Data Quality**: Now processes "Hidden Gems" (high score, no sales history) for better recommendations.
-   **Rate Limit Resilience**: Implemented internal exponential backoff retry logic to handle `429 Resource Exhausted` errors from the Gemini API without failing the batch.

## Local Development

1.  Create a virtual environment:
    ```bash
    python -m venv venv
    .\venv\Scripts\Activate
    ```
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Set up `.env` file (copy from `.env.example` if available, or ask admin).
4.  Run locally:
    ```bash
    python main.py
    ```

## Deployment

Use the provided PowerShell script:

```powershell
.\deploy.ps1
```

This builds the container and deploys it to Cloud Run.
