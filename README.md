# AIM Growth Job Project

This repository contains the microservices and job orchestration for the AIM Growth Job.

## Global Structure

The project is divided into three main components:

1.  **`aim-config-pro/`**: A Next.js frontend application for configuring and running the job.
2.  **`aim-job/`**: The core Python job orchestrator that runs on Cloud Run Jobs. It handles BigQuery data loading, processing, and orchestration.
3.  **`AIM-Waves/`** (aka `aim-engine`): A Cloud Run service acting as the AI Engine. It processes requests from `aim-job`, calling Vertex AI (Gemini) to generate recommendations.

## Prerequisites

-   Google Cloud Platform (GCP) Project
-   Python 3.11+
-   Node.js 18+ (for `aim-config-pro`)
-   `gcloud` CLI installed and authenticated

## Deployment

Each service has its own deployment script (`deploy.ps1` or `package.json` scripts). Refer to the `README.md` in each subdirectory for specific instructions.

## Key Workflows

1.  User constructs a configuration via **AIM Config Pro**.
2.  Configuration is validated and sent to trigger **AIM Job**.
3.  **AIM Job** loads data from BigQuery and iterates through segments.
4.  For each batch of data, **AIM Job** calls **AIM Engine** (`AIM-Waves`).
5.  **AIM Engine** uses Gemini to generate insights/recommendations and returns them.
6.  **AIM Job** saves results back to BigQuery.

## Recent Updates (Feb 2026)

-   **AIM-Waves**: Upgraded to **Gemini 2.5 Flash-Lite**. Fixed SKU truncation (now returns 24 items). Fixed BigQuery case-sensitivity issues.
-   **aim-job**: Fixed Stage 1 bucket access config. Updated cost reporting to GBP. Corrected SQL file path resolution.
-   **Full Pipeline**: Verified end-to-end processing of 24-token recommendations.
