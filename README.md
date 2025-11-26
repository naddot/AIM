# AIM Growth Job

This project implements the AIM Growth pipeline as a Google Cloud Run Job. It processes data from GCS, runs the TyreScore algorithm, executes the AIM batch runner, and updates various BigQuery tables.

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Google Cloud SDK (`gcloud`) installed and authenticated
- Docker (for local container testing)

### Local Setup
1.  **Clone/Open the repository.**
2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    .\venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure Environment:**
    - Copy `.env.example` (if available) or create a `.env` file based on the Configuration section below.
    - Ensure you have a valid `application_default_credentials.json` or run `gcloud auth application-default login`.

## ⚙️ Configuration

The application is configured primarily via **Environment Variables**, with an optional **GCS Configuration File** for specific overrides.

### Environment Variables (`.env`)
Create a `.env` file in the root directory. Key variables include:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PROJECT_ID` | GCP Project ID | `bqsqltesting` |
| `DRY_RUN` | If `True`, skips all write operations (BQ inserts, GCS uploads). | `False` (defaults to `True` in local `.env`) |
| `CONFIG_GCS_URI` | GCS URI for the optional JSON config file. | `None` |
| `IGNORE_GCS_CONFIG` | If `True`, ignores `CONFIG_GCS_URI` and uses env vars only. | `False` |
| `AIM_BASE_URL` | Base URL for the AIM API. | `...` |
| `AIM_SERVICE_PASSWORD` | Password for the AIM service. | `...` |

### GCS Configuration Override
You can store a JSON configuration file in GCS (e.g., `gs://your-bucket/config.json`) to allow non-developers (e.g., Merchandising) to update specific parameters without redeploying.

**Supported Overrides:**
- `TOTAL_PER_SEGMENT`
- `GOLDILOCKS_ZONE_PCT`
- `PRICE_FLUCTUATION_UPPER` / `LOWER`
- `BRAND_ENHANCER` / `MODEL_ENHANCER`
- `SEASON`
- `LIMIT_TO_SEGMENTS`

**Behavior:**
1.  The script checks `CONFIG_GCS_URI`.
2.  If found, it downloads the JSON.
3.  It validates each key individually. Invalid values are logged as warnings, and the default (env var) value is kept.
4.  To force the script to ignore this file, set `IGNORE_GCS_CONFIG=True`.

## 🛠️ Local Development

### Running the Script
To run the full pipeline locally:

```bash
python main.py
```

**Note:** Ensure `DRY_RUN=True` in your `.env` file to prevent accidental writes to production tables during testing.

### Docker Testing
To build and run the container locally:

```bash
docker build -t aim-job .
docker run --env-file .env aim-job
```

## ☁️ Deployment

Deployment is automated via the `deploy.ps1` PowerShell script.

### Usage
```powershell
.\deploy.ps1
```

### What it does:
1.  **Reads `.env`**: Loads configuration variables.
2.  **Builds Image**: Submits a build to Google Cloud Build (`gcr.io/PROJECT_ID/aim-growth-job`).
3.  **Deploys Job**: Creates or updates the Cloud Run Job `aim-growth-job`.
4.  **Enforces Production Mode**: Explicitly sets `DRY_RUN=False` for the deployed job.

## 📂 Project Structure

- `main.py`: Entry point. Contains logic for all stages (1-9) and configuration loading.
- `deploy.ps1`: Deployment automation script.
- `Dockerfile`: Container definition.
- `requirements.txt`: Python dependencies.
- `*.sql`: SQL queries used by BigQuery stages.
