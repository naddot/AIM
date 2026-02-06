# AIM-Config-Pro: Management Dashboard

AIM-Config-Pro is the primary interface for managing and monitoring the AIM Growth Job. It provides tools for configuration management, job triggering, and real-time status visibility.

## 🚀 Features

- **Dynamic Configuration**: Adjust `TOTAL_OVERALL`, `BATCH_SIZE`, and ranking parameters via a sleek UI.
- **Run Mode Selector**: Switch between `Global Priority` and `Per-Segment` modes instantly.
- **Improved Stability**: Native `tsx` server execution and hardened initialization wait times (10s) for reliable local demo startup.
- **One-Click Triggering**: Manually launch the Growth Job as a Cloud Run Job (production) or local subprocess (demo).
- **Status Dashboard**: Monitoring of job states (Idle, Running, Success, Failed) with real-time heartbeat and progress tracking.

## 🏗 Component Roles

- **Frontend (Vite/React)**: The presentation layer for configuration and monitoring.
- **API Server (Node/Express)**:
    - **Cloud Mode**: Interfaces with Google Cloud Storage and Cloud Run Jobs API.
    - **Local Mode**: Interfaces with the local `./demo` directory for file-based configuration and state management.

## ⚙️ Development & Local Run

### Prerequisites
- Node.js (v18+)
- npm

### Running Locally
1. Install dependencies:
   ```bash
   npm install
   ```
2. Start the development stack:
   ```bash
   npm run dev
   ```
   *Note: This starts both the Vite frontend (Port 5173) and the API proxy server (Port 8081).*

## 🔌 API Gateway Endpoints

- `GET /api/job-status`: Returns the authoritative state and progress.
- `GET /api/current-config`: Fetches the current configuration from GCS or local file.
- `POST /api/save-config`: Persists updated parameters.
- `POST /api/trigger-job`: Spawns the Job execution.

---
*The control center for Autonomous Inventory Management.*
