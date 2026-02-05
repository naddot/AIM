#!/bin/bash
# run_demo.sh
# Sets up environment and starts the AIM Local Demo

# Ensure we are in the root directory
if [ ! -d "aim-job" ] || [ ! -d "aim-config-pro" ]; then
    echo "❌ Error: Please run this script from the repository root (AIM_growth_job)."
    exit 1
fi

ROOT_PATH=$(pwd)
echo "🏠 Root Path: $ROOT_PATH"

export AIM_MODE="local"
export AIM_LOCAL_ROOT="$ROOT_PATH/demo"
export AIM_WAVES_URL="http://localhost:5000"
export AIM_JOB_CMD="python aim-job/main.py"
export AIM_JOB_CWD="$ROOT_PATH"
export PORT="8081"
export APP_ACCESS_PASSWORD="test"

# 1. Waves Backend (Port 5000)
echo -e "\n🚀 Starting Waves Backend..."
export FLASK_APP="aim_waves.main:create_app"
cd "$ROOT_PATH/AIM-Waves"
python -m flask run --port 5000 > /dev/null 2>&1 &
WAVES_PID=$!
echo "   (PID: $WAVES_PID)"

# 2. Config Pro API Server (Port 8081)
echo "🚀 Starting Config Pro API Server..."
cd "$ROOT_PATH/aim-config-pro"
npm run server > /dev/null 2>&1 &
BACKEND_PID=$!
echo "   (PID: $BACKEND_PID)"

# 3. Config Pro Vite Frontend (Port 5173)
echo "🚀 Starting Config Pro Frontend..."
npm run dev

# Cleanup background processes on exit
trap "kill $WAVES_PID $BACKEND_PID; exit" SIGINT SIGTERM EXIT
