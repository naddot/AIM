# run_demo.ps1
# Sets up environment and starts the AIM Local Demo

# 1. Ensure we are in the root directory
if (!(Test-Path "aim-job") -or !(Test-Path "aim-config-pro")) {
    Write-Error "❌ Error: Please run this script from the repository root (AIM_growth_job)."
    exit 1
}

$RootPath = (Get-Item .).FullName
$DemoPath = (Get-Item demo).FullName
Write-Host "🏠 Root: $RootPath" -ForegroundColor Gray
Write-Host "☁️ Demo Root: $DemoPath" -ForegroundColor Gray

# 2. Nuclear Cleanup: Find and Kill processes on demo ports
Write-Host "`n🧹 Nuclear Cleanup: Freeing ports 5000, 8081, and 5173..." -ForegroundColor Gray
$DemoPorts = @(5000, 8081, 5173)
foreach ($Port in $DemoPorts) {
    # Find all processes listening on this port
    $Connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($Connections) {
        foreach ($Conn in $Connections) {
            $TargetProcId = $Conn.OwningProcess
            if ($TargetProcId -gt 0) {
                Write-Host "   - Killing process on port $Port (PID: $TargetProcId)" -ForegroundColor Yellow
                Stop-Process -Id $TargetProcId -Force -ErrorAction SilentlyContinue
                # Fallback for stubborn Node processes on Windows
                if (Get-Process -Id $TargetProcId -ErrorAction SilentlyContinue) {
                    taskkill /F /PID $TargetProcId /T | Out-Null
                }
            }
        }
    }
}
Write-Host "   - Waiting for ports to free..." -ForegroundColor Gray
Start-Sleep -Seconds 3

# 3. Waves Backend (Port 5000)
Write-Host "`n🚀 Starting Waves Backend..." -ForegroundColor Cyan
$WavesCmd = "`$host.UI.RawUI.WindowTitle = 'AIM Local: Waves Backend (5000)'; `$env:FLASK_APP='aim_waves.main:create_app'; `$env:APP_ACCESS_PASSWORD='test'; cd '$RootPath/AIM-Waves'; python -m flask run --port 5000"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $WavesCmd
Start-Sleep -Seconds 2

# 4. Config Pro API Server (Port 8081)
Write-Host "🚀 Starting Config Pro API Server..." -ForegroundColor Cyan
# Passing ABSOLUTE paths to ensure server.ts can find the demo folder
# Using 'npx tsx server.ts' directly to avoid npm script issues and ensure visible output
$BackendCmd = "`$host.UI.RawUI.WindowTitle = 'AIM Local: Config Pro API (8081)'; `$env:AIM_MODE='local'; `$env:AIM_LOCAL_ROOT='$DemoPath'; `$env:AIM_WAVES_URL='http://localhost:5000'; `$env:AIM_SERVICE_PASSWORD='test'; `$env:AIM_JOB_CMD='python aim-job/main.py'; `$env:AIM_JOB_CWD='$RootPath'; `$env:PORT='8081'; cd '$RootPath/aim-config-pro'; npx tsx server.ts"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $BackendCmd

Write-Host "⏳ Waiting 10s for API Server to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# 5. Config Pro Vite Frontend (Port 5173)
Write-Host "🚀 Starting Config Pro Frontend..." -ForegroundColor Cyan
Set-Location "$RootPath/aim-config-pro"
npm run dev
