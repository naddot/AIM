# Deploy Script for AIM-Engine (AIM-Waves)
# Usage: .\deploy.ps1

$ErrorActionPreference = "Stop"
$SERVICE_NAME = "aim-engine"
$REGION = "europe-west1"

# 1. Load Configuration from .env
Write-Host "Reading .env file..." -ForegroundColor Cyan
$EnvVars = @{}
if (Test-Path .env) {
    Get-Content .env | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $parts = $line.Split("=", 2)
            if ($parts.Length -eq 2) {
                $key = $parts[0].Trim()
                $value = $parts[1].Trim()
                $EnvVars[$key] = $value
                if ($key -eq "PROJECT_ID") {
                    $global:PROJECT_ID = $value
                }
            }
        }
    }
}

if (-not $PROJECT_ID) {
    Write-Error "PROJECT_ID not found in .env"
}

# Construct Env Vars String
$EnvVarString = ""
foreach ($key in $EnvVars.Keys) {
    $val = $EnvVars[$key]
    if ($EnvVarString -eq "") {
        $EnvVarString = "$key=`"$val`""
    } else {
        $EnvVarString += ",$key=`"$val`""
    }
}

$SA_EMAIL = "aim-cloud-sa@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "Deploying $SERVICE_NAME to Cloud Run (Project: $PROJECT_ID)..."
Write-Host "Service Account: $SA_EMAIL"

$IMAGE_TAG = "gcr.io/$PROJECT_ID/aim-engine:latest"

Write-Host "Building and Pushing Docker Image..." -ForegroundColor Cyan
gcloud builds submit --tag $IMAGE_TAG . --project $PROJECT_ID

Write-Host "Deploying to Cloud Run..." -ForegroundColor Cyan
gcloud run deploy $SERVICE_NAME `
    --image $IMAGE_TAG `
    --region $REGION `
    --project $PROJECT_ID `
    --no-allow-unauthenticated `
    --service-account $SA_EMAIL `
    --set-env-vars "$EnvVarString" `
    --memory 2Gi `
    --timeout 3600

Write-Host "`n✅ Deployment logic initiated!" -ForegroundColor Green
