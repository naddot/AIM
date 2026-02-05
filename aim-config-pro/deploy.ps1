$SERVICE_NAME = "aim-frontend"
$DEPLOY_REGION = "europe-west1" # Region for this service
$JOB_REGION = "europe-west2"    # Region where the Cloud Run Job lives
$JOB_NAME = "aim-runner"

# 1. Load PROJECT_ID from parent .env
$EnvFilePath = ".env"
if (Test-Path $EnvFilePath) {
  Write-Host "Reading .env file..." -ForegroundColor Cyan
  Get-Content $EnvFilePath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
      $parts = $line.Split("=", 2)
      if ($parts.Length -eq 2) {
        $key = $parts[0].Trim()
        $value = $parts[1].Trim()
        if ($key -eq "PROJECT_ID") {
          $global:PROJECT_ID = $value
        }
      }
    }
  }
}

if (-not $PROJECT_ID) {
  Write-Error "PROJECT_ID not found in ../.env"
}

$SA_EMAIL = "aim-cloud-sa@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "Deploying $SERVICE_NAME to Cloud Run (Project: $PROJECT_ID)..."
Write-Host "Service Account: $SA_EMAIL"

# Deploy from source
gcloud run deploy $SERVICE_NAME `
  --source . `
  --region $DEPLOY_REGION `
  --project $PROJECT_ID `
  --allow-unauthenticated `
  --service-account $SA_EMAIL `
  --set-env-vars "NODE_ENV=production, PROJECT_ID=$PROJECT_ID, REGION=$JOB_REGION, JOB_NAME=$JOB_NAME"
