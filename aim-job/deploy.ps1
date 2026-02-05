# Deploy Script for AIM Growth Job
# Usage: .\deploy.ps1

$ErrorActionPreference = "Stop"

# 1. Load Configuration from .env
Write-Host "Reading .env file..." -ForegroundColor Cyan
$EnvVars = @{}
Get-Content .env | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#")) {
        $parts = $line.Split("=", 2)
        if ($parts.Length -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim()
            $EnvVars[$key] = $value
        }
    }
}

$PROJECT_ID = $EnvVars["PROJECT_ID"]
if (-not $PROJECT_ID) {
    Write-Error "PROJECT_ID not found in .env"
}

$REGION = "europe-west2" # Default region
$IMAGE_NAME = "aim-runner"
$JOB_NAME = "aim-runner"
$IMAGE_URI = "gcr.io/$PROJECT_ID/$IMAGE_NAME"

$SA_EMAIL = "aim-cloud-sa@$PROJECT_ID.iam.gserviceaccount.com"

Write-Host "Project ID: $PROJECT_ID" -ForegroundColor Yellow
Write-Host "Region:     $REGION" -ForegroundColor Yellow
Write-Host "Image URI:  $IMAGE_URI" -ForegroundColor Yellow
Write-Host "Service Account: $SA_EMAIL" -ForegroundColor Yellow

# 2. Build and Push Image
Write-Host "`nBuilding and Pushing Docker Image..." -ForegroundColor Cyan
gcloud builds submit --tag $IMAGE_URI . --project $PROJECT_ID

if ($LASTEXITCODE -ne 0) {
    Write-Error "Build failed."
}

# 3. Construct Env Vars String for Deployment
# We exclude DRY_RUN from the loop because we want to force it to False
$EnvVarString = "DRY_RUN=False" 
$EnvVarString += ",AIM_PRIORITY_RUNLIST_GCS_URI=gs://aim-home/aim-priority-runlist/AIM rankings priority_runlist_current.csv" 

foreach ($key in $EnvVars.Keys) {
    if ($key -ne "DRY_RUN") {
        $val = $EnvVars[$key]
        $EnvVarString += ",$key=$val"
    }
}

# 4. Deploy Cloud Run Job
Write-Host "`nDeploying Cloud Run Job: $JOB_NAME..." -ForegroundColor Cyan

# Check if job exists to decide between create or update (though deploy/update handles both usually, 'deploy' isn't a command for jobs, 'update' or 'create' is)
# Actually 'gcloud run jobs deploy' allows creating/updating in one go in newer versions, but 'update' is safer if it exists, 'create' if not.
# Simplest is `gcloud run jobs update` if it exists, or `create` if not.
# But `gcloud run jobs deploy` is the declarative command. Let's use `update` and fall back to `create` or just `create --overwrite`?
# `gcloud run jobs deploy` is NOT a standard command. It is `gcloud run jobs create` or `gcloud run jobs update`.

# We will try to update, if it fails (doesn't exist), we create.
Write-Host "Attempting to update existing job..."
gcloud run jobs update $JOB_NAME `
    --image $IMAGE_URI `
    --region $REGION `
    --set-env-vars $EnvVarString `
    --project $PROJECT_ID `
    --service-account $SA_EMAIL `
    --tasks 1 `
    --max-retries 0 `
    --quiet

if ($LASTEXITCODE -ne 0) {
    Write-Host "Job might not exist. Attempting to create..." -ForegroundColor Yellow
    gcloud run jobs create $JOB_NAME `
        --image $IMAGE_URI `
        --region $REGION `
        --set-env-vars $EnvVarString `
        --project $PROJECT_ID `
        --service-account $SA_EMAIL `
        --tasks 1 `
        --max-retries 0 `
        --quiet
}

Write-Host "`n✅ Deployment Complete!" -ForegroundColor Green
Write-Host "To execute the job immediately, run:"
Write-Host "gcloud run jobs execute $JOB_NAME --region $REGION --project $PROJECT_ID" -ForegroundColor White
