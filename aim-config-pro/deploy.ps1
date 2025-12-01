$SERVICE_NAME = "aim-config-pro-frontend"
$REGION = "europe-west1"

Write-Host "Deploying $SERVICE_NAME to Cloud Run..."

# Deploy from source
gcloud run deploy $SERVICE_NAME `
  --source . `
  --region $REGION `
  --allow-unauthenticated `
  --set-env-vars NODE_ENV=production
