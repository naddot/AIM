$token = Get-Content token.txt
if (!$token) { Write-Error "No token found"; exit 1 }
try {
    $response = Invoke-WebRequest -Uri "https://aim-config-dashboard-qfcxf7jn2q-nw.a.run.app/api/job-status" -Headers @{Authorization = "Bearer $token"} -UseBasicParsing
    Write-Host "Status: $($response.StatusCode)"
    Write-Host "Content: $($response.Content)"
} catch {
    Write-Error $_
}
