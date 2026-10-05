# Builds every service into a runnable .jar file
$services = "customer", "restaurant", "order", "payment", "delivery", "notification", "admin"

foreach ($s in $services) {
    Write-Host "Building $s-service..." -ForegroundColor Cyan
    Push-Location "services/$s-service"
    bal build
    if ($LASTEXITCODE -ne 0) {
        Pop-Location
        Write-Host "Build FAILED for $s-service" -ForegroundColor Red
        exit 1
    }
    Pop-Location
}

Write-Host "All 7 services built!" -ForegroundColor Green