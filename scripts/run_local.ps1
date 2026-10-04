param([int]$Rows = 10000, [string]$RunId = ("local_" + (Get-Date -Format "yyyyMMddTHHmmss")))
$ErrorActionPreference = "Stop"
foreach ($stage in @("ingest", "export", "prepare", "features", "train", "score")) {
    docker compose run --rm runner -m churn.cli $stage --mode local --rows $Rows --run-id $RunId
    if ($LASTEXITCODE -ne 0) { throw "Stage failed: $stage. Check the output above." }
}
Write-Host "Finished run $RunId"
