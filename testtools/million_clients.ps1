[CmdletBinding()]
param(
    [ValidateRange(1, 2147483647)]
    [int]$Clients = 1024,

    [ValidateRange(1, 2147483647)]
    [int]$MessagesPerClient = 1024,

    [ValidateRange(1, 2147483647)]
    [int]$Resources = 1000,

    [ValidateRange(1, 2147483647)]
    [int]$Concurrency = 512,

    [ValidateRange(1, 2147483647)]
    [int]$LockBatch = 64,

    [ValidateNotNullOrEmpty()]
    [string]$Timeout = "60m",

    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$artifactRoot = Join-Path $repoRoot ".test-artifacts"
$serverExecutable = Join-Path $artifactRoot "raft-node.exe"
$runnerExecutable = Join-Path $artifactRoot "clustercheck.exe"
$runID = [DateTimeOffset]::UtcNow.ToString("yyyyMMdd-HHmmss")
$workRoot = Join-Path $artifactRoot "million-clients-$runID"
$resultPath = Join-Path $artifactRoot "million-clients-$runID.log"
$resourcePrefix = "million-$runID"

New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null
$env:GOCACHE = Join-Path $artifactRoot "go-build-cache"
New-Item -ItemType Directory -Path $env:GOCACHE -Force | Out-Null

Push-Location $repoRoot
try {
    if (-not $SkipBuild) {
        go test ./...
        if ($LASTEXITCODE -ne 0) {
            throw "go test failed with exit code $LASTEXITCODE"
        }

        go vet ./...
        if ($LASTEXITCODE -ne 0) {
            throw "go vet failed with exit code $LASTEXITCODE"
        }

        go build -buildvcs=false -trimpath -ldflags="-s -w" -o $serverExecutable ./cmd/server
        if ($LASTEXITCODE -ne 0) {
            throw "server build failed with exit code $LASTEXITCODE"
        }

        go build -buildvcs=false -trimpath -ldflags="-s -w" -o $runnerExecutable ./testtools/clustercheck
        if ($LASTEXITCODE -ne 0) {
            throw "clustercheck build failed with exit code $LASTEXITCODE"
        }
    }

    if (-not (Test-Path -LiteralPath $serverExecutable)) {
        throw "server executable not found: $serverExecutable"
    }
    if (-not (Test-Path -LiteralPath $runnerExecutable)) {
        throw "clustercheck executable not found: $runnerExecutable"
    }

    Write-Host "Starting $Clients logical clients with $Concurrency concurrent workers."
    Write-Host "Results: $resultPath"

    # Go's standard logger writes normal progress to stderr. Windows
    # PowerShell otherwise promotes that output to a NativeCommandError when
    # ErrorActionPreference is Stop, terminating an otherwise healthy run.
    $savedErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $runnerExecutable `
            --manage-servers `
            --mode=multi `
            --server-exe=$serverExecutable `
            --server-work-root=$workRoot `
            --resource-prefix=$resourcePrefix `
            --clients=$Clients `
            --messages=$MessagesPerClient `
            --resources=$Resources `
            --concurrency=$Concurrency `
            --lock-batch=$LockBatch `
            --snapshot-threshold=5000000 `
            --timeout=$Timeout 2>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath $resultPath
        $runnerExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorActionPreference
    }
    if ($runnerExitCode -ne 0) {
        throw "million-client test failed with exit code $runnerExitCode; see $resultPath"
    }
}
finally {
    Pop-Location
}
