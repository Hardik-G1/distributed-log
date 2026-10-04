[CmdletBinding()]
param(
    [switch]$SkipVet,
    [switch]$KeepGoing
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$runID = Get-Date -Format "yyyyMMdd-HHmmss"
$runRoot = Join-Path $repoRoot ".test-artifacts\correctness-$runID"
$serverExe = Join-Path $runRoot "server.exe"
$clustercheckExe = Join-Path $runRoot "clustercheck.exe"
$summaryPath = Join-Path $runRoot "summary.txt"
$previousGoCache = $env:GOCACHE
$env:GOCACHE = Join-Path $runRoot "go-build-cache"

New-Item -ItemType Directory -Path $runRoot -Force | Out-Null
Push-Location $repoRoot

$results = [System.Collections.Generic.List[string]]::new()

function Invoke-NativeStep {
    param(
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$Executable,
        [Parameter(Mandatory)] [string[]]$Arguments
    )

    $safeName = $Name.ToLowerInvariant() -replace '[^a-z0-9]+', '-'
    $logPath = Join-Path $runRoot "$safeName.log"
    $started = Get-Date
    Write-Host "`n=== $Name ===" -ForegroundColor Cyan
    # Windows PowerShell converts native stderr into ErrorRecord objects. Go's
    # logger writes normal progress to stderr, so do not treat those records as
    # terminating PowerShell errors; the process exit code remains authoritative.
    $savedErrorPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        & $Executable @Arguments 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $savedErrorPreference
    }
    $elapsed = (Get-Date) - $started
    if ($exitCode -eq 0) {
        $results.Add("PASS`t$Name`t$($elapsed.ToString())`t$logPath")
        return
    }

    $results.Add("FAIL`t$Name`t$($elapsed.ToString())`t$logPath")
    if (-not $KeepGoing) {
        throw "$Name failed with exit code $exitCode. See $logPath"
    }
}

function Invoke-ClusterScenario {
    param(
        [Parameter(Mandatory)] [string]$Name,
        [Parameter(Mandatory)] [string]$Mode,
        [string[]]$ExtraArguments = @()
    )

    $safeName = $Name.ToLowerInvariant() -replace '[^a-z0-9]+', '-'
    $workRoot = Join-Path $runRoot "cluster-$safeName"
    $arguments = @(
        "--mode=$Mode",
        "--manage-servers",
        "--server-exe=$serverExe",
        "--server-work-root=$workRoot",
        "--timeout=5m"
    ) + $ExtraArguments
    Invoke-NativeStep -Name $Name -Executable $clustercheckExe -Arguments $arguments
}

try {
    Invoke-NativeStep -Name "Go unit tests" -Executable "go" -Arguments @("test", "./...", "-count=1", "-timeout=3m")
    if (-not $SkipVet) {
        Invoke-NativeStep -Name "Go vet" -Executable "go" -Arguments @("vet", "./...")
    }
    Invoke-NativeStep -Name "Build server" -Executable "go" -Arguments @("build", "-buildvcs=false", "-o", $serverExe, "./cmd/server")
    Invoke-NativeStep -Name "Build cluster checker" -Executable "go" -Arguments @("build", "-buildvcs=false", "-o", $clustercheckExe, "./testtools/clustercheck")

    Invoke-ClusterScenario -Name "API and idempotency" -Mode "sanity"
    Invoke-ClusterScenario -Name "Concurrent exact messages" -Mode "multi" -ExtraArguments @(
        "--clients=64",
        "--messages=64",
        "--resources=128",
        "--concurrency=64",
        "--lock-batch=8"
    )
    Invoke-ClusterScenario -Name "Same lock concurrent appends" -Mode "stress" -ExtraArguments @(
        "--requests=2000",
        "--concurrency=64"
    )
    Invoke-ClusterScenario -Name "Follower loss and catch-up" -Mode "follower-recovery"
    Invoke-ClusterScenario -Name "Leader failover" -Mode "failover"
    Invoke-ClusterScenario -Name "No quorum rejects writes" -Mode "no-quorum"
    Invoke-ClusterScenario -Name "Snapshot and full restart recovery" -Mode "restart-recovery" -ExtraArguments @(
        "--clients=32",
        "--messages=128",
        "--resources=64",
        "--concurrency=64",
        "--lock-batch=8",
        "--snapshot-threshold=250"
    )
}
finally {
    $results | Set-Content -LiteralPath $summaryPath
	$env:GOCACHE = $previousGoCache
    Pop-Location
    Write-Host "`nCorrectness summary: $summaryPath" -ForegroundColor Yellow
    $results | ForEach-Object { Write-Host $_ }
}
