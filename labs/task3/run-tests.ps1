$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$jmeterDir = Join-Path $root "jmeter"
$resultsDir = Join-Path $root "results"
$summary = Join-Path $resultsDir "summary.csv"

if (-not (Test-Path $resultsDir)) { New-Item -ItemType Directory -Path $resultsDir | Out-Null }

$threads = 20
$duration = 60

$modes = @(
    @{ name = "lazy";          host = "app-lazy"; hostPort = 8001 },
    @{ name = "write_through"; host = "app-wt";   hostPort = 8002 },
    @{ name = "write_back";    host = "app-wb";   hostPort = 8003 }
)

$mixes = @(
    @{ name = "read-heavy"; read = 80; write = 20 },
    @{ name = "balanced";   read = 50; write = 50 },
    @{ name = "write-heavy"; read = 20; write = 80 }
)

"mode,mix,samples,errors,throughput_rps,avg_ms,p95_ms,db_reads,db_writes,cache_hits,cache_misses,hit_rate,wb_flush_batches,wb_queue_after" |
    Out-File -FilePath $summary -Encoding utf8

function Wait-Ready($url) {
    for ($i = 0; $i -lt 60; $i++) {
        try {
            $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 2
            if ($r.StatusCode -eq 200) { return }
        } catch {}
        Start-Sleep -Seconds 1
    }
    throw "service not ready: $url"
}

function Parse-Jtl($path) {
    $rows = Import-Csv -Path $path
    $samples = $rows.Count
    if ($samples -eq 0) {
        return [pscustomobject]@{ samples=0; errors=0; throughput=0; avg=0; p95=0 }
    }
    $errors = ($rows | Where-Object { $_.success -ne "true" }).Count
    $elapsed = $rows | ForEach-Object { [int]$_.elapsed }
    $avg = [math]::Round(($elapsed | Measure-Object -Average).Average, 2)
    $sorted = $elapsed | Sort-Object
    $p95idx = [math]::Floor($sorted.Count * 0.95)
    if ($p95idx -ge $sorted.Count) { $p95idx = $sorted.Count - 1 }
    $p95 = $sorted[$p95idx]
    $tsMin = ($rows | ForEach-Object { [long]$_.timeStamp } | Measure-Object -Minimum).Minimum
    $tsMax = ($rows | ForEach-Object { [long]$_.timeStamp } | Measure-Object -Maximum).Maximum
    $durSec = ($tsMax - $tsMin) / 1000.0
    if ($durSec -le 0) { $durSec = 1 }
    $tput = [math]::Round($samples / $durSec, 2)
    return [pscustomobject]@{ samples=$samples; errors=$errors; throughput=$tput; avg=$avg; p95=$p95 }
}

foreach ($m in $modes) {
    Write-Host ""
    Write-Host "=== MODE: $($m.name) (port $($m.hostPort)) ===" -ForegroundColor Cyan
    Wait-Ready "http://localhost:$($m.hostPort)/health"

    foreach ($mix in $mixes) {
        $tag = "$($m.name)__$($mix.name)"
        $jtl = "/results/$tag.jtl"
        $jtlLocal = Join-Path $resultsDir "$tag.jtl"
        if (Test-Path $jtlLocal) { Remove-Item $jtlLocal }

        Write-Host "-> $tag (read=$($mix.read)% write=$($mix.write)%)"

        Invoke-RestMethod -Uri "http://localhost:$($m.hostPort)/stats/reset" -Method Post | Out-Null

        $dockerArgs = @(
            "run", "--rm", "--network", "task3_net",
            "-v", "${jmeterDir}:/test",
            "-v", "${resultsDir}:/results",
            "justb4/jmeter:5.5",
            "-n", "-t", "/test/test-plan.jmx",
            "-Jhost=$($m.host)",
            "-Jport=8000",
            "-Jread_pct=$($mix.read)",
            "-Jwrite_pct=$($mix.write)",
            "-Jthreads=$threads",
            "-Jduration=$duration",
            "-Jjtl=$jtl",
            "-l", $jtl,
            "-j", "/results/$tag.log"
        )
        & docker @dockerArgs | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "jmeter failed for $tag" }

        if ($m.name -eq "write_back") {
            Invoke-RestMethod -Uri "http://localhost:$($m.hostPort)/admin/flush" -Method Post | Out-Null
        }

        $stats = Invoke-RestMethod -Uri "http://localhost:$($m.hostPort)/stats"
        $jr = Parse-Jtl $jtlLocal

        $line = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13}" -f `
            $m.name, $mix.name, $jr.samples, $jr.errors, $jr.throughput, $jr.avg, $jr.p95, `
            $stats.db_reads, $stats.db_writes, $stats.cache_hits, $stats.cache_misses, `
            $stats.hit_rate, $stats.wb_flush_batches, $stats.write_back_queue_size

        Add-Content -Path $summary -Value $line -Encoding utf8
        Write-Host ("   tput={0} rps  avg={1} ms  hit_rate={2}  db_reads={3}  db_writes={4}" -f `
            $jr.throughput, $jr.avg, $stats.hit_rate, $stats.db_reads, $stats.db_writes)
    }
}

Write-Host ""
Write-Host "DONE. summary: $summary" -ForegroundColor Green
Get-Content $summary
