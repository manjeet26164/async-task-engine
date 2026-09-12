# ==============================================================================
# TaskFlow High-Concurrency Load Simulation Script (PowerShell)
# Spawns 200 concurrent HTTP POST requests to /api/v1/jobs/submit
# Demonstrates Thread Pool saturation, Redis Queue buffering & CallerRuns backpressure
# ==============================================================================

param(
    [string]$TargetUrl = "http://localhost:8080/api/v1/jobs/submit",
    [string]$ApiKey = "taskflow-secret-key-2026",
    [int]$TotalRequests = 200,
    [int]$Concurrency = 50
)

Write-Host "`n========================================================" -ForegroundColor Cyan
Write-Host " 🚀 TaskFlow Concurrency Load Simulator (200 Tasks)" -ForegroundColor Yellow
Write-Host " Target URL:    $TargetUrl" -ForegroundColor Gray
Write-Host " Total Tasks:   $TotalRequests" -ForegroundColor Gray
Write-Host "========================================================`n" -ForegroundColor Cyan

Add-Type -AssemblyName System.Net.Http

$clientHandler = New-Object System.Net.Http.HttpClientHandler
$httpClient = New-Object System.Net.Http.HttpClient($clientHandler)
$httpClient.Timeout = [TimeSpan]::FromSeconds(30)

$taskTypes = @("IMAGE_PROCESSING", "SEND_EMAIL", "PAYMENT_GATEWAY", "DATA_SYNC", "DOCUMENT_OCR")
$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

$tasks = New-Object System.Collections.Generic.List[System.Threading.Tasks.Task]
$acceptedCount = 0
$conflictCount = 0
$errorCount = 0
$lockObj = New-Object object

Write-Host "Dispatching $TotalRequests concurrent job submissions..." -ForegroundColor Green

for ($i = 1; $i -le $TotalRequests; $i++) {
    $taskType = $taskTypes[$i % $taskTypes.Length]
    $isFailureTest = ($i % 15 -eq 0) # 6-7% of tasks simulate failure for DLQ
    $idempKey = "load-test-$(New-Guid)"
    
    $payloadObj = @{
        taskIndex = $i
        batchId = "batch-load-test-2026"
        timestamp = (Get-Date).ToString("o")
        fail = $isFailureTest
    }
    $payloadJson = $payloadObj | ConvertTo-Json -Compress

    $requestBodyObj = @{
        taskType = $taskType
        payload = $payloadJson
    }
    $requestBodyJson = $requestBodyObj | ConvertTo-Json -Compress

    $httpRequest = New-Object System.Net.Http.HttpRequestMessage([System.Net.Http.HttpMethod]::Post, $TargetUrl)
    $httpRequest.Headers.Add("Idempotency-Key", $idempKey)
    $httpRequest.Headers.Add("X-API-KEY", $ApiKey)
    $httpRequest.Content = New-Object System.Net.Http.StringContent($requestBodyJson, [System.Text.Encoding]::UTF8, "application/json")

    $asyncTask = $httpClient.SendAsync($httpRequest).ContinueWith([Action[System.Threading.Tasks.Task[System.Net.Http.HttpResponseMessage]]]{
        param($t)
        try {
            if ($t.IsFaulted -or $t.IsCanceled) {
                [System.Threading.Monitor]::Enter($lockObj)
                $script:errorCount++
                [System.Threading.Monitor]::Exit($lockObj)
            } else {
                $status = [int]$t.Result.StatusCode
                [System.Threading.Monitor]::Enter($lockObj)
                if ($status -eq 202) {
                    $script:acceptedCount++
                } elseif ($status -eq 409) {
                    $script:conflictCount++
                } else {
                    $script:errorCount++
                }
                [System.Threading.Monitor]::Exit($lockObj)
            }
        } catch {
            [System.Threading.Monitor]::Enter($lockObj)
            $script:errorCount++
            [System.Threading.Monitor]::Exit($lockObj)
        }
    })

    $tasks.Add($asyncTask)
}

[System.Threading.Tasks.Task]::WaitAll($tasks.ToArray())
$stopwatch.Stop()

$elapsedSec = [Math]::Round($stopwatch.Elapsed.TotalSeconds, 2)
$throughput = [Math]::Round($TotalRequests / [Math]::Max(0.01, $stopwatch.Elapsed.TotalSeconds), 1)

Write-Host "`n================ Load Test Summary ================" -ForegroundColor Cyan
Write-Host " Total Requests Dispatched: $TotalRequests" -ForegroundColor White
Write-Host " HTTP 202 Accepted:        $acceptedCount" -ForegroundColor Green
Write-Host " HTTP 409 Conflict:        $conflictCount" -ForegroundColor Yellow
Write-Host " Other / Errors:           $errorCount" -ForegroundColor Red
Write-Host " Elapsed Time:             $elapsedSec seconds" -ForegroundColor White
Write-Host " Ingestion Throughput:     $throughput req/sec" -ForegroundColor Yellow
Write-Host "====================================================`n" -ForegroundColor Cyan
Write-Host "✨ Check the real-time Dashboard at: http://localhost:8080/index.html to observe queue draining and worker thread saturation!" -ForegroundColor Green

$httpClient.Dispose()
