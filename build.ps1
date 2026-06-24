# build_nopthread.ps1
Write-Host "=== Building Kafka Clone on Windows (No pthread) ===" -ForegroundColor Green

# Create directories
Write-Host "`nCreating directories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "data" | Out-Null
New-Item -ItemType Directory -Force -Path "data\meta" | Out-Null
New-Item -ItemType Directory -Force -Path "data\offsets" | Out-Null

# Kill existing process if running
Get-Process -Name broker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Get-Process -Name kafka_broker -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue

# Delete old exe if exists
Remove-Item -Force *.exe -ErrorAction SilentlyContinue

# Check if g++ exists
$gpp = (Get-Command g++ -ErrorAction SilentlyContinue)
if (-not $gpp) {
    Write-Host "ERROR: g++ not found! Please install MinGW." -ForegroundColor Red
    exit 1
}

Write-Host "`nCompiling broker (without pthread)..." -ForegroundColor Yellow

# Compile broker WITHOUT -pthread flag
g++ -std=c++17 broker\main.cpp broker\Broker.cpp broker\TopicManager.cpp broker\ConsumerGroup.cpp broker\GroupManager.cpp -o kafka_broker.exe -lws2_32

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Broker compiled successfully as kafka_broker.exe!" -ForegroundColor Green
} else {
    Write-Host "❌ Broker compilation failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`nCompiling test (without pthread)..." -ForegroundColor Yellow

# Compile test WITHOUT -pthread flag
g++ -std=c++17 tests\test_consumer_groups.cpp broker\TopicManager.cpp broker\ConsumerGroup.cpp broker\GroupManager.cpp -o test_consumer_groups.exe -lws2_32

if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Test compiled successfully!" -ForegroundColor Green
} else {
    Write-Host "❌ Test compilation failed!" -ForegroundColor Red
    exit 1
}

Write-Host "`n=== Build Complete ===" -ForegroundColor Green
Write-Host "Run broker: .\kafka_broker.exe" -ForegroundColor Cyan
Write-Host "Run tests: .\test_consumer_groups.exe" -ForegroundColor Cyan