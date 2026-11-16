<#
.SYNOPSIS
    OneGate 本地开发环境检查、Schema 初始化与引导启动脚本 (无本机硬编码路径)
.DESCRIPTION
    1. 自动探测环境中的 Java / Maven / MySQL 客户端工具；
    2. 检测 MySQL 连通性并幂等执行 sql/schema.sql；
    3. 检测 Redis (6379) 与 RabbitMQ (5672)，支持 -StartDocker 一键拉起 docker-compose；
    4. 支持 -StartApp 以 dev Profile (--spring.profiles.active=dev) 启动网关服务。
#>
param(
    [string]$MysqlHost = $(if ($env:MYSQL_HOST) { $env:MYSQL_HOST } else { "127.0.0.1" }),
    [int]$MysqlPort = $(if ($env:MYSQL_PORT) { [int]$env:MYSQL_PORT } else { 3306 }),
    [string]$MysqlUser = $(if ($env:SPRING_DATASOURCE_USERNAME) { $env:SPRING_DATASOURCE_USERNAME } else { "root" }),
    [string]$MysqlPassword = $(if ($env:SPRING_DATASOURCE_PASSWORD) { $env:SPRING_DATASOURCE_PASSWORD } else { "" }),
    [string]$RedisHost = $(if ($env:REDIS_HOST) { $env:REDIS_HOST } else { "127.0.0.1" }),
    [int]$RedisPort = $(if ($env:REDIS_PORT) { [int]$env:REDIS_PORT } else { 6379 }),
    [string]$RabbitHost = $(if ($env:RABBITMQ_HOST) { $env:RABBITMQ_HOST } else { "127.0.0.1" }),
    [int]$RabbitPort = $(if ($env:RABBITMQ_PORT) { [int]$env:RABBITMQ_PORT } else { 5672 }),
    [string]$Profile = "dev",
    [switch]$StartDocker,
    [switch]$StartApp,
    [switch]$SkipSchemaInit
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$SchemaPath = Join-Path $ProjectRoot "sql\schema.sql"

function Write-Step([string]$Msg) {
    Write-Host "`n==> $Msg" -ForegroundColor Cyan
}

function Write-Ok([string]$Msg) {
    Write-Host "  [OK] $Msg" -ForegroundColor Green
}

function Write-WarnMsg([string]$Msg) {
    Write-Host "  [WARN] $Msg" -ForegroundColor Yellow
}

function Resolve-ToolCommand([string]$PreferredName, [string]$EnvVarPath = "") {
    if (-not [string]::IsNullOrWhiteSpace($EnvVarPath) -and (Test-Path $EnvVarPath)) {
        return $EnvVarPath
    }
    $cmd = Get-Command $PreferredName -ErrorAction SilentlyContinue
    if ($null -ne $cmd) {
        return $cmd.Source
    }
    return $null
}

function Test-TcpPort([string]$TargetHost, [int]$Port) {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect($TargetHost, $Port, $null, $null)
        $connected = $async.AsyncWaitHandle.WaitOne(1200, $false)
        if ($connected -and $client.Connected) {
            $client.EndConnect($async)
            return $true
        }
        return $false
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

Write-Host "============================================================" -ForegroundColor DarkCyan
Write-Host "  OneGate High-Concurrency LLM API Gateway - Dev Bootstrap" -ForegroundColor White
Write-Host "============================================================" -ForegroundColor DarkCyan

# 1. Java & Maven 工具链检查
Write-Step "1. Checking Java & Maven toolchain"
if (-not [string]::IsNullOrWhiteSpace($env:JAVA_HOME)) {
    Write-Ok "JAVA_HOME = $env:JAVA_HOME"
} else {
    $javaCmd = Resolve-ToolCommand -PreferredName "java"
    if ($null -eq $javaCmd) {
        throw "Java runtime not found. Please install JDK 17+ and configure JAVA_HOME / PATH."
    }
    Write-Ok "java = $javaCmd"
}

$mvnwCmd = Join-Path $ProjectRoot "mvnw.cmd"
$MavenCmd = if (Test-Path $mvnwCmd) { $mvnwCmd } else { Resolve-ToolCommand -PreferredName "mvn" -EnvVarPath $env:MAVEN_BIN }
if ($null -eq $MavenCmd) {
    throw "Maven executable not found in PATH (and mvnw.cmd not present). Please install Maven 3.9+ or set MAVEN_BIN."
}
Write-Ok "Maven = $MavenCmd"

# 2. Docker 可选一键拉起中间件
if ($StartDocker) {
    Write-Step "2. Starting middleware stack via docker compose"
    Push-Location $ProjectRoot
    try {
        docker compose up -d
        Start-Sleep -Seconds 5
    } finally {
        Pop-Location
    }
}

# 3. MySQL 连通性检测与幂等 Schema 初始化
Write-Step "3. Checking MySQL ($MysqlHost`:$MysqlPort) & initializing sql/schema.sql"
$mysqlOnline = Test-TcpPort -TargetHost $MysqlHost -Port $MysqlPort
if (-not $mysqlOnline) {
    Write-WarnMsg "MySQL port $MysqlPort on $MysqlHost is not reachable."
    Write-Host "  Hint: run 'docker compose up -d mysql' or start your local MySQL 8.0/9.0 service." -ForegroundColor Gray
    exit 1
}
Write-Ok "MySQL TCP $MysqlHost`:$MysqlPort is online."

if (-not $SkipSchemaInit) {
    if (-not (Test-Path $SchemaPath)) {
        throw "Schema file not found: $SchemaPath"
    }
    $MysqlExe = Resolve-ToolCommand -PreferredName "mysql" -EnvVarPath $env:MYSQL_BIN
    if ($null -ne $MysqlExe) {
        $mysqlArgs = @("-h", $MysqlHost, "-P", $MysqlPort.ToString(), "-u", $MysqlUser, "--default-character-set=utf8mb4")
        if (-not [string]::IsNullOrWhiteSpace($MysqlPassword)) {
            $mysqlArgs += "-p$MysqlPassword"
        }
        Get-Content -Raw -Encoding UTF8 $SchemaPath | & $MysqlExe @mysqlArgs
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to apply sql/schema.sql via $MysqlExe"
        }
        Write-Ok "Idempotent schema initialized (database: one_gate)."
    } else {
        Write-WarnMsg "Local 'mysql' CLI not found in PATH; skipping CLI schema import. (If using Docker MySQL, /docker-entrypoint-initdb.d already auto-initializes schema.sql)."
    }
}

# 4. Redis & RabbitMQ 端口检测
Write-Step "4. Checking Redis ($RedisHost`:$RedisPort) & RabbitMQ ($RabbitHost`:$RabbitPort)"
$redisOnline = Test-TcpPort -TargetHost $RedisHost -Port $RedisPort
$rabbitOnline = Test-TcpPort -TargetHost $RabbitHost -Port $RabbitPort

if ($redisOnline) {
    Write-Ok "Redis TCP $RedisHost`:$RedisPort is online."
} else {
    Write-WarnMsg "Redis TCP $RedisHost`:$RedisPort is OFFLINE. Run: docker compose up -d redis"
}

if ($rabbitOnline) {
    Write-Ok "RabbitMQ TCP $RabbitHost`:$RabbitPort is online (Management UI: http://localhost:15672)."
} else {
    Write-WarnMsg "RabbitMQ TCP $RabbitHost`:$RabbitPort is OFFLINE. Run: docker compose up -d rabbitmq"
}

# 5. 安全提示与启动指引
Write-Step "5. Security check & launch guide"
if ([string]::IsNullOrWhiteSpace($env:ONEGATE_ADMIN_TOKEN)) {
    if ($Profile -eq "dev") {
        Write-WarnMsg "ONEGATE_ADMIN_TOKEN is unset; dev profile will use fallback dev token (set `$env:ONEGATE_ADMIN_TOKEN for custom value)."
    } else {
        Write-WarnMsg "ONEGATE_ADMIN_TOKEN is unset! Non-dev profiles reject all /admin/** calls until ONEGATE_ADMIN_TOKEN is configured."
    }
} else {
    Write-Ok "ONEGATE_ADMIN_TOKEN is configured via environment."
}

Write-Host "  Project Root : $ProjectRoot" -ForegroundColor Gray
Write-Host "  Run Gateway  : & `"$MavenCmd`" spring-boot:run `"-Dspring-boot.run.profiles=$Profile`"" -ForegroundColor Gray
Write-Host "  Run Tests    : & `"$MavenCmd`" test" -ForegroundColor Gray

if ($StartApp) {
    if (-not $redisOnline -or -not $rabbitOnline) {
        Write-WarnMsg "Redis or RabbitMQ is offline; full application startup requires both middleware services."
        exit 1
    }
    Push-Location $ProjectRoot
    try {
        & $MavenCmd spring-boot:run "-Dspring-boot.run.profiles=$Profile"
    } finally {
        Pop-Location
    }
}
