#!/usr/bin/env pwsh
# Preflight for `up`: can this stack start here, right now? Read-only; changes nothing.
#
# Refusals, each with its own code (exit 1 if any fires):
#   DOCTOR-NO-DOCKER     the Docker daemon is not answering
#   DOCTOR-PORT-FOREIGN  a port this project claims is held by something that is not ours
#   DOCTOR-RAM-SHORT     Docker memory minus what foreign containers MAY take is below our budget
#   DOCTOR-PATH-LONG     the repo path is so long that .venv DLLs pass MAX_PATH (gotcha #6)
#
# Foreign containers are counted at their mem_limit (what they are allowed to take), not their
# live use: a stack using 7 GB now may grow to its 20 GB of limits, and two stacks that each
# looked small at start-up would then run the WSL VM out of memory. A container without a limit
# is counted at its live use. Ported from olist-lakehouse, which counts live use only.
#
# A port held by THIS project's own container is fine (the stack is already up), so the holder
# is resolved rather than just "is the port busy?". Keep this file ASCII-only (PowerShell 5.1).

[CmdletBinding()]
param(
    [int]$BudgetGB = 14
)

$ErrorActionPreference = 'Stop'
# COMPOSE_PROJECT_NAME overrides compose.yaml's `name:` (the clean-clone demo runs as its own project).
$Project = if ($env:COMPOSE_PROJECT_NAME) { $env:COMPOSE_PROJECT_NAME } else { 'ghflow' }
# The 3xxxx port family (CLAUDE.md fleet ledger). Later stories' ports are claimed now, so a
# collision shows up on day one rather than in M4.
$Ports = [ordered]@{
    'airflow' = 38082; 's3-api' = 39000; 's3-ui' = 39001; 'postgres' = 35432
    'redpanda' = 39092; 'schema-registry' = 38081; 'console' = 38080
    'clickhouse' = 38123; 'grafana' = 33000; 'dashboard' = 33001
}

$refusals = New-Object System.Collections.Generic.List[string]

function Invoke-Docker {
    param([string[]]$Arguments)
    $ErrorActionPreference = 'Continue'
    $out = & docker @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) { return $null }
    return $out
}

function ConvertTo-Bytes {
    param([string]$Text)
    if ($Text -notmatch '^\s*([\d.]+)\s*([KMGT]?i?B)\s*$') { return 0 }
    $n = [double]$Matches[1]
    $unit = $Matches[2]
    # Hashtable keys are case-insensitive, so 'kB' also covers 'KB'.
    $scale = @{ 'B' = 1; 'KiB' = 1KB; 'kB' = 1000; 'MiB' = 1MB; 'MB' = 1000000
                'GiB' = 1GB; 'GB' = 1000000000; 'TiB' = 1TB }
    if (-not $scale.ContainsKey($unit)) { return 0 }
    return $n * $scale[$unit]
}

# 0. Path -----------------------------------------------------------------------------------
$repoRoot = Split-Path -Parent $PSScriptRoot
if ($repoRoot.Length -gt 150) {
    $refusals.Add("DOCTOR-PATH-LONG: the repo path is $($repoRoot.Length) characters (max 150); clone it somewhere shorter")
}

# 1. Docker ---------------------------------------------------------------------------------
$memTotal = Invoke-Docker @('info', '--format', '{{.MemTotal}}')
if (-not $memTotal) {
    Write-Host 'DOCTOR-NO-DOCKER: the Docker daemon is not answering. Start Docker Desktop.' -ForegroundColor Red
    exit 1
}
$memTotal = [double]$memTotal

# `{{.Labels}}`, not `{{.Label "..."}}`: Windows PowerShell 5.1 strips the inner quotes (gotcha #10).
$containers = @(Invoke-Docker @('ps', '--format', '{{.ID}}|{{.Names}}|{{.Labels}}|{{.Ports}}') |
    Where-Object { $_ } |
    ForEach-Object {
        $id, $name, $labels, $portText = $_ -split '\|', 4
        $proj = if ($labels -match '(?:^|,)com\.docker\.compose\.project=([^,]*)') { $Matches[1] } else { '' }
        [pscustomobject]@{ Id = $id; Name = $name; Project = $proj; Ports = $portText }
    })

# 2. Ports ----------------------------------------------------------------------------------
Write-Host ''
Write-Host '  ports' -ForegroundColor Cyan
foreach ($entry in $Ports.GetEnumerator()) {
    $port = $entry.Value
    $holder = $containers | Where-Object { $_.Ports -match ":$port->" } | Select-Object -First 1
    if ($holder) {
        if ($holder.Project -eq $Project) {
            '    {0,-16} {1,5}  ours ({2})' -f $entry.Key, $port, $holder.Name | Write-Host
        } else {
            $label = if ($holder.Project) { "project $($holder.Project)" } else { 'no compose project' }
            $refusals.Add("DOCTOR-PORT-FOREIGN: $port ($($entry.Key)) is held by container $($holder.Name), $label")
            '    {0,-16} {1,5}  FOREIGN container {2} ({3})' -f $entry.Key, $port, $holder.Name, $label | Write-Host -ForegroundColor Red
        }
        continue
    }
    $listener = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listener) {
        $proc = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
        $procName = if ($proc) { $proc.ProcessName } else { "pid $($listener.OwningProcess)" }
        $refusals.Add("DOCTOR-PORT-FOREIGN: $port ($($entry.Key)) is held by host process $procName")
        '    {0,-16} {1,5}  FOREIGN process {2}' -f $entry.Key, $port, $procName | Write-Host -ForegroundColor Red
    } else {
        '    {0,-16} {1,5}  free' -f $entry.Key, $port | Write-Host
    }
}

# 3. Memory ---------------------------------------------------------------------------------
$foreign = @($containers | Where-Object { $_.Project -ne $Project })
$usage = @{}
if ($foreign.Count -gt 0) {
    foreach ($line in @(Invoke-Docker @('stats', '--no-stream', '--format', '{{.Name}}|{{.MemUsage}}') | Where-Object { $_ })) {
        $name, $mem = $line -split '\|', 2
        $usage[$name] = ConvertTo-Bytes (($mem -split '/')[0])
    }
    $limits = @{}
    foreach ($line in @(Invoke-Docker (@('inspect', '--format', '{{.Name}}|{{.HostConfig.Memory}}') + @($foreign | ForEach-Object { $_.Id })) | Where-Object { $_ })) {
        $name, $limit = $line -split '\|', 2
        $limits[$name.TrimStart('/')] = [double]$limit
    }
}
$foreignBytes = 0.0
$foreignStacks = @{}
foreach ($c in $foreign) {
    $limit = [double]$limits[$c.Name]
    $counted = if ($limit -gt 0) { $limit } else { [double]$usage[$c.Name] }
    $foreignBytes += $counted
    $stack = if ($c.Project) { $c.Project } else { $c.Name }
    $foreignStacks[$stack] = [double]($foreignStacks[$stack]) + $counted
}
$availableGB = ($memTotal - $foreignBytes) / 1GB

Write-Host ''
Write-Host '  memory (foreign containers at their mem_limit)' -ForegroundColor Cyan
'    docker total      {0,6:N1} GB' -f ($memTotal / 1GB) | Write-Host
foreach ($s in $foreignStacks.GetEnumerator()) {
    '    foreign {0,-10} {1,6:N1} GB' -f $s.Key, ($s.Value / 1GB) | Write-Host
}
'    available         {0,6:N1} GB   (budget {1} GB)' -f $availableGB, $BudgetGB | Write-Host
if ($availableGB -lt $BudgetGB) {
    $refusals.Add(('DOCTOR-RAM-SHORT: {0:N1} GB left after foreign containers'' limits, budget is {1} GB; stop other stacks first' -f $availableGB, $BudgetGB))
}

# Verdict -----------------------------------------------------------------------------------
Write-Host ''
if ($refusals.Count -gt 0) {
    $refusals | ForEach-Object { Write-Host $_ -ForegroundColor Red }
    exit 1
}
Write-Host 'doctor: no port or memory conflicts' -ForegroundColor Green
exit 0
