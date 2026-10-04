#!/usr/bin/env pwsh
# The project's single task runner. GNU Make is not installed on this Windows machine and the
# project path contains a space, so every command a story documents goes through here.
# Keep this file ASCII-only: Windows PowerShell 5.1 reads a BOM-less script as ANSI.
#
# Usage:  .\make.ps1 help
#         .\make.ps1 check

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = 'help'
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Checked {
    param([string]$Exe, [string[]]$Arguments)
    # Windows PowerShell 5.1 turns a native tool's stderr into error records whenever output
    # is redirected, and 'Stop' would abort on uv's progress lines. Judge by exit code only.
    $ErrorActionPreference = 'Continue'
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Exe $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

# Verb-noun on purpose: a function named `uv` would shadow the executable (gotcha #22).
function Invoke-Uv { param([string[]]$Arguments) Invoke-Checked 'uv' $Arguments }

switch ($Target) {
    'help' {
        Write-Host ''
        Write-Host '  ghflow targets' -ForegroundColor Cyan
        Write-Host ''
        @(
            @('hooks',     'Install git hooks (refuses direct pushes to main)'),
            @('sync',      'Create/refresh the local venv from uv.lock'),
            @('lint',      'Check formatting and lint rules'),
            @('format',    'Auto-fix formatting and lint rules'),
            @('typecheck', 'mypy --strict'),
            @('test',      'Unit tests'),
            @('check',     'sync + lint + typecheck + test (what CI runs)'),
            @('profile',   'Measure GH Archive (network), write the record, render docs/data/gharchive.md'),
            @('profile-render', 'Re-render docs/data/gharchive.md from the committed record (no network)'),
            @('contract-check', 'Apply the event contract to seven real hours (network) -> automation/runs/m0-s3/')
        ) | ForEach-Object { Write-Host ('  {0,-12} {1}' -f $_[0], $_[1]) }
        Write-Host ''
    }
    'hooks'     { Invoke-Checked 'git' @('config', 'core.hooksPath', '.githooks'); Write-Host 'git hooks installed from .githooks/' }
    'sync'      { Invoke-Uv @('sync', '--locked', '--group', 'dev') }
    'lint'      { Invoke-Uv @('run', 'ruff', 'check', '.'); Invoke-Uv @('run', 'ruff', 'format', '--check', '.') }
    'format'    { Invoke-Uv @('run', 'ruff', 'check', '--fix', '.'); Invoke-Uv @('run', 'ruff', 'format', '.') }
    'typecheck' { Invoke-Uv @('run', 'mypy') }
    'test'      { Invoke-Uv @('run', 'pytest', 'tests/unit') }
    'profile'   { Invoke-Uv @('run', 'python', '-m', 'ghflow.profile', 'measure') }
    'profile-render' { Invoke-Uv @('run', 'python', '-m', 'ghflow.profile', 'render') }
    'contract-check' {
        # 2024 baseline, both sides of the trim, an outage hour, the broken-mix hour, a partial hour, a healthy hour
        $hours = @('2024-06-15-15', '2025-10-08-16', '2025-10-08-17', '2026-09-02-5', '2026-09-05-15', '2026-09-21-15', '2026-10-02-15')
        Invoke-Uv (@('run', 'python', '-m', 'ghflow.contract') + $hours + @('--record', 'automation/runs/m0-s3/real_hours.json'))
    }
    'check'     {
        & $PSCommandPath sync
        & $PSCommandPath lint
        & $PSCommandPath typecheck
        & $PSCommandPath test
        Write-Host 'check: lint, types and unit tests all passed' -ForegroundColor Green
    }
    default {
        Write-Host "Unknown target '$Target'. Run .\make.ps1 help" -ForegroundColor Red
        exit 2
    }
}
