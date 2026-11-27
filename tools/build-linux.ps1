<#
.SYNOPSIS
Builds the Linux Python 3.11 dependency lock and Function App package in a container.
.DESCRIPTION
Every pip call uses public PyPI unless an HTTPS index is supplied explicitly.
Do not put credentials in the index URL.
Lock writes functions/requirements.lock. Package audits that lock, installs it with hashes
enforced into dist/site-packages, and writes dist/functionapp.zip. Requires a running Docker daemon.
#>
#Requires -Version 7.4
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [ValidateSet('Lock', 'Package')] [string] $Step,
    [string] $IndexUrl = 'https://pypi.org/simple'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

$index = $IndexUrl.Trim()
$uri = [uri] $index
if (-not $uri.IsAbsoluteUri -or $uri.Scheme -cne 'https' -or $uri.UserInfo -or $uri.Query -or $uri.Fragment) {
    throw 'IndexUrl must be an absolute HTTPS URL without credentials, query or fragment.'
}
Write-Host "Using package index host: $($uri.Host)"

$project = Split-Path $PSScriptRoot
$commands = switch ($Step) {
    'Lock' {
        'python -m pip install --quiet pip-tools && ' +
        'python -m piptools compile --quiet --generate-hashes --allow-unsafe --strip-extras --no-emit-index-url ' +
        '--output-file functions/requirements.lock functions/requirements.txt'
    }
    'Package' {
        'python -m pip install --quiet pip-audit && ' +
        'pip-audit --strict --require-hashes --disable-pip -r functions/requirements.lock && ' +
        'rm -rf dist/site-packages && ' +
        'python -m pip install --quiet --require-hashes --no-deps --target dist/site-packages -r functions/requirements.lock && ' +
        'python tools/build_function_package.py --require-lock --site-packages dist/site-packages'
    }
}

$previous = $env:PIP_INDEX_URL
try {
    $env:PIP_INDEX_URL = $index
    # Name-only -e keeps the index URL off the process command line.
    & docker run --rm -e PIP_INDEX_URL -e PIP_DISABLE_PIP_VERSION_CHECK=1 -e PIP_ROOT_USER_ACTION=ignore `
        -v "${project}:/work" -w /work python:3.11-slim sh -c $commands
    if ($LASTEXITCODE -ne 0) { throw "$Step failed (exit $LASTEXITCODE)." }
} finally {
    $env:PIP_INDEX_URL = $previous
}
