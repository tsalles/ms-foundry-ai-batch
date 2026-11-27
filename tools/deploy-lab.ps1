<#
.SYNOPSIS
Plans and optionally deploys the Batch lab infrastructure from a Windows desktop.
.DESCRIPTION
Use the approved public egress IPv4 of the desktop's current network or VPN, without a CIDR suffix.
Use Terraform 1.11+, Python 3.11+ with the local project installed, and an Azure CLI user with
deployment and role assignment permissions. Use the user's approved Entra object ID for state access.
By default no Azure resources or state are written. A fresh lab can only plan bootstrap until it exists.
With Apply, confirm each reviewed saved plan separately. Supply the recorded G1 approval reference.
The state account sits behind a network security perimeter (the tenant policy forces public access off
otherwise) that admits only the desktop IPv4 and, once the runner exists, a private endpoint. With
RunnerSshPublicKeyPath the workload also creates the private runner, then two more separately reviewed
plans grant it read access to the artifact container, the state private endpoint and its registration
roles. This script does not deploy app code, register webhooks, submit batches, or tear down resources.
Plans and Terraform state are kept under LOCALAPPDATA, outside the source checkout. Treat them as sensitive.
.EXAMPLE
./tools/deploy-lab.ps1 -DesktopPublicIp $ApprovedIPv4 -StateContributorObjectId $ApprovedObjectId `
    -ModelName $ApprovedModel -ModelVersion $ApprovedVersion -ModelCapacity $ApprovedCapacity `
    -DynamicQuota $ApprovedDynamicQuota
Add -Apply -ApprovalReference $RecordedG1Approval to enter the reviewed apply flow.
#>

#Requires -Version 7.4
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $DesktopPublicIp,
    [guid] $StateContributorObjectId,
    [ValidateScript({ $_ -ne [guid]::Empty })] [guid] $RegistrationPrincipalObjectId,
    [string] $RunnerSshPublicKeyPath,
    [Parameter(Mandatory)] [ValidatePattern('^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$')] [string] $ModelName,
    [Parameter(Mandatory)] [ValidatePattern('^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$')] [string] $ModelVersion,
    [Parameter(Mandatory)] [ValidateRange(1, [int]::MaxValue)] [int] $ModelCapacity,
    [Parameter(Mandatory)] [ValidateSet('enabled', 'disabled')] [string] $DynamicQuota,
    [ValidatePattern('^rg-[a-z0-9][a-z0-9-]{0,85}$')] [string] $ResourceGroupName = 'rg-foundry-batch-lab',
    [guid] $TenantId = [guid]::Empty,
    [guid] $SubscriptionId = [guid]::Empty,
    [string] $Owner = $env:BATCH_OWNER,
    [string] $CostCenter = $env:BATCH_COST_CENTER,
    [string] $TerraformPath = 'terraform',
    [string] $ApprovalReference,
    [switch] $CheckOnly,
    [switch] $Apply
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

if (-not $PSBoundParameters.ContainsKey('TenantId') -and $env:TF_VAR_tenant_id) {
    $TenantId = [guid] $env:TF_VAR_tenant_id
}
if (-not $PSBoundParameters.ContainsKey('SubscriptionId') -and $env:TF_VAR_subscription_id) {
    $SubscriptionId = [guid] $env:TF_VAR_subscription_id
}
if ([string]::IsNullOrWhiteSpace($Owner) -or [string]::IsNullOrWhiteSpace($CostCenter) -or
    $Owner -match '(?i)^replace' -or $CostCenter -match '(?i)^replace') {
    throw 'Supply Owner and CostCenter parameters or BATCH_OWNER and BATCH_COST_CENTER environment variables.'
}

$workloadEnvironment = if ($ResourceGroupName -ieq 'rg-foundry-batch-poc') { 'poc' } else { 'lab' }
if ($DesktopPublicIp -notmatch '^(0|[1-9][0-9]{0,2})(\.(0|[1-9][0-9]{0,2})){3}$') {
    throw 'DesktopPublicIp must be one bare, public IPv4 address (no CIDR suffix).'
}
$octets = [int[]] ($DesktopPublicIp.Split('.'))
if (($octets | Where-Object { $_ -gt 255 }) -or $octets[0] -in @(0, 10, 127) -or $octets[0] -ge 224 -or
    ($octets[0] -eq 100 -and $octets[1] -in (64..127)) -or
    ($octets[0] -eq 169 -and $octets[1] -eq 254) -or
    ($octets[0] -eq 172 -and $octets[1] -in (16..31)) -or
    ($octets[0] -eq 192 -and ($octets[1] -eq 168 -or ($octets[1] -eq 0 -and $octets[2] -in @(0, 2)) -or
        ($octets[1] -eq 88 -and $octets[2] -eq 99))) -or
    ($octets[0] -eq 198 -and ($octets[1] -in @(18, 19) -or ($octets[1] -eq 51 -and $octets[2] -eq 100))) -or
    ($octets[0] -eq 203 -and $octets[1] -eq 0 -and $octets[2] -eq 113)) {
    throw 'DesktopPublicIp is not a public IPv4 address.'
}
if ([guid]::Empty -in @($TenantId, $SubscriptionId) -or
    ($PSBoundParameters.ContainsKey('StateContributorObjectId') -and $StateContributorObjectId -eq [guid]::Empty)) {
    throw 'GUID inputs cannot be empty.'
}
if ($ModelName -match 'replace' -or $ModelVersion -match 'replace') { throw 'Use an approved model and version, not placeholders.' }
if ($CheckOnly -and $Apply) { throw 'Choose CheckOnly or Apply, not both.' }
if ($Apply -and [string]::IsNullOrWhiteSpace($ApprovalReference)) {
    throw 'Apply requires the recorded G1 approval reference (D-02 and D-12).'
}
if ($RunnerSshPublicKeyPath) {
    if (-not (Test-Path -LiteralPath $RunnerSshPublicKeyPath -PathType Leaf) -or
        (Get-Content -LiteralPath $RunnerSshPublicKeyPath -Raw).Trim() -notmatch '^ssh-rsa [A-Za-z0-9+/]+={0,3}( [^\r\n]*)?$') {
        throw 'RunnerSshPublicKeyPath must be an existing one-line RSA public key (ssh-rsa); the runner module rejects other key types.'
    }
}

$terraform = (Get-Command $TerraformPath -CommandType Application | Select-Object -First 1).Source
$azure = (Get-Command az -CommandType Application | Select-Object -First 1).Source
$python = (Get-Command python -CommandType Application | Select-Object -First 1).Source
$location = 'eastus2'
$sourceInfra = Join-Path (Split-Path $PSScriptRoot) 'infra'
$runRoot = Join-Path $env:LOCALAPPDATA "FoundryBatchLab/$SubscriptionId/$ResourceGroupName"

function Invoke-Checked([string] $Executable, [string[]] $Arguments) {
    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$(Split-Path $Executable -Leaf) $($Arguments[0]) failed (exit $LASTEXITCODE)." }
}

function Get-AzureJson([string[]] $Arguments) {
    Invoke-Checked $azure ($Arguments + @('--only-show-errors', '--output', 'json')) | ConvertFrom-Json
}

function Assert-AzureTarget {
    $account = Get-AzureJson @('account', 'show')
    if ($account.id -ine "$SubscriptionId" -or $account.tenantId -ine "$TenantId" -or
        $account.state -ne 'Enabled' -or $account.environmentName -ne 'AzureCloud' -or $account.user.type -ne 'user') {
        throw "The active Azure CLI user must be on enabled AzureCloud subscription $SubscriptionId, tenant $TenantId."
    }
    $identity = Get-AzureJson @('ad', 'signed-in-user', 'show')
    $resolvedId = [guid] $identity.id
    if ($resolvedId -eq [guid]::Empty -or
        ($StateContributorObjectId -ne [guid]::Empty -and $resolvedId -ne $StateContributorObjectId)) {
        throw 'StateContributorObjectId must match the signed-in desktop user approved to access both state containers.'
    }
    $script:StateContributorObjectId = $resolvedId
}

function Assert-WorkloadPermissions([string] $Scope) {
    Assert-ArmPermissions $Scope @(
        'Microsoft.Authorization/roleAssignments/write',
        'Microsoft.CognitiveServices/accounts/write', 'Microsoft.CognitiveServices/accounts/deployments/write',
        'Microsoft.CognitiveServices/accounts/privateEndpointConnectionsApproval/action',
        'Microsoft.Insights/actionGroups/write', 'Microsoft.Insights/components/write',
        'Microsoft.Insights/diagnosticSettings/write', 'Microsoft.Insights/privateLinkScopes/write',
        'Microsoft.Insights/privateLinkScopes/scopedResources/write', 'Microsoft.Insights/scheduledQueryRules/write',
        'Microsoft.Insights/workbooks/write', 'Microsoft.KeyVault/vaults/write',
        'Microsoft.ManagedIdentity/userAssignedIdentities/write', 'Microsoft.ManagedIdentity/userAssignedIdentities/assign/action',
        'Microsoft.Network/networkSecurityGroups/write', 'Microsoft.Network/networkSecurityGroups/securityRules/write',
        'Microsoft.Network/privateDnsZones/write', 'Microsoft.Network/privateDnsZones/virtualNetworkLinks/write',
        'Microsoft.Network/privateEndpoints/write', 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups/write',
        'Microsoft.Network/natGateways/write', 'Microsoft.Network/publicIPAddresses/write',
        'Microsoft.Network/networkInterfaces/write', 'Microsoft.Compute/virtualMachines/write',
        'Microsoft.Network/routeTables/write', 'Microsoft.Network/virtualNetworks/write',
        'Microsoft.Network/virtualNetworks/subnets/write', 'Microsoft.Network/virtualNetworks/subnets/join/action',
        'Microsoft.OperationalInsights/workspaces/write',
        'Microsoft.Storage/storageAccounts/write', 'Microsoft.Storage/storageAccounts/blobServices/write',
        'Microsoft.Storage/storageAccounts/blobServices/containers/write',
        'Microsoft.Storage/storageAccounts/queueServices/queues/write', 'Microsoft.Storage/storageAccounts/tableServices/tables/write',
        'Microsoft.Web/serverfarms/write', 'Microsoft.Web/sites/write', 'Microsoft.Web/sites/basicPublishingCredentialsPolicies/write'
    )
}
function Assert-ArmPermissions([string] $Scope, [string[]] $Actions) {
    $path = "$Scope/providers/Microsoft.Authorization/permissions"
    $uri = "https://management.azure.com${path}?api-version=2022-04-01"
    $visited = [Collections.Generic.HashSet[string]]::new()
    $grants = @(while ($uri) {
        $pageUri = [uri] $uri
        if ($pageUri.Scheme -cne 'https' -or $pageUri.Host -cne 'management.azure.com' -or
            -not $pageUri.IsDefaultPort -or $pageUri.UserInfo -or $pageUri.AbsolutePath -ine $path -or
            $visited.Count -ge 100 -or -not $visited.Add($uri)) {
            throw 'Invalid ARM permissions paging link.'
        }
        $page = Get-AzureJson @('rest', '--method', 'get', '--url', $uri, '--subscription', "$SubscriptionId")
        if ($null -eq $page -or $page.value -isnot [array]) { throw 'Invalid ARM permissions response.' }
        foreach ($grant in $page.value) {
            if ($grant.actions -isnot [array] -or $grant.notActions -isnot [array]) {
                throw 'Invalid ARM permissions grant.'
            }
            $grant
        }
        $uri = if ($page.PSObject.Properties['nextLink']) { $page.nextLink } else { $null }
    })
    $missing = @(foreach ($action in $Actions) {
        $allowed = $false
        foreach ($grant in $grants) {
            $included = @($grant.actions | Where-Object { $action -ilike $_ }).Count -gt 0
            $excluded = @($grant.notActions | Where-Object { $action -ilike $_ }).Count -gt 0
            if ($included -and -not $excluded) { $allowed = $true; break }
        }
        if (-not $allowed) { $action }
    })
    if ($missing.Count) { throw "Signed-in user is missing ARM actions at $Scope`: $($missing -join ', ')." }
    Write-Host 'ARM permission check passed for the requested actions.'
}

function Write-JsonFile([string] $Path, [System.Collections.IDictionary] $Values) {
    [IO.File]::WriteAllText($Path, ($Values | ConvertTo-Json -Depth 20))
}

function Copy-TerraformConfiguration {
    foreach ($directory in @('bootstrap', 'envs/poc') + @(Get-ChildItem (Join-Path $sourceInfra 'modules') -Directory | ForEach-Object { "modules/$($_.Name)" })) {
        $target = Join-Path $runRoot $directory
        $null = New-Item -ItemType Directory -Force -Path $target
        Get-ChildItem (Join-Path $sourceInfra $directory) -File -Force |
            Where-Object { $_.Extension -eq '.tf' -or $_.Name -eq '.terraform.lock.hcl' } |
            Copy-Item -Destination $target
    }
}

function Invoke-ReviewedPlan([string] $Root, [string] $Stage, [System.Collections.IDictionary] $Variables) {
    $variablesPath = Join-Path $Root 'lab.tfvars.json'
    $plan = Join-Path $Root "$Stage.tfplan"
    $json = Join-Path $Root "$Stage.plan.json"
    Write-JsonFile $variablesPath $Variables
    $terraformRoot = "-chdir=$Root"
    Invoke-Checked $terraform @($terraformRoot, 'validate', '-no-color') | Out-Host
    Invoke-Checked $terraform @($terraformRoot, 'plan', '-input=false', '-no-color', '-lock-timeout=60s', "-var-file=$variablesPath", "-out=$plan") | Out-Null
    $hash = (Get-FileHash -LiteralPath $plan -Algorithm SHA256).Hash
    try {
        $planJson = Invoke-Checked $terraform @($terraformRoot, 'show', '-json', $plan)
        [IO.File]::WriteAllText($json, ($planJson -join "`n"))
        Invoke-Checked $python @((Join-Path $PSScriptRoot 'secret_scan.py'), '--strict', $json) | Out-Host
        Invoke-Checked $python @((Join-Path $PSScriptRoot 'plan_guard.py'), $json, '--resource-group', $ResourceGroupName,
            '--subscription-id', "$SubscriptionId", '--allowed-location', $location) | Out-Host
    } finally {
        if (Test-Path -LiteralPath $json) { Remove-Item -LiteralPath $json }
    }
    Invoke-Checked $terraform @($terraformRoot, 'show', '-no-color', $plan) | Out-Host
    Write-Host "Saved $Stage plan: $plan (SHA256: $hash)"
    if (-not $Apply) { return $false }
    $confirmation = "apply $Stage $ResourceGroupName"
    if ((Read-Host "After independent review and approval, type '$confirmation' to apply this exact plan") -cne $confirmation) {
        Write-Host 'Plan left unapplied.'
        return $false
    }
    Assert-AzureTarget
    if ((Get-FileHash -LiteralPath $plan -Algorithm SHA256).Hash -cne $hash) { throw 'Saved plan changed after review.' }
    if ($Stage -eq 'bootstrap' -and -not $groupExists -and
        (Get-AzureJson @('group', 'exists', '--name', $ResourceGroupName, '--subscription', "$SubscriptionId")) -ne $false) {
        throw 'The target group appeared after planning. No apply or adoption was attempted.'
    }
    Invoke-Checked $terraform @($terraformRoot, 'apply', '-input=false', '-no-color', '-lock-timeout=60s', $plan) | Out-Host
    return $true
}

function Get-BootstrapOutputs {
    Invoke-Checked $terraform @("-chdir=$bootstrap", 'output', '-json') | ConvertFrom-Json
}

function Test-StateBlob([string] $Account, [string] $Container, [string] $Key) {
    $result = Get-AzureJson @('storage', 'blob', 'exists', '--account-name', $Account, '--container-name', $Container,
        '--name', $Key, '--auth-mode', 'login', '--subscription', "$SubscriptionId")
    if ($result.exists -isnot [bool]) { throw 'Azure CLI did not return a valid state blob existence result.' }
    return $result.exists
}

function Initialize-RemoteBackend([string] $Root, [string] $Container, [string] $Key, [string[]] $InitOptions) {
    $backendConfig = Join-Path $Root 'lab.backend.hcl'
    [IO.File]::WriteAllLines($backendConfig, @(
        "resource_group_name = `"$ResourceGroupName`"", "storage_account_name = `"$($state.account)`"",
        "container_name = `"$Container`"", "key = `"$Key`"", "subscription_id = `"$SubscriptionId`"",
        "tenant_id = `"$TenantId`"", 'use_azuread_auth = true', 'use_cli = true'
    ))
    Invoke-Checked $terraform (@("-chdir=$Root", 'init', '-no-color', '-lockfile=readonly', "-backend-config=$backendConfig") + $InitOptions) | Out-Host
}

function Assert-BootstrapOutputs($Outputs, [string] $Account) {
    if ($Outputs.resource_group_name.value -cne $ResourceGroupName -or
        $Outputs.state_storage_account_name.value -cne $Account -or
        $Outputs.bootstrap_state_container_name.value -cne 'tfstate-bootstrap' -or
        $Outputs.state_container_name.value -cne 'tfstate' -or
        $Outputs.state_public_network_access_enabled.value -ne $true) {
        throw 'Bootstrap outputs do not match the lab target and its IP-restricted state profile.'
    }
}

function Initialize-LabState {
    if ($state.account -cnotmatch '^[a-z0-9]{3,24}$' -or $state.lineage -cnotmatch '^[a-f0-9-]{36}$') {
        throw 'Invalid local bootstrap state record.'
    }
    $account = Get-AzureJson @('storage', 'account', 'show', '--name', $state.account, '--resource-group',
        $ResourceGroupName, '--subscription', "$SubscriptionId")
    $expectedId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$($state.account)"
    $perimeterUrl = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Network/networkSecurityPerimeters/nsp-$ResourceGroupName-state"
    $desktopRule = Get-AzureJson @('rest', '--method', 'get', '--url', "$perimeterUrl/profiles/state/accessRules/desktop-inbound?api-version=2024-07-01", '--subscription', "$SubscriptionId")
    $association = Get-AzureJson @('rest', '--method', 'get', '--url', "$perimeterUrl/resourceAssociations/state-account?api-version=2024-07-01", '--subscription', "$SubscriptionId")
    if ($account.id -ine $expectedId -or $account.publicNetworkAccess -ne 'SecuredByPerimeter' -or
        $account.allowSharedKeyAccess -ne $false -or $account.networkRuleSet.defaultAction -ne 'Deny' -or
        $account.networkRuleSet.bypass -ne 'None' -or @($account.networkRuleSet.ipRules).Count -ne 0 -or
        $association.properties.accessMode -ne 'Enforced' -or
        @($desktopRule.properties.addressPrefixes).Count -ne 1 -or
        $desktopRule.properties.addressPrefixes[0] -ne "$DesktopPublicIp/32") {
        throw 'The state account is not in this lab, or its perimeter does not admit exactly the approved desktop IPv4 in Enforced mode. No firewall changes were attempted.'
    }
    try {
        $remoteExists = Test-StateBlob $state.account 'tfstate-bootstrap' $bootstrapKey
        $workloadExists = Test-StateBlob $state.account 'tfstate' $workloadKey
    } catch {
        throw 'Cannot access both state containers with Entra ID. Verify desktop VPN egress and wait for scoped RBAC propagation, then rerun the same command.'
    }
    if (-not $remoteExists -and $workloadExists) { throw 'Workload state exists but bootstrap state is missing. Recover bootstrap state before retrying.' }
    if (-not $remoteExists -and -not $Apply) {
        Write-Host 'Bootstrap state is still local. Run with Apply and the recorded approval to complete state migration.'
        return $false
    }
    $backendDeclaration = Join-Path $bootstrap 'lab-backend.tf'
    if (-not $remoteExists) {
        if (-not (Test-Path -LiteralPath $localStatePath -PathType Leaf)) {
            throw 'Bootstrap state is missing in both locations. Recover state; do not recreate or import the group.'
        }
        $localState = Get-Content -LiteralPath $localStatePath -Raw | ConvertFrom-Json
        if ($localState.lineage -cne $state.lineage -or $localState.serial -ne $state.serial) {
            throw 'Local bootstrap state changed after the migration record was created. Review and recover state before retrying.'
        }
        if (Test-Path -LiteralPath $backendDeclaration) {
            Remove-Item -LiteralPath $backendDeclaration
            Invoke-Checked $terraform @("-chdir=$bootstrap", 'init', '-reconfigure', '-input=false', '-no-color', '-lockfile=readonly') | Out-Host
        }
    }
    [IO.File]::WriteAllLines($backendDeclaration, @('terraform {', '  backend "azurerm" {}', '}'))
    if ($remoteExists) {
        Initialize-RemoteBackend $bootstrap 'tfstate-bootstrap' $bootstrapKey @('-reconfigure', '-input=false')
    } else {
        Write-Host "Review the state migration prompt: local bootstrap state -> $($state.account)/tfstate-bootstrap/$bootstrapKey."
        Initialize-RemoteBackend $bootstrap 'tfstate-bootstrap' $bootstrapKey @('-migrate-state', '-input=true')
    }
    $remoteState = Invoke-Checked $terraform @("-chdir=$bootstrap", 'state', 'pull') | ConvertFrom-Json
    if ($null -eq $remoteState -or $remoteState.lineage -cne $state.lineage -or $remoteState.serial -lt $state.serial) {
        throw 'Remote bootstrap state does not match the local migration record. No workload plan was attempted.'
    }
    Assert-BootstrapOutputs (Get-BootstrapOutputs) $state.account
    return $true
}

function Invoke-RunnerAccessStages {
    $outputs = Invoke-Checked $terraform @("-chdir=$workload", 'output', '-json') | ConvertFrom-Json
    $principal = if ($outputs.PSObject.Properties['runner_identity_principal_id']) { $outputs.runner_identity_principal_id.value } else { $null }
    if (-not $principal) {
        Write-Host 'The runner is not in the workload state yet. Apply the workload plan, then rerun to plan its access.'
        return
    }
    $record = @{
        runner_principal_id = $principal
        private_endpoint_subnet_id = $outputs.private_endpoint_subnet_id.value
        blob_zone_id = $outputs.private_dns_zone_ids.value.blob
    }
    Write-JsonFile $accessPath $record
    $null = Invoke-ReviewedPlan $bootstrap 'bootstrap-runner' ($baseBootstrapVars + @{
        artifact_reader_object_ids = @($principal)
        state_private_endpoint_subnet_id = $record.private_endpoint_subnet_id
        state_private_dns_zone_id = $record.blob_zone_id
    })
    $roleVars = $workloadVars.Clone()
    $roleVars.registration_principal_object_id = $principal
    $null = Invoke-ReviewedPlan $workload 'workload-runner-roles' $roleVars
}

$savedEnvironment = @{}
$lock = $null
try {
    foreach ($entry in Get-ChildItem Env: | Where-Object Name -Match '^(ARM_|TF_)') {
        $savedEnvironment[$entry.Name] = $entry.Value
        Remove-Item -LiteralPath "Env:$($entry.Name)"
    }
    $version = Invoke-Checked $terraform @('version', '-json') | ConvertFrom-Json
    if ($version.terraform_version -notmatch '^\d+\.\d+\.\d+$' -or [version] $version.terraform_version -lt [version] '1.11.0') {
        throw 'Terraform 1.11 or later (stable) is required.'
    }
    Assert-AzureTarget
    $null = New-Item -ItemType Directory -Force -Path $runRoot
    $lock = [IO.File]::Open((Join-Path $runRoot 'deploy.lock'), 'OpenOrCreate', 'ReadWrite', 'None')
    $bootstrap = Join-Path $runRoot 'bootstrap'
    $workload = Join-Path $runRoot 'envs/poc'
    $localStatePath = Join-Path $bootstrap 'terraform.tfstate'
    $stateRecord = Join-Path $runRoot 'bootstrap-state.json'
    $accessPath = Join-Path $runRoot 'runner-access.json'
    $access = if (Test-Path -LiteralPath $accessPath -PathType Leaf) { Get-Content -LiteralPath $accessPath -Raw | ConvertFrom-Json } else { $null }
    $bootstrapKey = "bootstrap/$ResourceGroupName.tfstate"
    $workloadKey = "workload/$ResourceGroupName.tfstate"
    $groupExists = Get-AzureJson @('group', 'exists', '--name', $ResourceGroupName, '--subscription', "$SubscriptionId")
    $recordExists = Test-Path -LiteralPath $stateRecord -PathType Leaf
    $localExists = Test-Path -LiteralPath $localStatePath -PathType Leaf
    if ($groupExists -isnot [bool]) { throw 'Azure CLI did not return a valid resource group existence result.' }
    if ($groupExists -and -not ($localExists -or $recordExists)) {
        throw "Resource group $ResourceGroupName already exists without this desktop's bootstrap state. No import or adoption was attempted."
    }
    if (-not $groupExists -and ($recordExists -or $localExists)) {
        throw 'Bootstrap state exists but the lab resource group is missing. Verify Azure and recover state before retrying.'
    }
    if ($localExists -and -not $recordExists) {
        $localState = Get-Content -LiteralPath $localStatePath -Raw | ConvertFrom-Json
        $ownedGroup = $localState.resources | Where-Object { $_.mode -eq 'managed' -and $_.type -eq 'azurerm_resource_group' -and $_.name -eq 'this' }
        if ($null -eq $ownedGroup -or $ownedGroup.instances.attributes.id -ine "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName") {
            throw 'The local bootstrap state does not own this lab group. No import or adoption was attempted.'
        }
    }
    if ($Apply) { Write-Host "Recorded G1 approval reference: $ApprovalReference" }
    Write-Host "Target: $SubscriptionId / $ResourceGroupName / $location; $ModelName $ModelVersion GlobalBatch (capacity $ModelCapacity; dynamic quota $DynamicQuota)."
    $preflightInput = Join-Path $runRoot 'preflight.json'
    Write-JsonFile $preflightInput @{
        tenant_id = "$TenantId"
        subscription_id = "$SubscriptionId"
        resource_group = $ResourceGroupName
        location = $location
        model_name = $ModelName
        model_version = $ModelVersion
        sku_name = 'GlobalBatch'
        deployer_object_id = "$StateContributorObjectId"
    }
    $rawFindings = & $python -m foundry_batch preflight --input $preflightInput --format json
    $preflightExit = $LASTEXITCODE
    if (-not $rawFindings) { throw 'Preflight did not return findings. Install the local project with python -m pip install -e . and retry.' }
    $findings = ($rawFindings -join "`n") | ConvertFrom-Json -NoEnumerate
    $findings | Format-Table check, status, detail -Wrap | Out-Host
    if ($preflightExit -ne 0 -or $findings.Count -eq 0 -or
        @($findings | Where-Object { $_.status -eq 'fail' -or ($_.check -like 'provider:*' -and $_.status -ne 'pass') }).Count -gt 0) {
        throw 'Preflight did not pass. Fix the findings before planning; the script never registers providers.'
    }
    if ($Apply -or $CheckOnly) {
        $subscriptionScope = "/subscriptions/$SubscriptionId"
        $bootstrapScope = if ($groupExists) { "$subscriptionScope/resourceGroups/$ResourceGroupName" } else { $subscriptionScope }
        Assert-ArmPermissions $bootstrapScope @('Microsoft.Resources/subscriptions/resourceGroups/write',
            'Microsoft.Storage/storageAccounts/write', 'Microsoft.Storage/storageAccounts/blobServices/write',
            'Microsoft.Storage/storageAccounts/blobServices/containers/write', 'Microsoft.Authorization/roleAssignments/write',
            'Microsoft.Network/networkSecurityPerimeters/write', 'Microsoft.Network/networkSecurityPerimeters/profiles/write',
            'Microsoft.Network/networkSecurityPerimeters/profiles/accessRules/write',
            'Microsoft.Network/networkSecurityPerimeters/resourceAssociations/write')
        if ($CheckOnly) { Assert-WorkloadPermissions $bootstrapScope }
    }
    if ($CheckOnly) { return }
    Copy-TerraformConfiguration
    $common = @{
        subscription_id = "$SubscriptionId"
        tenant_id = "$TenantId"
        resource_group_name = $ResourceGroupName
        location = $location
        tags = @{ owner = $Owner; 'cost-center' = $CostCenter }
    }
    $bootstrapVars = $common + @{
        allowed_ip_ranges = @($DesktopPublicIp)
        state_public_network_access_enabled = $true
        state_network_security_perimeter_enabled = $true
        artifact_container_enabled = $true
        artifact_uploader_object_ids = @("$StateContributorObjectId")
        state_contributor_object_ids = @("$StateContributorObjectId")
        bootstrap_state_contributor_object_ids = @("$StateContributorObjectId")
    }
    # Once recorded, keep the runner's access in every later plan so a rerun never proposes removing it.
    $baseBootstrapVars = $bootstrapVars
    if ($access) {
        $bootstrapVars = $bootstrapVars + @{
            artifact_reader_object_ids = @($access.runner_principal_id)
            state_private_endpoint_subnet_id = $access.private_endpoint_subnet_id
            state_private_dns_zone_id = $access.blob_zone_id
        }
    }
    if ($recordExists) {
        $state = Get-Content -LiteralPath $stateRecord -Raw | ConvertFrom-Json
        if (-not (Initialize-LabState)) { return }
        if (-not (Invoke-ReviewedPlan $bootstrap 'bootstrap' $bootstrapVars) -and $Apply) { return }
    } else {
        Invoke-Checked $terraform @("-chdir=$bootstrap", 'init', '-input=false', '-no-color', '-lockfile=readonly') | Out-Host
        if (-not (Invoke-ReviewedPlan $bootstrap 'bootstrap' $bootstrapVars)) { return }
        $outputs = Get-BootstrapOutputs
        $stateAccount = $outputs.state_storage_account_name.value
        Assert-BootstrapOutputs $outputs $stateAccount
        $localState = Get-Content -LiteralPath $localStatePath -Raw | ConvertFrom-Json
        $state = @{ account = $stateAccount; lineage = $localState.lineage; serial = $localState.serial }
        Write-JsonFile $stateRecord $state
        if (-not (Initialize-LabState)) { return }
    }
    $workloadVars = $common + @{
        environment = $workloadEnvironment
        foundry_resources = @{
            eus2 = @{
                dynamic_throttling_enabled = $DynamicQuota -eq 'enabled'
                deployments = @(@{
                    name = 'batch-default'
                    model_name = $ModelName
                    model_version = $ModelVersion
                    sku_name = 'GlobalBatch'
                    capacity = $ModelCapacity
                })
            }
        }
    }
    $workloadVars.operator_principal_object_ids = @("$StateContributorObjectId")
    if ($RunnerSshPublicKeyPath) {
        $workloadVars.runner_enabled = $true
        $workloadVars.runner_admin_ssh_public_key = (Get-Content -LiteralPath $RunnerSshPublicKeyPath -Raw).Trim()
    }
    if ($RegistrationPrincipalObjectId -and $RegistrationPrincipalObjectId -ne [guid]::Empty) {
        $workloadVars.registration_principal_object_id = "$RegistrationPrincipalObjectId"
    } elseif ($access) {
        $workloadVars.registration_principal_object_id = $access.runner_principal_id
    }
    if ($Apply) { Assert-WorkloadPermissions "$subscriptionScope/resourceGroups/$ResourceGroupName" }
    Initialize-RemoteBackend $workload 'tfstate' $workloadKey @('-reconfigure', '-input=false')
    if (Invoke-ReviewedPlan $workload 'workload' $workloadVars) {
        Write-Host 'Terraform infrastructure applied. App packaging, registration, and smoke tests run from the private runner (see infra/README.md and docs/runbooks/README.md).'
    }
    if ($RunnerSshPublicKeyPath) { Invoke-RunnerAccessStages }
} finally {
    if ($null -ne $lock) { $lock.Dispose() }
    foreach ($entry in $savedEnvironment.GetEnumerator()) { Set-Item -LiteralPath "Env:$($entry.Key)" -Value $entry.Value }
}