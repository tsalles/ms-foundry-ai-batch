# Created through ARM with azapi, like the storage accounts: azurerm_log_analytics_workspace
# reads the shared keys on every refresh, which would put them in state and in every plan.
# Local (shared key) authentication is off, so ingestion and queries need Entra ID.
resource "azapi_resource" "workspace" {
  type      = "Microsoft.OperationalInsights/workspaces@2023-09-01"
  name      = "log-${var.name_suffix}"
  location  = var.location
  parent_id = var.resource_group_id
  tags      = var.tags

  body = {
    properties = {
      sku = {
        name = "PerGB2018"
      }
      retentionInDays = var.retention_in_days
      features = {
        disableLocalAuth                            = true
        enableLogAccessUsingOnlyResourcePermissions = true
      }
      # Telemetry arrives through the AMPLS private endpoint only. Queries from public networks
      # (portal, workbook) stay open while exception NX-2 is on (ADR 0010).
      publicNetworkAccessForIngestion = var.private_link_enabled ? "Disabled" : "Enabled"
      publicNetworkAccessForQuery     = var.public_query_enabled ? "Enabled" : "Disabled"
    }
  }

  lifecycle {
    precondition {
      condition     = var.private_link_enabled || var.public_query_enabled
      error_message = "Public queries can only be turned off together with the private link (nothing could query the workspace)."
    }
  }
}

resource "azurerm_application_insights" "this" {
  name                = "appi-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  workspace_id        = azapi_resource.workspace.id
  application_type    = "other"
  retention_in_days   = 90

  # Entra-only ingestion: the connection string alone cannot be used to write telemetry.
  local_authentication_enabled = false
  internet_ingestion_enabled   = !var.private_link_enabled
  internet_query_enabled       = var.public_query_enabled
  tags                         = var.tags
}

# Azure Monitor Private Link Scope (AMPLS): the workload VNet reaches this workspace and this
# Application Insights component only, through one private endpoint. Private Only access modes
# also stop the VNet from sending telemetry to any other Azure Monitor resource. ARM through
# azapi (global resource, stable API) like the workspace.
resource "azapi_resource" "private_link_scope" {
  count = var.private_link_enabled ? 1 : 0

  type      = "Microsoft.Insights/privateLinkScopes@2021-07-01-preview"
  name      = "ampls-${var.name_suffix}"
  location  = "global"
  parent_id = var.resource_group_id
  tags      = var.tags

  body = {
    properties = {
      accessModeSettings = {
        ingestionAccessMode = "PrivateOnly"
        queryAccessMode     = "PrivateOnly"
        exclusions          = []
      }
    }
  }
}

resource "azapi_resource" "scoped_resource" {
  for_each = {
    for key, id in {
      workspace            = azapi_resource.workspace.id
      application-insights = azurerm_application_insights.this.id
    } : key => id if var.private_link_enabled
  }

  type          = "Microsoft.Insights/privateLinkScopes/scopedResources@2021-07-01-preview"
  name          = "scoped-${each.key}"
  parent_id     = azapi_resource.private_link_scope[0].id
  ignore_casing = true # the service lower-cases linkedResourceId

  body = {
    properties = {
      linkedResourceId = each.value
    }
  }
}

# Created after the scoped resources so the DNS zone group receives the workspace-specific
# (oms/ods/agentsvc) records.
resource "azurerm_private_endpoint" "monitor" {
  count = var.private_link_enabled ? 1 : 0

  name                = "pe-ampls-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  location            = coalesce(var.private_endpoint_location, var.location)
  subnet_id           = var.private_endpoint_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-ampls-${var.name_suffix}"
    private_connection_resource_id = azapi_resource.private_link_scope[0].id
    subresource_names              = ["azuremonitor"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name = "default"
    private_dns_zone_ids = [
      for key in ["monitor", "oms", "ods", "agentsvc", "blob"] : var.private_dns_zone_ids[key]
    ]
  }

  depends_on = [azapi_resource.scoped_resource]

  lifecycle {
    precondition {
      condition     = var.private_endpoint_subnet_id != null && alltrue([for key in ["monitor", "oms", "ods", "agentsvc", "blob"] : contains(keys(var.private_dns_zone_ids), key)])
      error_message = "The AMPLS needs private_endpoint_subnet_id and the monitor, oms, ods, agentsvc and blob zone ids."
    }
  }
}

resource "azurerm_monitor_diagnostic_setting" "this" {
  for_each = var.diagnostic_targets

  name                       = "diag-to-${azapi_resource.workspace.name}"
  target_resource_id         = each.value.resource_id
  log_analytics_workspace_id = azapi_resource.workspace.id

  dynamic "enabled_log" {
    for_each = each.value.logs ? ["allLogs"] : []

    content {
      category_group = enabled_log.value
    }
  }

  dynamic "enabled_metric" {
    for_each = each.value.metrics ? ["AllMetrics"] : []

    content {
      category = enabled_metric.value
    }
  }

  # Storage sub-services expand AllMetrics into Capacity and Transaction on read-back.
  lifecycle {
    ignore_changes = [enabled_metric]
  }
}

resource "azurerm_monitor_action_group" "this" {
  name                = "ag-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  short_name          = "aibatch"
  tags                = var.tags

  dynamic "email_receiver" {
    for_each = var.alert_email_receivers

    content {
      name                    = email_receiver.value.name
      email_address           = email_receiver.value.email_address
      use_common_alert_schema = true
    }
  }
}

locals {
  # Every rule reads the app's structured events (AppTraces.Properties.batch_event); names are
  # part of the contract in docs/configuration.md.
  event = "tostring(Properties.batch_event)"

  alerts = {
    poison-queue = {
      description = "Poison queue not empty. Runbook: docs/runbooks/README.md#replay-and-poison-messages"
      severity    = 2
      frequency   = "PT5M"
      window      = "PT15M"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "ops.queue_depth"
        | where tostring(Properties.queue) endswith "-poison"
        | where toint(Properties.approximate_count) > 0
      KQL
    }
    signature-failures = {
      description = "Rejected deliveries rising (signature, timestamp, headers, unknown resource key). Runbook: docs/runbooks/README.md#register-and-rotate"
      severity    = 2
      frequency   = "PT5M"
      window      = "PT15M"
      operator    = "GreaterThan"
      threshold   = var.signature_failure_threshold
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "webhook.rejected"
        | where tostring(Properties.reason) in ("signature_mismatch", "timestamp_out_of_tolerance", "missing_headers", "unknown_resource")
      KQL
    }
    missed-events = {
      description = "Webhook-covered job reached a final state without an event. Runbook: docs/runbooks/README.md#reconcile"
      severity    = 3
      frequency   = "PT15M"
      window      = "PT1H"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "accounting.missed_event"
      KQL
    }
    job-over-24h = {
      description = "A batch job is older than 24 hours and not final. Runbook: docs/runbooks/README.md#reconcile"
      severity    = 3
      frequency   = "PT15M"
      window      = "PT1H"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "job.over_24h"
      KQL
    }
    sweep-failed = {
      description = "Reconciliation sweep failed. Runbook: docs/runbooks/README.md#reconcile"
      severity    = 2
      frequency   = "PT15M"
      window      = "PT30M"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "reconcile.sweep_failed"
      KQL
    }
    sweep-absent = {
      description = "No completed reconciliation sweep in 45 minutes (expected every 15). Runbook: docs/runbooks/README.md#reconcile"
      severity    = 2
      frequency   = "PT15M"
      window      = "PT45M"
      operator    = "LessThan"
      threshold   = 1
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "reconcile.sweep_completed"
      KQL
    }
    # lag_s: seconds since the resource's last sweep that finished both the list traversal and
    # the fallback (null until one has). Fires on lag above the convergence threshold, or when
    # the last three sweeps of a resource all ended with traversal_complete=false.
    sweep-lagging = {
      description = "Reconciliation is not converging (lag above threshold or 3 incomplete sweeps). Runbook: docs/runbooks/README.md#reconcile"
      severity    = 2
      frequency   = "PT15M"
      window      = "PT1H"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        let sweeps = AppTraces
          | where ${local.event} == "reconcile.sweep_completed"
          | extend resource_key = tostring(Properties.resource_key),
                   lag_s = todouble(Properties.lag_s),
                   traversal_complete = tostring(Properties.traversal_complete);
        let lagging = sweeps
          | where TimeGenerated > ago(30m) and lag_s > ${var.sweep_lag_threshold_seconds}
          | summarize max_lag_s = max(lag_s) by resource_key
          | extend reason = "lag";
        let incomplete = sweeps
          | order by resource_key asc, TimeGenerated desc
          | extend run = row_number(1, prev(resource_key) != resource_key)
          | where run <= 3
          | summarize runs = count(), incomplete = countif(traversal_complete =~ "false") by resource_key
          | where runs == 3 and incomplete == 3
          | extend reason = "traversal_incomplete";
        union lagging, incomplete
      KQL
    }
    relay-quarantined = {
      description = "Relay gave up on an event (permanent error or attempts exhausted). Runbook: docs/runbooks/README.md#replay-and-poison-messages"
      severity    = 2
      frequency   = "PT5M"
      window      = "PT15M"
      operator    = "GreaterThan"
      threshold   = 0
      query       = <<-KQL
        AppTraces
        | where ${local.event} == "relay.quarantined"
      KQL
    }
  }
}

resource "azurerm_monitor_scheduled_query_rules_alert_v2" "this" {
  for_each = local.alerts

  name                 = "alert-${each.key}-${var.name_suffix}"
  display_name         = "Batch batch: ${each.key}"
  description          = each.value.description
  resource_group_name  = var.resource_group_name
  location             = var.location
  scopes               = [azapi_resource.workspace.id]
  severity             = each.value.severity
  evaluation_frequency = each.value.frequency
  window_duration      = each.value.window
  enabled              = true

  # The app's tables do not exist until it logs for the first time.
  skip_query_validation   = true
  auto_mitigation_enabled = true

  criteria {
    query                   = each.value.query
    time_aggregation_method = "Count"
    operator                = each.value.operator
    threshold               = each.value.threshold

    failing_periods {
      minimum_failing_periods_to_trigger_alert = 1
      number_of_evaluation_periods             = 1
    }
  }

  action {
    action_groups = [azurerm_monitor_action_group.this.id]
  }

  tags = var.tags
}

resource "azurerm_application_insights_workbook" "this" {
  # Workbook names must be GUIDs; uuidv5 keeps the name stable across plans.
  name                = uuidv5("url", "https://foundry-batch/workbook/${var.name_suffix}")
  resource_group_name = var.resource_group_name
  location            = var.location
  display_name        = "Batch Batch PoC - ${var.name_suffix}"
  source_id           = lower(azapi_resource.workspace.id)
  data_json           = jsonencode(local.workbook)
  tags                = var.tags
}
