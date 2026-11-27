locals {
  workspace_resource_type = "microsoft.operationalinsights/workspaces"

  # One record per job (latest lifecycle event) so duplicates never skew the indicators.
  lifecycle_base = <<-KQL
    AppTraces
    | where tostring(Properties.batch_event) == "job.lifecycle"
    | extend resource_key = tostring(Properties.resource_key),
         batch_id = tostring(Properties.batch_id), deployment = tostring(Properties.deployment),
             region = tostring(Properties.region), model = tostring(Properties.model),
             raw_status = tostring(Properties.raw_status), source = tostring(Properties.source),
             turnaround_s = todouble(Properties.turnaround_s), time_to_start_s = todouble(Properties.time_to_start_s),
             per_request_s = todouble(Properties.per_request_s)
    | summarize arg_max(TimeGenerated, *) by resource_key, batch_id
  KQL

  workbook_queries = {
    indicators     = <<-KQL
      ${local.lifecycle_base}
      | summarize jobs = count(),
                  p50_turnaround_h = round(percentile(turnaround_s, 50) / 3600, 2),
                  p95_turnaround_h = round(percentile(turnaround_s, 95) / 3600, 2),
                  max_turnaround_h = round(max(turnaround_s) / 3600, 2),
                  p95_per_request_s = round(percentile(per_request_s, 95), 1),
                  completed = countif(raw_status == "completed"), failed = countif(raw_status == "failed"),
                  expired = countif(raw_status == "expired"), cancelled = countif(raw_status == "cancelled"),
                  above_24h = countif(turnaround_s > 86400), learned_by_polling = countif(source == "polling")
        by deployment, region, model
      | extend completion_rate_pct = round(100.0 * completed / jobs, 1),
               expiration_rate_pct = round(100.0 * expired / jobs, 1),
               cancellation_rate_pct = round(100.0 * cancelled / jobs, 1),
               share_above_24h_pct = round(100.0 * above_24h / jobs, 1)
    KQL
    webhook_health = <<-KQL
      AppTraces
      | extend ev = tostring(Properties.batch_event)
      | where ev in ("webhook.accepted", "webhook.rejected", "processor.duplicate", "relay.retry_scheduled", "relay.quarantined", "accounting.missed_event")
      | summarize events = count(),
                  p95_delivery_delay_s = percentile(todouble(Properties.delivery_delay_s), 95)
        by ev, reason = tostring(Properties.reason)
    KQL
    api_pressure   = <<-KQL
      AppTraces
      | where tostring(Properties.batch_event) == "api.calls"
      | summarize calls = sum(toint(Properties.count))
        by resource_key = tostring(Properties.resource_key), operation = tostring(Properties.operation),
           status_class = tostring(Properties.status_class)
      | order by calls desc
    KQL
    sweeps         = <<-KQL
      AppTraces
      | where tostring(Properties.batch_event) == "reconcile.sweep_completed"
      | summarize sweeps = count(), list_pages = sum(toint(Properties.list_pages)),
                  retrieves = sum(toint(Properties.retrieves)), throttled_429 = sum(toint(Properties.status_429)),
                  server_5xx = sum(toint(Properties.status_5xx)), terminal_found = sum(toint(Properties.terminal_found)),
                  max_duration_s = max(todouble(Properties.duration_s))
        by resource_key = tostring(Properties.resource_key)
    KQL
    queues         = <<-KQL
      AppTraces
      | where tostring(Properties.batch_event) == "ops.queue_depth"
      | summarize arg_max(TimeGenerated, approximate_count = toint(Properties.approximate_count)) by queue = tostring(Properties.queue)
    KQL
  }

  workbook = {
    version = "Notebook/1.0"
    items = concat(
      [
        {
          type = 1
          name = "intro"
          content = {
            json = "## Batch Batch PoC\nIndicators come from `job.lifecycle` events (one per job). Percentiles here are KQL estimates; the authoritative definitions (nearest rank, D-09) are produced by `foundry-batch indicators`."
          }
        },
        {
          type = 9
          name = "parameters"
          content = {
            version = "KqlParameterItem/1.0"
            parameters = [
              {
                id         = "f0c3f1b6-4d43-4bb9-9b1e-2d7c1c5a0e01"
                version    = "KqlParameterItem/1.0"
                name       = "TimeRange"
                type       = 4
                isRequired = true
                value      = { durationMs = 86400000 }
                typeSettings = {
                  selectableValues = [
                    { durationMs = 86400000 },
                    { durationMs = 604800000 },
                    { durationMs = 2592000000 },
                  ]
                }
              },
            ]
          }
        },
      ],
      [
        for key, query in local.workbook_queries : {
          type = 3
          name = key
          content = {
            version                  = "KqlItem/1.0"
            title                    = replace(key, "_", " ")
            query                    = query
            size                     = 0
            queryType                = 0
            resourceType             = local.workspace_resource_type
            timeContext              = { durationMs = 86400000 }
            timeContextFromParameter = "TimeRange"
          }
        }
      ],
    )
    fallbackResourceIds = [lower(azapi_resource.workspace.id)]
  }
}
