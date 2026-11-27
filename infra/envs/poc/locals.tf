locals {
  region_short = lookup({
    eastus2       = "eus2"
    eastus        = "eus"
    swedencentral = "swc"
    brazilsouth   = "brs"
    westus3       = "wus3"
  }, var.location, substr(replace(var.location, "-", ""), 0, 6))

  # Deterministic per subscription/RG/environment: globally unique names without a random
  # provider, so a re-plan never renames anything.
  unique = substr(sha1("${var.subscription_id}/${var.resource_group_name}/${var.environment}"), 0, 6)

  name_suffix = "${var.workload}-${var.environment}-${local.region_short}"

  names = {
    function_app    = "func-${var.workload}-${var.environment}-${local.unique}"
    storage_account = "st${var.workload}${var.environment}${local.unique}"
    key_vault       = "kv-${var.workload}-${var.environment}-${local.unique}"
    foundry         = { for key in keys(var.foundry_resources) : key => "oai-${var.workload}-${var.environment}-${key}-${local.unique}" }
  }

  tags = merge(var.tags, {
    environment = var.environment
    workload    = var.workload
    managed-by  = "terraform"
    source-repo = "ms-foundry-ai-batch"
  })

  # Names the Python app depends on (docs/configuration.md). Changing one is a breaking change.
  queues           = ["batch-events", "batch-events-poison", "relay-dispatch", "relay-dispatch-poison"]
  tables           = ["batchjobs", "batchcontrol", "batchquarantine", "batchsubmissions", "demoreceipts"]
  deploy_container = "app-package"

  # Public paths kept on purpose (ADR 0010). tools/plan_guard.py derives the same allowances
  # from the plan's variables and rejects every other public network access.
  network_exceptions = concat(
    var.function_public_ingress ? [{
      id      = "NX-1"
      path    = "Function App public endpoint (webhook delivery)"
      reason  = "The provider delivers webhooks only to publicly reachable HTTPS endpoints (CON-W05, Q-P01)."
      control = length(var.webhook_source_service_tags) > 0 ? "Signatures, body cap, SCM private, service tags: ${join(", ", var.webhook_source_service_tags)}" : "Signatures, body cap, SCM private, no source restriction yet"
    }] : [],
    var.monitor_public_query_enabled ? [{
      id      = "NX-2"
      path    = "Log Analytics and Application Insights queries from public networks"
      reason  = "Operators query from the portal without an in-network client; telemetry ingestion stays private."
      control = "Entra ID and resource permissions; turn off after TC-H13"
    }] : [],
    var.monitor_private_link_enabled ? [] : [{
      id      = "NX-2b"
      path    = "Telemetry ingestion over public Azure Monitor endpoints"
      reason  = "Azure Monitor Private Link Scope disabled"
      control = "Entra-only ingestion"
    }],
  )

  resources = [
    for key in sort(keys(var.foundry_resources)) : {
      key         = key
      endpoint    = module.foundry[key].openai_endpoint
      region      = coalesce(var.foundry_resources[key].location, var.location)
      deployments = module.foundry[key].deployment_names
    }
  ]

  app_settings = {
    BATCH_ENVIRONMENT                 = var.environment
    BATCH_RESOURCES_JSON              = jsonencode(local.resources)
    BATCH_KEY_VAULT_URI               = module.key_vault.vault_uri
    BATCH_STORAGE__queueServiceUri    = trimsuffix(module.storage.primary_queue_endpoint, "/")
    BATCH_STORAGE__tableServiceUri    = trimsuffix(module.storage.primary_table_endpoint, "/")
    BATCH_INCOMING_QUEUE              = "batch-events"
    BATCH_RELAY_QUEUE                 = "relay-dispatch"
    BATCH_TABLE_JOBS                  = "batchjobs"
    BATCH_TABLE_CONTROL               = "batchcontrol"
    BATCH_TABLE_QUARANTINE            = "batchquarantine"
    BATCH_TABLE_SUBMISSIONS           = "batchsubmissions"
    BATCH_TABLE_DEMO_RECEIPTS         = "demoreceipts"
    BATCH_DOWNSTREAM_MODE             = var.downstream_mode
    BATCH_DOWNSTREAM_URL              = var.downstream_url
    BATCH_RELAY_SIGNING_SECRET_NAME   = "relay-signing-${var.environment}"
    BATCH_MAX_BODY_BYTES              = tostring(var.max_body_bytes)
    BATCH_TIMESTAMP_TOLERANCE_SECONDS = tostring(var.timestamp_tolerance_seconds)
    BATCH_RECEIVER_RATE_PER_SECOND    = tostring(var.receiver_rate_per_second)
    BATCH_RECEIVER_BURST              = tostring(var.receiver_burst)
    BATCH_SWEEP_SCHEDULE              = var.sweep_schedule
    BATCH_OUTBOX_SCHEDULE             = var.outbox_schedule
    BATCH_API_TOKEN_SCOPE             = "https://cognitiveservices.azure.com/.default"

    # Host-level request body cap in bytes (default about 210 MB). Kept above the app's highest
    # allowed BATCH_MAX_BODY_BYTES (48 KiB) so the receiver's own limit still answers first.
    FUNCTIONS_REQUEST_BODY_SIZE_LIMIT = "65536"
  }
}
