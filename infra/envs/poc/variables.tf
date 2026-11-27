variable "tenant_id" {
  description = "Entra tenant of the target subscription (REQ-I08)."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.tenant_id))
    error_message = "tenant_id must be a lowercase GUID."
  }
}

variable "subscription_id" {
  description = "Target subscription (REQ-I08)."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.subscription_id))
    error_message = "subscription_id must be a lowercase GUID."
  }
}

variable "allowed_locations" {
  description = "Allowed workload regions; review residency and model availability before deployment."
  type        = list(string)
  default     = ["eastus2"]
}

variable "location" {
  description = "Region of the workload."
  type        = string
  default     = "eastus2"

  validation {
    condition     = contains(var.allowed_locations, var.location)
    error_message = "location must be one of allowed_locations."
  }
}

variable "environment" {
  description = "Environment name; part of names, keys and secret names."
  type        = string
  default     = "poc"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,9}$", var.environment))
    error_message = "environment must be 2-10 lowercase letters/digits starting with a letter."
  }

  # The app treats these names as its local profile (config.py LOCAL_ENVIRONMENTS).
  validation {
    condition     = !contains(["local", "dev", "test", "ci"], lower(var.environment))
    error_message = "environment must not be local, dev, test or ci: the app reserves them for local runs."
  }
}

variable "workload" {
  description = "Workload short name used in resource names."
  type        = string
  default     = "aibatch"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{2,11}$", var.workload))
    error_message = "workload must be 3-12 lowercase letters/digits."
  }
}

variable "resource_group_name" {
  description = "Resource group created by the bootstrap root (this root only references it)."
  type        = string
  default     = "rg-foundry-batch-poc"
}

variable "tags" {
  description = "Tags for every resource. owner and cost-center are mandatory."
  type        = map(string)

  validation {
    condition     = alltrue([for k in ["owner", "cost-center"] : contains(keys(var.tags), k) && trimspace(lookup(var.tags, k, "")) != ""])
    error_message = "tags must include non-empty owner and cost-center."
  }
}

variable "foundry_resources" {
  description = <<-EOT
    Foundry (Azure OpenAI) accounts keyed by the resource key used in webhook paths and secret
    names (e.g. eus2). Adding an entry adds an account, a receiver path and secret slots with no
    code change (US-7.4). Model, version and capacity are pinned by decision D-12.
  EOT
  type = map(object({
    location                   = optional(string)
    kind                       = optional(string, "AIServices")
    local_auth_enabled         = optional(bool, false)
    dynamic_throttling_enabled = optional(bool, true)
    deployments = list(object({
      name          = string
      model_name    = string
      model_version = string
      sku_name      = optional(string, "GlobalBatch")
      capacity      = number
    }))
  }))

  validation {
    condition     = length(var.foundry_resources) > 0 && alltrue([for k in keys(var.foundry_resources) : can(regex("^[a-z][a-z0-9]{1,11}$", k))])
    error_message = "Provide at least one resource; keys are 2-12 lowercase letters/digits."
  }

  validation {
    condition = alltrue(flatten([
      for r in values(var.foundry_resources) : [
        for d in r.deployments :
        trimspace(d.model_version) != "" && !strcontains(upper(d.model_version), "REPLACE") && !strcontains(upper(d.model_name), "REPLACE")
      ]
    ]))
    error_message = "Pin every model name and version (D-12); placeholders are rejected."
  }

  validation {
    condition     = alltrue([for r in values(var.foundry_resources) : r.local_auth_enabled == false])
    error_message = "API keys stay disabled until the D-04 exception is approved; change this validation in the same reviewed change."
  }

  validation {
    condition     = alltrue([for r in values(var.foundry_resources) : r.location == null || contains(var.allowed_locations, coalesce(r.location, "-"))])
    error_message = "A Foundry location override must be one of allowed_locations (REQ-I08)."
  }
}

variable "vnet_address_space" {
  description = "Workload VNet address space."
  type        = list(string)
  default     = ["10.42.0.0/24"]
}

variable "function_subnet_prefix" {
  description = "Subnet delegated to Flex Consumption (/27 minimum)."
  type        = string
  default     = "10.42.0.0/26"
}

variable "private_endpoint_subnet_prefix" {
  description = "Private endpoint subnet."
  type        = string
  default     = "10.42.0.64/27"
}

variable "runner_enabled" {
  description = "Create the private lab runner and its dedicated network resources."
  type        = bool
  default     = false
}

variable "runner_subnet_prefix" {
  description = "Dedicated runner subnet within the workload VNet."
  type        = string
  default     = "10.42.0.96/27"
}

variable "runner_admin_ssh_public_key" {
  description = "Administrator SSH public key required by Azure for the private runner. Inbound SSH remains denied."
  type        = string
  default     = null
  sensitive   = true
}

variable "runner_vnet_ids" {
  description = "VNets of private build agents that must resolve the private endpoints (peering is managed outside this root)."
  type        = map(string)
  default     = {}
}

# ---- Private networking (REQ-I13, ADR 0010) ----------------------------------------------------
# Every Azure path uses a private endpoint. The defaults keep the recorded PoC exceptions:
# NX-1 webhook ingress (function_public_ingress) and NX-2 portal log queries
# (monitor_public_query_enabled). tools/plan_guard.py rejects any other public access.

variable "function_public_ingress" {
  description = "Exception NX-1: keep the Function App's public endpoint so the provider can deliver webhooks (CON-W05). false: private-only app, no webhook registration, polling-only reconciliation."
  type        = bool
  default     = true
}

variable "webhook_source_service_tags" {
  description = "Service tags allowed on the public webhook endpoint (empty: any source; signatures still required). Set once Q-P01 names the provider's source ranges."
  type        = list(string)
  default     = []
}

variable "monitor_private_link_enabled" {
  description = "Send telemetry through an Azure Monitor Private Link Scope (private endpoint) and refuse public ingestion."
  type        = bool
  default     = true
}

variable "monitor_public_query_enabled" {
  description = "Exception NX-2: accept log queries from public networks (portal, workbook) for operators without an in-network client. Turn off after TC-H13."
  type        = bool
  default     = true

  validation {
    condition     = var.monitor_public_query_enabled || var.monitor_private_link_enabled
    error_message = "monitor_public_query_enabled = false needs monitor_private_link_enabled = true (otherwise nothing can query the logs)."
  }
}

variable "function_outbound_route_all" {
  description = "Route all Function App outbound traffic through the VNet (outboundVnetRouting.allTraffic; verify on Flex, see infra/README.md)."
  type        = bool
  default     = true
}

variable "function_egress_lockdown" {
  description = "NSG on the Function App subnet: deny internet egress except Entra ID. Downstream http mode then needs a private path to Batch (NL-1)."
  type        = bool
  default     = true
}

variable "egress_next_hop_ip" {
  description = "Landing zone firewall IP for a 0.0.0.0/0 route on the Function App subnet (null: none). Use instead of function_egress_lockdown."
  type        = string
  default     = null

  validation {
    condition     = var.egress_next_hop_ip == null || (can(regex("^[0-9]{1,3}(\\.[0-9]{1,3}){3}$", coalesce(var.egress_next_hop_ip, "-"))) && can(cidrhost("${coalesce(var.egress_next_hop_ip, "0.0.0.0")}/32", 0)))
    error_message = "egress_next_hop_ip must be an IPv4 address."
  }

  validation {
    condition     = var.egress_next_hop_ip == null || !var.function_egress_lockdown
    error_message = "Choose one egress control: the NSG lockdown drops internet-bound packets before they reach the firewall. Set function_egress_lockdown = false with egress_next_hop_ip."
  }
}

variable "existing_private_dns_zone_ids" {
  description = "Central private DNS zones (landing zone hub) to use instead of creating them, keyed like the network module's private_dns_zone_ids output."
  type        = map(string)
  default     = {}
}

variable "function_runtime_version" {
  description = "Python version on Flex Consumption."
  type        = string
  default     = "3.11"
}

variable "function_maximum_instance_count" {
  description = "Function App scale-out ceiling."
  type        = number
  default     = 40
}

variable "function_instance_memory_mb" {
  description = "Function App instance memory (512, 2048 or 4096)."
  type        = number
  default     = 2048
}

variable "log_retention_days" {
  description = "Log Analytics retention (>= 45 recommended for 30-day views)."
  type        = number
  default     = 45
}

variable "alert_email_receivers" {
  description = "Owner-approved alert recipients."
  type = list(object({
    name          = string
    email_address = string
  }))
  default = []
}

variable "signature_failure_threshold" {
  description = "Rejected deliveries per 15 minutes that raise an alert."
  type        = number
  default     = 10
}

variable "sweep_lag_threshold_seconds" {
  description = "Seconds since the last complete sweep of a resource that raise the sweep-lagging alert (convergence criterion: 20 minutes)."
  type        = number
  default     = 1200

  validation {
    condition     = floor(var.sweep_lag_threshold_seconds) == var.sweep_lag_threshold_seconds && var.sweep_lag_threshold_seconds >= 300
    error_message = "sweep_lag_threshold_seconds must be a whole number of at least 300."
  }
}

variable "registration_principal_object_id" {
  description = "Object id of the pipeline identity that runs relay-secret, webhook registration and the smoke test (null to skip)."
  type        = string
  default     = null
}

variable "registration_foundry_role_name" {
  description = "Role the registration identity needs on Foundry accounts (confirm with spike SP-3)."
  type        = string
  default     = "Cognitive Services OpenAI Contributor"
}

variable "operator_principal_object_ids" {
  description = "Operators who run replay/triage (queue and table data roles; no Key Vault access)."
  type        = list(string)
  default     = []
}

variable "downstream_mode" {
  description = "demo (M1, this app's /api/demo/events) or http (Batch's API)."
  type        = string
  default     = "demo"

  validation {
    condition     = contains(["demo", "http"], var.downstream_mode)
    error_message = "downstream_mode must be demo or http."
  }
}

variable "downstream_url" {
  description = "HTTPS URL of the downstream API when downstream_mode is http."
  type        = string
  default     = ""

  validation {
    condition     = var.downstream_url == "" || startswith(var.downstream_url, "https://")
    error_message = "downstream_url must use https."
  }

  # Mirrors the app's start-up check: http mode without a URL would fail every invocation.
  validation {
    condition     = var.downstream_mode != "http" || var.downstream_url != ""
    error_message = "downstream_url is required when downstream_mode is http."
  }
}

variable "sweep_schedule" {
  description = "Reconciliation timer (NCRONTAB)."
  type        = string
  default     = "0 */15 * * * *"
}

variable "outbox_schedule" {
  description = "Outbox and queue-depth timer (NCRONTAB)."
  type        = string
  default     = "0 */2 * * * *"
}

variable "max_body_bytes" {
  description = "Receiver body cap in bytes (<= 49152 to fit a queue message)."
  type        = number
  default     = 32768

  validation {
    condition     = floor(var.max_body_bytes) == var.max_body_bytes && var.max_body_bytes >= 1024 && var.max_body_bytes <= 49152
    error_message = "max_body_bytes must be a whole number between 1024 and 49152."
  }
}

variable "timestamp_tolerance_seconds" {
  description = "Accepted webhook timestamp skew in seconds, past and future."
  type        = number
  default     = 300

  # Same bounds as the app (config.py); a value outside them stops every invocation.
  validation {
    condition     = floor(var.timestamp_tolerance_seconds) == var.timestamp_tolerance_seconds && var.timestamp_tolerance_seconds >= 30 && var.timestamp_tolerance_seconds <= 900
    error_message = "timestamp_tolerance_seconds must be a whole number between 30 and 900."
  }
}

variable "receiver_rate_per_second" {
  description = "Receiver token-bucket refill rate per resource path and instance (events per second)."
  type        = number
  default     = 50

  # Same bounds as the app (config.py).
  validation {
    condition     = var.receiver_rate_per_second >= 1 && var.receiver_rate_per_second <= 10000
    error_message = "receiver_rate_per_second must be between 1 and 10000."
  }
}

variable "receiver_burst" {
  description = "Receiver token-bucket capacity per resource path and instance."
  type        = number
  default     = 200

  # Same bounds as the app (config.py).
  validation {
    condition     = floor(var.receiver_burst) == var.receiver_burst && var.receiver_burst >= 1 && var.receiver_burst <= 100000
    error_message = "receiver_burst must be a whole number between 1 and 100000."
  }
}
