variable "name_suffix" {
  description = "Naming suffix shared by the workload."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group for monitoring resources."
  type        = string
}

variable "resource_group_id" {
  description = "Resource group id (parent of the azapi workspace)."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "retention_in_days" {
  description = "Log Analytics retention; at least 45 days so 30-day views have margin (US-6.6 AC4)."
  type        = number
  default     = 45

  validation {
    condition     = var.retention_in_days >= 30 && var.retention_in_days <= 730
    error_message = "Retention must be between 30 and 730 days."
  }
}

variable "diagnostic_targets" {
  description = "Resources whose platform logs/metrics go to the workspace, keyed by a static name."
  type = map(object({
    resource_id = string
    logs        = optional(bool, true)
    metrics     = optional(bool, true)
  }))
}

variable "alert_email_receivers" {
  description = "Alert e-mail recipients (owner-approved; none by default)."
  type = list(object({
    name          = string
    email_address = string
  }))
  default = []
}

variable "signature_failure_threshold" {
  description = "Rejected deliveries (signature/timestamp/headers) per 15 minutes that raise an alert."
  type        = number
  default     = 10
}

variable "sweep_lag_threshold_seconds" {
  description = "Seconds since a resource's last complete sweep that raise the sweep-lagging alert."
  type        = number
  default     = 1200
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}

variable "private_link_enabled" {
  description = "Send telemetry through an Azure Monitor Private Link Scope and refuse public ingestion."
  type        = bool
  default     = true
}

variable "public_query_enabled" {
  description = "Accept log and metric queries from public networks (exception NX-2: portal and workbook access without an in-network client)."
  type        = bool
  default     = true
}

variable "private_endpoint_subnet_id" {
  description = "Subnet for the AMPLS private endpoint (required when private_link_enabled)."
  type        = string
  default     = null
}

variable "private_endpoint_location" {
  description = "Region of the private endpoint subnet's VNet (defaults to location)."
  type        = string
  default     = null
}

variable "private_dns_zone_ids" {
  description = "Zone ids with keys monitor, oms, ods, agentsvc and blob (required when private_link_enabled)."
  type        = map(string)
  default     = {}
}
