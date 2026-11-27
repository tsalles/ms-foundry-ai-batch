variable "name" {
  description = "Account name, also used as the custom subdomain (<name>.openai.azure.com)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$", var.name))
    error_message = "Use 3-64 lowercase letters, digits and hyphens."
  }
}

variable "resource_group_name" {
  description = "Resource group of the account."
  type        = string
}

variable "location" {
  description = "Region of the account (must offer the pinned model with the batch SKU)."
  type        = string
}

variable "kind" {
  description = "Account kind. AIServices is proposed; confirm the batch endpoint form in preflight (CON-B14)."
  type        = string
  default     = "AIServices"

  validation {
    condition     = contains(["AIServices", "OpenAI"], var.kind)
    error_message = "kind must be AIServices or OpenAI."
  }
}

variable "local_auth_enabled" {
  description = "API keys. false by default; enabling it needs the D-04 exception."
  type        = bool
  default     = false
}

variable "public_network_access_enabled" {
  description = "Public data-plane access. Webhook delivery is outbound from the service and does not need it."
  type        = bool
  default     = false
}

variable "deployments" {
  description = "Batch deployments pinned by decision D-12."
  type = list(object({
    name          = string
    model_name    = string
    model_version = string
    sku_name      = optional(string, "GlobalBatch")
    capacity      = number
  }))

  validation {
    condition     = length(var.deployments) > 0
    error_message = "At least one batch deployment is required."
  }

  validation {
    condition = alltrue([
      for d in var.deployments : contains(["GlobalBatch", "DataZoneBatch"], d.sku_name)
    ])
    error_message = "Batch deployments use the GlobalBatch or DataZoneBatch SKU."
  }

  validation {
    condition = alltrue([
      for d in var.deployments :
      trimspace(d.model_version) != "" && !strcontains(upper(d.model_version), "REPLACE") &&
      !strcontains(upper(d.model_name), "REPLACE") && d.capacity > 0
    ])
    error_message = "Pin model name, version and capacity (D-12); placeholders are rejected."
  }
}

variable "dynamic_throttling_enabled" {
  description = "Dynamic quota on batch deployments (recommended by Microsoft Learn for Global Batch)."
  type        = bool
  default     = true
}

variable "private_endpoint_subnet_id" {
  description = "Subnet for the account's private endpoint."
  type        = string
}

variable "private_endpoint_location" {
  description = "Region of the private endpoint subnet's virtual network (may differ from location)."
  type        = string
}

variable "private_dns_zone_ids" {
  description = "Zone ids keyed cognitiveservices, openai and aiservices."
  type        = map(string)
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}
