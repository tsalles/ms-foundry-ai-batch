variable "tenant_id" {
  description = "Entra tenant of the target subscription."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.tenant_id))
    error_message = "tenant_id must be a lowercase GUID."
  }
}

variable "subscription_id" {
  description = "Target subscription."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.subscription_id))
    error_message = "subscription_id must be a lowercase GUID."
  }
}

variable "location" {
  description = "Region of the resource group and state account."
  type        = string
  default     = "eastus2"

  validation {
    condition     = var.location == "eastus2"
    error_message = "The approved PoC region is eastus2 (REQ-I08)."
  }
}

variable "resource_group_name" {
  description = "Resource group owned by this root (the workload root only references it)."
  type        = string
  default     = "rg-foundry-batch-poc"
}

variable "state_contributor_object_ids" {
  description = "Principals (pipeline identity, PoC lead) allowed to read/write the workload state container (tfstate)."
  type        = list(string)
  default     = []
}

variable "bootstrap_state_contributor_object_ids" {
  description = "Principals (PoC lead or platform admins, never the workload pipeline identity) allowed to read/write the bootstrap state container (tfstate-bootstrap)."
  type        = list(string)
  default     = []
}

variable "artifact_container_enabled" {
  description = "Create the separate artifact container and its scoped role assignments."
  type        = bool
  default     = false
}

variable "artifact_uploader_object_ids" {
  description = "Principals allowed to upload to the artifact container."
  type        = list(string)
  default     = []
}

variable "artifact_reader_object_ids" {
  description = "Runner principals allowed to read the artifact container, but not assigned state or artifact write access by this root."
  type        = list(string)
  default     = []

  validation {
    condition = length(setintersection(
      toset([for id in var.artifact_reader_object_ids : lower(id)]),
      toset([for id in concat(var.artifact_uploader_object_ids, var.state_contributor_object_ids, var.bootstrap_state_contributor_object_ids) : lower(id)])
    )) == 0
    error_message = "Artifact readers must not be assigned artifact upload or state access by this root."
  }
}

variable "allowed_ip_ranges" {
  description = "Public egress ranges of the runners allowed to reach the state account while its public endpoint is on (phase 1)."
  type        = list(string)
  default     = []
}

variable "state_public_network_access_enabled" {
  description = "Exception NX-3: keep the state account's IP-restricted public endpoint (phase 1). Set false in phase 2, after the private endpoint resolves from the runner."
  type        = bool
  default     = true

  validation {
    condition     = var.state_public_network_access_enabled || (var.state_private_endpoint_subnet_id != null && var.state_private_dns_zone_id != null)
    error_message = "Disable public access only with state_private_endpoint_subnet_id and state_private_dns_zone_id set (phase 2)."
  }
}

variable "state_network_security_perimeter_enabled" {
  description = "Admit allowed_ip_ranges through a Network Security Perimeter (publicNetworkAccess SecuredByPerimeter) instead of storage IP rules, which the tenant policy overrides."
  type        = bool
  default     = false

  validation {
    condition     = !var.state_network_security_perimeter_enabled || (var.state_public_network_access_enabled && length(var.allowed_ip_ranges) > 0)
    error_message = "The perimeter needs state_public_network_access_enabled and at least one allowed_ip_ranges entry."
  }
}

variable "state_private_endpoint_subnet_id" {
  description = "Phase 2: subnet for the state account's blob private endpoint (workload output private_endpoint_subnet_id)."
  type        = string
  default     = null
}

variable "state_private_dns_zone_id" {
  description = "Phase 2: privatelink.blob.core.windows.net zone id (workload output private_dns_zone_ids.blob)."
  type        = string
  default     = null

  validation {
    condition     = (var.state_private_dns_zone_id == null) == (var.state_private_endpoint_subnet_id == null)
    error_message = "Set state_private_endpoint_subnet_id and state_private_dns_zone_id together."
  }
}

variable "tags" {
  description = "Tags for every resource. owner and cost-center are mandatory."
  type        = map(string)

  validation {
    condition     = alltrue([for k in ["owner", "cost-center"] : contains(keys(var.tags), k) && trimspace(lookup(var.tags, k, "")) != ""])
    error_message = "tags must include non-empty owner and cost-center."
  }
}
