variable "name" {
  description = "Function App name (32 characters or fewer to avoid host id collisions)."
  type        = string

  validation {
    condition     = length(var.name) <= 32 && can(regex("^[a-z0-9][a-z0-9-]*[a-z0-9]$", var.name))
    error_message = "Use at most 32 lowercase letters, digits and hyphens."
  }
}

variable "resource_group_name" {
  description = "Resource group of the app."
  type        = string
}

variable "resource_group_id" {
  description = "Resource group id (parent of the azapi site)."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "runtime_version" {
  description = "Python version on Flex Consumption."
  type        = string
  default     = "3.11"
}

variable "maximum_instance_count" {
  description = "Scale-out ceiling (bounds the burst load on storage and the provider)."
  type        = number
  default     = 40

  validation {
    condition     = var.maximum_instance_count >= 1 && var.maximum_instance_count <= 1000
    error_message = "maximum_instance_count must be between 1 and 1000."
  }
}

variable "instance_memory_in_mb" {
  description = "Instance memory size."
  type        = number
  default     = 2048

  validation {
    condition     = contains([512, 2048, 4096], var.instance_memory_in_mb)
    error_message = "Flex Consumption instance memory is 512, 2048 or 4096 MB."
  }
}

variable "storage_account_name" {
  description = "Runtime storage account (host storage, queues, tables, deployment container)."
  type        = string
}

variable "storage_blob_endpoint" {
  description = "Blob endpoint of the runtime storage account (ends with /)."
  type        = string
}

variable "deployment_container_name" {
  description = "Blob container that holds the deployment package."
  type        = string
}

variable "subnet_id" {
  description = "Subnet delegated to Microsoft.App/environments for VNet integration."
  type        = string
}

variable "application_insights_connection_string" {
  description = "Application Insights connection string (not a credential when local auth is disabled)."
  type        = string
  sensitive   = true
}

variable "api_version" {
  description = "ARM API version for Microsoft.Web/sites (Flex Consumption functionAppConfig)."
  type        = string
  default     = "2025-03-01"
}

variable "public_network_access_enabled" {
  description = "Keep the public endpoint for webhook delivery (exception NX-1). false: private endpoint only (polling-only reconciliation)."
  type        = bool
  default     = true
}

variable "webhook_source_service_tags" {
  description = "Service tags allowed on the public endpoint (empty: no source restriction). Set once the provider's source ranges are known (Q-P01)."
  type        = list(string)
  default     = []

  validation {
    condition     = length(var.webhook_source_service_tags) <= 20 && alltrue([for tag in var.webhook_source_service_tags : can(regex("^[A-Za-z][A-Za-z0-9.]*$", tag))])
    error_message = "Use at most 20 service tag names such as AzureCloud or AzureCloud.eastus2."
  }
}

variable "outbound_route_all" {
  description = "Route all outbound traffic (app and configuration, including managed identity tokens) through the VNet integration."
  type        = bool
  default     = true
}

variable "private_endpoint_subnet_id" {
  description = "Subnet for the app's inbound private endpoint."
  type        = string
}

variable "private_dns_zone_id" {
  description = "privatelink.azurewebsites.net zone id for the app's private endpoint."
  type        = string
}

variable "app_settings" {
  description = "Application settings (the configuration contract in docs/configuration.md)."
  type        = map(string)
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}
