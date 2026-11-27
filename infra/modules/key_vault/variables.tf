variable "name" {
  description = "Key Vault name (3-24 characters: letters, digits, hyphens; globally unique)."
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9-]{1,22}[a-zA-Z0-9]$", var.name))
    error_message = "Key Vault names are 3-24 characters, start with a letter and use letters, digits and hyphens."
  }
}

variable "resource_group_name" {
  description = "Resource group of the vault."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "tenant_id" {
  description = "Entra tenant of the vault."
  type        = string
}

variable "soft_delete_retention_days" {
  description = "Soft-delete retention (7-90 days)."
  type        = number
  default     = 90
}

variable "private_endpoint_subnet_id" {
  description = "Subnet for the vault's private endpoint."
  type        = string
}

variable "private_dns_zone_id" {
  description = "privatelink.vaultcore.azure.net zone id."
  type        = string
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}
