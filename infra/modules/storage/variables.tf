variable "name" {
  description = "Storage account name (3-24 lowercase letters and digits, globally unique)."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,24}$", var.name))
    error_message = "Storage account names are 3-24 lowercase letters and digits."
  }
}

variable "resource_group_name" {
  description = "Resource group of the storage account."
  type        = string
}

variable "resource_group_id" {
  description = "Resource group id (parent of the azapi storage account)."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "replication_type" {
  description = "Replication (LRS for the PoC; ZRS or GZRS for production)."
  type        = string
  default     = "LRS"

  validation {
    condition     = contains(["LRS", "ZRS", "GRS", "GZRS"], var.replication_type)
    error_message = "replication_type must be LRS, ZRS, GRS or GZRS."
  }
}

variable "containers" {
  description = "Blob containers to create (for example the Flex Consumption deployment container)."
  type        = set(string)
}

variable "queues" {
  description = "Queues to create, including their -poison companions."
  type        = set(string)
}

variable "tables" {
  description = "Tables to create."
  type        = set(string)
}

variable "private_endpoint_subnet_id" {
  description = "Subnet for the blob, queue and table private endpoints."
  type        = string
}

variable "private_dns_zone_ids" {
  description = "Private DNS zone ids keyed blob, queue and table."
  type        = map(string)
}

variable "blob_soft_delete_days" {
  description = "Blob and container soft-delete retention in days."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}
