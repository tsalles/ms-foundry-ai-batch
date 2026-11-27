variable "name_suffix" {
  description = "Naming suffix shared by the workload, e.g. aibatch-poc-eus2."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group that holds the network resources."
  type        = string
}

variable "location" {
  description = "Azure region of the virtual network."
  type        = string
}

variable "address_space" {
  description = "Address space of the workload virtual network."
  type        = list(string)
}

variable "function_subnet_prefix" {
  description = "Prefix of the subnet delegated to Flex Consumption VNet integration (/27 or larger)."
  type        = string
}

variable "private_endpoint_subnet_prefix" {
  description = "Prefix of the subnet that holds the private endpoints."
  type        = string
}

variable "additional_dns_link_vnet_ids" {
  description = "Other VNets (for example the private build agent's) that must resolve the private endpoints."
  type        = map(string)
  default     = {}
}

variable "monitor_private_link_enabled" {
  description = "Create the Azure Monitor private DNS zones (AMPLS) and link them to the workload VNet only."
  type        = bool
  default     = true
}

variable "existing_private_dns_zone_ids" {
  description = <<-EOT
    Central private DNS zones to use instead of creating them, keyed like the private_dns_zone_ids
    output (blob, queue, table, vault, cognitiveservices, openai, aiservices, sites, monitor, oms,
    ods, agentsvc). The landing zone owns these zones and their VNet links.
  EOT
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key in keys(var.existing_private_dns_zone_ids) :
      contains(["blob", "queue", "table", "vault", "cognitiveservices", "openai", "aiservices", "sites", "monitor", "oms", "ods", "agentsvc"], key)
    ])
    error_message = "Unknown zone key in existing_private_dns_zone_ids."
  }
}

variable "function_egress_lockdown" {
  description = "Deny internet egress from the Function App subnet except Entra ID (and Azure Monitor without the AMPLS)."
  type        = bool
  default     = true
}

variable "egress_next_hop_ip" {
  description = "Private IP of the landing zone firewall for a 0.0.0.0/0 route on the Function App subnet (null: no route)."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to every taggable resource."
  type        = map(string)
}
