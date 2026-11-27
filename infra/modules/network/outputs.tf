output "location" {
  description = "Region of the virtual network; private endpoints on its subnet must use it."
  value       = azurerm_virtual_network.this.location
}

output "vnet_id" {
  description = "Workload virtual network id."
  value       = azurerm_virtual_network.this.id
}

output "function_subnet_id" {
  description = "Subnet delegated to the Function App VNet integration."
  value       = azurerm_subnet.function.id
}

output "private_endpoint_subnet_id" {
  description = "Subnet for private endpoints."
  value       = azurerm_subnet.private_endpoints.id
}

output "private_dns_zone_ids" {
  description = "Private DNS zone ids by key (blob, queue, table, vault, cognitiveservices, openai, aiservices, sites and, with the AMPLS, monitor, oms, ods, agentsvc): created zones plus existing ones."
  value       = merge({ for key, zone in azurerm_private_dns_zone.this : key => zone.id }, var.existing_private_dns_zone_ids)
}

output "created_private_dns_zone_names" {
  description = "Zones this module created (tests and reviews)."
  value       = sort([for zone in azurerm_private_dns_zone.this : zone.name])
}

output "function_egress_rule_names" {
  description = "Outbound NSG rules on the Function App subnet (tests and reviews)."
  value       = sort(keys(azurerm_network_security_rule.function_egress))
}
