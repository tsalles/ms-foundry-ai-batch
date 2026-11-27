output "resource_group_name" {
  description = "Resource group shared with the workload root."
  value       = azurerm_resource_group.this.name
}

output "state_storage_account_name" {
  description = "State account for backend.hcl."
  value       = azapi_resource.state.name
}

output "state_container_name" {
  description = "Workload state container (infra/envs/poc backend.hcl)."
  value       = azapi_resource.tfstate.name
}

output "bootstrap_state_container_name" {
  description = "Bootstrap state container (bootstrap.backend.hcl)."
  value       = azapi_resource.tfstate_bootstrap.name
}

output "artifact_container_name" {
  description = "Artifact container name, or null when disabled."
  value       = one(azapi_resource.artifacts[*].name)
}

output "state_private_endpoint_id" {
  description = "Private endpoint of the state account (null before phase 2)."
  value       = one(azurerm_private_endpoint.state[*].id)
}

output "state_public_network_access_enabled" {
  description = "Whether the state account still has its IP-restricted public endpoint (exception NX-3)."
  value       = var.state_public_network_access_enabled
}
