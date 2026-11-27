output "id" {
  description = "Function App resource id."
  value       = azapi_resource.site.id
}

output "name" {
  description = "Function App name."
  value       = azapi_resource.site.name
}

output "default_hostname" {
  description = "Default host name (receiver and demo API routes live under /api)."
  value       = azapi_resource.site.output.properties.defaultHostName
}

output "identity_principal_id" {
  description = "Object id of the app's user-assigned identity (for role assignments)."
  value       = azurerm_user_assigned_identity.app.principal_id
}

output "identity_client_id" {
  description = "Client id of the app's user-assigned identity."
  value       = azurerm_user_assigned_identity.app.client_id
}

output "app_setting_names" {
  description = "Names (not values) of the configured app settings, for tests and reviews."
  value       = local.setting_names
}

output "private_endpoint_id" {
  description = "Inbound private endpoint of the app."
  value       = azurerm_private_endpoint.site.id
}

output "public_network_access_enabled" {
  description = "Whether the app keeps its public endpoint (exception NX-1)."
  value       = var.public_network_access_enabled
}
