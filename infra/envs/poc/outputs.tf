# No output carries a key, secret or connection string.
output "environment" {
  description = "Environment name (BATCH_ENVIRONMENT); the CLI settings and Key Vault secret names use it."
  value       = var.environment
}

output "function_app_name" {
  description = "Function App name (package deployment target)."
  value       = module.function_app.name
}

output "function_app_default_hostname" {
  description = "Function App host name."
  value       = module.function_app.default_hostname
}

output "webhook_receiver_base_url" {
  description = "Base URL passed to `foundry-batch register ensure --receiver-base-url`."
  value       = "https://${module.function_app.default_hostname}/api/webhooks"
}

output "webhook_receiver_urls" {
  description = "Receiver URL per Foundry resource key; empty without public ingress (polling-only: the pipeline registers no webhook)."
  value       = { for key in keys(var.foundry_resources) : key => "https://${module.function_app.default_hostname}/api/webhooks/${key}" if var.function_public_ingress }
}

output "webhook_ingress_enabled" {
  description = "Whether the provider can deliver webhooks (exception NX-1)."
  value       = var.function_public_ingress
}

output "network_exceptions" {
  description = "Public paths this configuration keeps (ADR 0010 register); everything else is private."
  value       = local.network_exceptions
}

output "private_endpoint_subnet_id" {
  description = "Private endpoint subnet (bootstrap phase 2: state account private endpoint)."
  value       = module.network.private_endpoint_subnet_id
}

output "runner_vm_id" {
  description = "Private runner VM ID, or null when the runner is disabled."
  value       = try(azurerm_linux_virtual_machine.runner[0].id, null)
}

output "runner_identity_principal_id" {
  description = "Runner managed identity principal ID for separately reviewed role assignments."
  value       = try(azurerm_user_assigned_identity.runner[0].principal_id, null)
}

output "runner_identity_client_id" {
  description = "Runner managed identity client ID for explicitly selecting its identity."
  value       = try(azurerm_user_assigned_identity.runner[0].client_id, null)
}

output "private_dns_zone_ids" {
  description = "Private DNS zone ids by key (bootstrap phase 2 uses blob)."
  value       = module.network.private_dns_zone_ids
}

output "resources_json" {
  description = "Value of BATCH_RESOURCES_JSON (also used by pipeline steps)."
  value       = jsonencode(local.resources)
}

output "key_vault_uri" {
  description = "Key Vault URI (registration script target)."
  value       = module.key_vault.vault_uri
}

output "storage_account_name" {
  description = "Runtime storage account."
  value       = module.storage.name
}

output "queue_service_uri" {
  description = "Queue endpoint for CLI settings."
  value       = trimsuffix(module.storage.primary_queue_endpoint, "/")
}

output "table_service_uri" {
  description = "Table endpoint for CLI settings."
  value       = trimsuffix(module.storage.primary_table_endpoint, "/")
}

output "log_analytics_workspace_id" {
  description = "Workspace with app traces, platform logs and alerts."
  value       = module.monitoring.log_analytics_workspace_id
}

output "application_insights_name" {
  description = "Application Insights resource name."
  value       = module.monitoring.application_insights_name
}

output "app_setting_names" {
  description = "Names of the Function App settings (values are not output)."
  value       = module.function_app.app_setting_names
}

output "alert_names" {
  description = "Alert rule names."
  value       = module.monitoring.alert_names
}
