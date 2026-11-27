output "log_analytics_workspace_id" {
  description = "Log Analytics workspace id."
  value       = azapi_resource.workspace.id
}

output "application_insights_id" {
  description = "Application Insights id (scope for Monitoring Metrics Publisher)."
  value       = azurerm_application_insights.this.id
}

output "application_insights_name" {
  description = "Application Insights name."
  value       = azurerm_application_insights.this.name
}

output "application_insights_connection_string" {
  description = "Connection string for the app (ingestion still requires Entra auth)."
  value       = azurerm_application_insights.this.connection_string
  sensitive   = true
}

output "alert_names" {
  description = "Scheduled query alert rule names."
  value       = sort([for a in azurerm_monitor_scheduled_query_rules_alert_v2.this : a.name])
}

output "private_link_scope_id" {
  description = "AMPLS id (null without the private link)."
  value       = var.private_link_enabled ? azapi_resource.private_link_scope[0].id : null
}
