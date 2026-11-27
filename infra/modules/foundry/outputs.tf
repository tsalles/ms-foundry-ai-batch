output "id" {
  description = "Account resource id."
  value       = azurerm_cognitive_account.this.id
}

output "name" {
  description = "Account name."
  value       = azurerm_cognitive_account.this.name
}

output "openai_endpoint" {
  description = "Endpoint form used by the Batch and webhook APIs (CON-B14)."
  value       = "https://${azurerm_cognitive_account.this.custom_subdomain_name}.openai.azure.com/"
}

output "deployment_names" {
  description = "Batch deployment names."
  value       = sort(keys(azurerm_cognitive_deployment.batch))
}

output "principal_id" {
  description = "System-assigned identity of the account (for a later Blob Storage assessment, SP-7)."
  value       = azurerm_cognitive_account.this.identity[0].principal_id
}
