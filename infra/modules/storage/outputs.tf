locals {
  endpoints = azapi_resource.account.output.properties.primaryEndpoints
}

output "id" {
  description = "Storage account resource id."
  value       = azapi_resource.account.id
}

output "name" {
  description = "Storage account name."
  value       = azapi_resource.account.name
}

output "primary_blob_endpoint" {
  description = "Blob service endpoint (ends with /)."
  value       = local.endpoints.blob
}

output "primary_queue_endpoint" {
  description = "Queue service endpoint (ends with /)."
  value       = local.endpoints.queue
}

output "primary_table_endpoint" {
  description = "Table service endpoint (ends with /)."
  value       = local.endpoints.table
}

output "service_ids" {
  description = "Sub-service ids for diagnostic settings (blob, queue, table)."
  value = {
    blob  = "${azapi_resource.account.id}/blobServices/default"
    queue = "${azapi_resource.account.id}/queueServices/default"
    table = "${azapi_resource.account.id}/tableServices/default"
  }
}

output "queue_names" {
  description = "Queues created."
  value       = sort([for q in azapi_resource.queue : q.name])
}

output "table_names" {
  description = "Tables created."
  value       = sort([for t in azapi_resource.table : t.name])
}

output "table_ids" {
  description = "Table resource ids by name (scopes for table-level data roles)."
  value       = { for name, table in azapi_resource.table : name => table.id }
}

output "container_names" {
  description = "Blob containers created."
  value       = sort([for c in azapi_resource.container : c.name])
}
