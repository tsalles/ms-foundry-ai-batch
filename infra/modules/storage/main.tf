# The account is created through ARM with azapi, not azurerm_storage_account: the azurerm resource
# calls listKeys on every read and would store account keys in state even with shared keys
# disabled (US-2.2, US-7.6 AC4).
resource "azapi_resource" "account" {
  type      = "Microsoft.Storage/storageAccounts@2024-01-01"
  name      = var.name
  location  = var.location
  parent_id = var.resource_group_id
  tags      = var.tags

  body = {
    kind = "StorageV2"
    sku = {
      name = "Standard_${var.replication_type}"
    }
    properties = {
      # Identity-only data access (REQ-I10): no shared keys, no SAS, no public data plane.
      allowSharedKeyAccess         = false
      defaultToOAuthAuthentication = true
      publicNetworkAccess          = "Disabled"
      allowBlobPublicAccess        = false
      allowCrossTenantReplication  = false
      supportsHttpsTrafficOnly     = true
      minimumTlsVersion            = "TLS1_2"
      encryption = {
        keySource                       = "Microsoft.Storage"
        requireInfrastructureEncryption = true
        services = {
          blob = { enabled = true }
          file = { enabled = true }
        }
      }
      # No trusted-service exception: every data-plane caller uses a private endpoint (ADR 0010).
      networkAcls = {
        defaultAction = "Deny"
        bypass        = "None"
      }
    }
  }

  response_export_values = ["properties.primaryEndpoints"]
}

# Azure creates the singleton `default` blob service with the account, so it can only be updated.
resource "azapi_update_resource" "blob_service" {
  type        = "Microsoft.Storage/storageAccounts/blobServices@2024-01-01"
  resource_id = "${azapi_resource.account.id}/blobServices/default"

  body = {
    properties = {
      isVersioningEnabled = true
      deleteRetentionPolicy = {
        enabled = true
        days    = var.blob_soft_delete_days
      }
      containerDeleteRetentionPolicy = {
        enabled = true
        days    = var.blob_soft_delete_days
      }
    }
  }
}

# Containers, queues and tables also go through ARM, so Terraform never needs the data plane of
# an account whose public endpoint is disabled.
resource "azapi_resource" "container" {
  for_each = var.containers

  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2024-01-01"
  name      = each.value
  parent_id = azapi_update_resource.blob_service.id

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

resource "azapi_resource" "queue" {
  for_each = var.queues

  type      = "Microsoft.Storage/storageAccounts/queueServices/queues@2024-01-01"
  name      = each.value
  parent_id = "${azapi_resource.account.id}/queueServices/default"

  body = {
    properties = {
      metadata = {}
    }
  }
}

resource "azapi_resource" "table" {
  for_each = var.tables

  type      = "Microsoft.Storage/storageAccounts/tableServices/tables@2024-01-01"
  name      = each.value
  parent_id = "${azapi_resource.account.id}/tableServices/default"

  body = {
    properties = {}
  }
}

resource "azurerm_private_endpoint" "this" {
  for_each = toset(["blob", "queue", "table"])

  name                = "pe-${var.name}-${each.value}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.name}-${each.value}"
    private_connection_resource_id = azapi_resource.account.id
    subresource_names              = [each.value]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [var.private_dns_zone_ids[each.value]]
  }
}
