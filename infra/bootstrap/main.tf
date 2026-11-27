locals {
  unique     = substr(sha1("${var.subscription_id}/${var.resource_group_name}/tfstate"), 0, 8)
  state_name = "sttfaibatch${local.unique}"
  tags       = merge(var.tags, { managed-by = "terraform", component = "terraform-state" })

  # The tenant's StorageAccount_PublicNetwork_Modify policy rewrites any value other than
  # Disabled or SecuredByPerimeter, so IP rules alone never reach the data plane.
  state_public_network_access = (
    var.state_network_security_perimeter_enabled ? "SecuredByPerimeter" :
    var.state_public_network_access_enabled ? "Enabled" : "Disabled"
  )
  perimeter_prefixes = [for ip in var.allowed_ip_ranges : strcontains(ip, "/") ? ip : "${ip}/32"]
}

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.location
  tags     = var.tags

  lifecycle {
    prevent_destroy = true
  }
}

# azapi keeps account keys out of state (azurerm_storage_account reads them on every refresh).
resource "azapi_resource" "state" {
  type      = "Microsoft.Storage/storageAccounts@2024-01-01"
  name      = local.state_name
  location  = var.location
  parent_id = azurerm_resource_group.this.id
  tags      = local.tags

  body = {
    kind = "StorageV2"
    sku = {
      name = "Standard_ZRS"
    }
    properties = {
      allowSharedKeyAccess         = false
      defaultToOAuthAuthentication = true
      allowBlobPublicAccess        = false
      allowCrossTenantReplication  = false
      supportsHttpsTrafficOnly     = true
      minimumTlsVersion            = "TLS1_2"
      # Phase 1: public endpoint restricted to runner IPs (exception NX-3, US-7.6 AC3).
      # Phase 2 (ADR 0010, infra/README.md): private endpoint, then public access disabled.
      publicNetworkAccess = local.state_public_network_access
      networkAcls = {
        defaultAction       = "Deny"
        bypass              = "None"
        ipRules             = local.state_public_network_access == "Enabled" ? [for ip in var.allowed_ip_ranges : { value = ip, action = "Allow" }] : []
        virtualNetworkRules = []
      }
      encryption = {
        keySource                       = "Microsoft.Storage"
        requireInfrastructureEncryption = true
        services = {
          blob = { enabled = true }
          file = { enabled = true }
        }
      }
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Perimeter that admits only the approved desktop IPs; private endpoint traffic needs no rule.
resource "azapi_resource" "perimeter" {
  count = var.state_network_security_perimeter_enabled ? 1 : 0

  type      = "Microsoft.Network/networkSecurityPerimeters@2024-07-01"
  name      = "nsp-${var.resource_group_name}-state"
  location  = var.location
  parent_id = azurerm_resource_group.this.id
  tags      = local.tags
  body      = { properties = {} }
}

resource "azapi_resource" "perimeter_profile" {
  count = var.state_network_security_perimeter_enabled ? 1 : 0

  type      = "Microsoft.Network/networkSecurityPerimeters/profiles@2024-07-01"
  name      = "state"
  parent_id = azapi_resource.perimeter[0].id
  body      = { properties = {} }
}

resource "azapi_resource" "perimeter_desktop_inbound" {
  count = var.state_network_security_perimeter_enabled ? 1 : 0

  type      = "Microsoft.Network/networkSecurityPerimeters/profiles/accessRules@2024-07-01"
  name      = "desktop-inbound"
  parent_id = azapi_resource.perimeter_profile[0].id

  body = {
    properties = {
      direction                 = "Inbound"
      addressPrefixes           = local.perimeter_prefixes
      fullyQualifiedDomainNames = []
      subscriptions             = []
    }
  }
}

resource "azapi_resource" "perimeter_state" {
  count = var.state_network_security_perimeter_enabled ? 1 : 0

  type      = "Microsoft.Network/networkSecurityPerimeters/resourceAssociations@2024-07-01"
  name      = "state-account"
  parent_id = azapi_resource.perimeter[0].id

  body = {
    properties = {
      accessMode          = "Enforced"
      privateLinkResource = { id = azapi_resource.state.id }
      profile             = { id = azapi_resource.perimeter_profile[0].id }
    }
  }

  depends_on = [azapi_resource.perimeter_desktop_inbound]
}

# Azure creates the singleton `default` blob service with the account, so it can only be updated.
resource "azapi_update_resource" "state_blob_service" {
  type        = "Microsoft.Storage/storageAccounts/blobServices@2024-01-01"
  resource_id = "${azapi_resource.state.id}/blobServices/default"
  body = {
    properties = {
      isVersioningEnabled = true
      changeFeed = {
        enabled         = true
        retentionInDays = 90
      }
      deleteRetentionPolicy = {
        enabled = true
        days    = 30
      }
      containerDeleteRetentionPolicy = {
        enabled = true
        days    = 30
      }
    }
  }
}

# Workload state (infra/envs/poc). The pipeline identity writes it.
resource "azapi_resource" "tfstate" {
  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2024-01-01"
  name      = "tfstate"
  parent_id = azapi_update_resource.state_blob_service.id

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

# Bootstrap state (this root: RG, state account, role assignments) in its own container, so the
# workload identity cannot rewrite the state that grants it access.
resource "azapi_resource" "tfstate_bootstrap" {
  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2024-01-01"
  name      = "tfstate-bootstrap"
  parent_id = azapi_update_resource.state_blob_service.id

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

resource "azapi_resource" "artifacts" {
  count = var.artifact_container_enabled ? 1 : 0

  type      = "Microsoft.Storage/storageAccounts/blobServices/containers@2024-01-01"
  name      = "artifacts"
  parent_id = azapi_update_resource.state_blob_service.id

  body = {
    properties = {
      publicAccess = "None"
    }
  }
}

resource "azurerm_role_assignment" "artifact_uploader" {
  for_each = var.artifact_container_enabled ? toset(var.artifact_uploader_object_ids) : toset([])

  scope                = azapi_resource.artifacts[0].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "artifact_reader" {
  for_each = var.artifact_container_enabled ? toset(var.artifact_reader_object_ids) : toset([])

  scope                = azapi_resource.artifacts[0].id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "state" {
  for_each = toset(var.state_contributor_object_ids)

  scope                = azapi_resource.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "bootstrap_state" {
  for_each = toset(var.bootstrap_state_contributor_object_ids)

  scope                = azapi_resource.tfstate_bootstrap.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = each.value
}

# Phase 2: the state account's blob endpoint on the workload's private endpoint subnet, registered
# in the workload's blob zone (both are outputs of infra/envs/poc). Created while public access is
# still on; disable it in a second apply once the runner resolves the private address.
resource "azurerm_private_endpoint" "state" {
  count = var.state_private_endpoint_subnet_id == null ? 0 : 1

  name                = "pe-${local.state_name}-blob"
  resource_group_name = azurerm_resource_group.this.name
  location            = var.location
  subnet_id           = var.state_private_endpoint_subnet_id
  tags                = local.tags

  private_service_connection {
    name                           = "psc-${local.state_name}-blob"
    private_connection_resource_id = azapi_resource.state.id
    subresource_names              = ["blob"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [var.state_private_dns_zone_id]
  }
}
