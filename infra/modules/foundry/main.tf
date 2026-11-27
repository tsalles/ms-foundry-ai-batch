resource "azurerm_cognitive_account" "this" {
  name                          = var.name
  resource_group_name           = var.resource_group_name
  location                      = var.location
  kind                          = var.kind
  sku_name                      = "S0"
  custom_subdomain_name         = var.name
  local_auth_enabled            = false
  public_network_access_enabled = var.public_network_access_enabled

  identity {
    type = "SystemAssigned"
  }

  # Private endpoint only; no trusted-service exception (ADR 0010). Webhook deliveries leave the
  # service outbound and are not affected.
  network_acls {
    default_action = "Deny"
    bypass         = "None"
  }

  tags = var.tags

  lifecycle {
    precondition {
      condition     = var.local_auth_enabled == false
      error_message = "Local authentication requires a separately approved D-04 change."
    }
  }
}

resource "azurerm_cognitive_deployment" "batch" {
  for_each = { for d in var.deployments : d.name => d }

  name                 = each.value.name
  cognitive_account_id = azurerm_cognitive_account.this.id

  # Pinned version: no silent model upgrades during the PoC.
  version_upgrade_option     = "NoAutoUpgrade"
  dynamic_throttling_enabled = var.dynamic_throttling_enabled

  model {
    format  = "OpenAI"
    name    = each.value.model_name
    version = each.value.model_version
  }

  # SKU and model force replacement when changed.
  # VERIFY: meaning of capacity for batch SKUs (enqueued tokens) against the preflight quota row.
  sku {
    name     = each.value.sku_name
    capacity = each.value.capacity
  }
}

# The endpoint lives in the VNet's region, which can differ from the account's (Azure requires
# the endpoint and its VNet in one region; the target resource may be elsewhere).
resource "azurerm_private_endpoint" "account" {
  name                = "pe-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.private_endpoint_location
  subnet_id           = var.private_endpoint_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.name}"
    private_connection_resource_id = azurerm_cognitive_account.this.id
    subresource_names              = ["account"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name = "default"
    private_dns_zone_ids = [
      var.private_dns_zone_ids["cognitiveservices"],
      var.private_dns_zone_ids["openai"],
      var.private_dns_zone_ids["aiservices"],
    ]
  }
}
