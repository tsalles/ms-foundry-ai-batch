resource "azurerm_key_vault" "this" {
  name                = var.name
  resource_group_name = var.resource_group_name
  location            = var.location
  tenant_id           = var.tenant_id
  sku_name            = "standard"

  # RBAC only, no access policies (azurerm 5.x argument name).
  rbac_authorization_enabled    = true
  soft_delete_retention_days    = var.soft_delete_retention_days
  purge_protection_enabled      = true
  public_network_access_enabled = false

  # No trusted-service exception: the app and the runner use the private endpoint (ADR 0010).
  network_acls {
    default_action = "Deny"
    bypass         = "None"
  }

  tags = var.tags

  # Signing secrets live here; teardown removes this guard in a reviewed change (runbook).
  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_private_endpoint" "vault" {
  name                = "pe-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.name}"
    private_connection_resource_id = azurerm_key_vault.this.id
    subresource_names              = ["vault"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [var.private_dns_zone_id]
  }
}
