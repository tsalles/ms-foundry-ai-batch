provider "azurerm" {
  features {
    key_vault {
      # Purge protection is on; never purge on destroy.
      purge_soft_delete_on_destroy = false
    }
  }

  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id

  # Data-plane calls (if any) use Entra ID; storage keys are disabled.
  storage_use_azuread = true

  # Provider registration is a separately approved platform step (US-7.5 AC3), never implicit.
  resource_provider_registrations = "none"
}

provider "azapi" {
  subscription_id = var.subscription_id
  tenant_id       = var.tenant_id
}
