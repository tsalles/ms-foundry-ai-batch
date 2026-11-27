data "azurerm_client_config" "current" {}

# The bootstrap root owns the resource group; this root only references it (US-7.6 AC1).
data "azurerm_resource_group" "this" {
  name = var.resource_group_name
}

# Refuse to plan against the wrong tenant/subscription or region (US-7.1 AC3, US-7.5).
resource "terraform_data" "target_guard" {
  input = {
    subscription_id = var.subscription_id
    location        = var.location
  }

  lifecycle {
    precondition {
      condition     = data.azurerm_client_config.current.tenant_id == var.tenant_id && data.azurerm_client_config.current.subscription_id == var.subscription_id
      error_message = "Signed-in tenant/subscription do not match tenant_id/subscription_id. Stop and check the target (REQ-I08)."
    }

    precondition {
      condition     = data.azurerm_resource_group.this.location == var.location
      error_message = "The bootstrap resource group is in a different region than var.location."
    }

    precondition {
      condition     = length(local.names.key_vault) <= 24 && length(local.names.storage_account) <= 24 && length(local.names.function_app) <= 32
      error_message = "Derived names exceed Azure limits; shorten workload or environment."
    }
  }
}

module "network" {
  source = "../../modules/network"

  name_suffix                    = local.name_suffix
  resource_group_name            = data.azurerm_resource_group.this.name
  location                       = var.location
  address_space                  = var.vnet_address_space
  function_subnet_prefix         = var.function_subnet_prefix
  private_endpoint_subnet_prefix = var.private_endpoint_subnet_prefix
  additional_dns_link_vnet_ids   = var.runner_vnet_ids
  monitor_private_link_enabled   = var.monitor_private_link_enabled
  existing_private_dns_zone_ids  = var.existing_private_dns_zone_ids
  function_egress_lockdown       = var.function_egress_lockdown
  egress_next_hop_ip             = var.egress_next_hop_ip
  tags                           = local.tags
}

module "storage" {
  source = "../../modules/storage"

  name                       = local.names.storage_account
  resource_group_name        = data.azurerm_resource_group.this.name
  resource_group_id          = data.azurerm_resource_group.this.id
  location                   = var.location
  containers                 = toset([local.deploy_container])
  queues                     = toset(local.queues)
  tables                     = toset(local.tables)
  private_endpoint_subnet_id = module.network.private_endpoint_subnet_id
  private_dns_zone_ids       = module.network.private_dns_zone_ids
  tags                       = local.tags
}

module "key_vault" {
  source = "../../modules/key_vault"

  name                       = local.names.key_vault
  resource_group_name        = data.azurerm_resource_group.this.name
  location                   = var.location
  tenant_id                  = var.tenant_id
  private_endpoint_subnet_id = module.network.private_endpoint_subnet_id
  private_dns_zone_id        = module.network.private_dns_zone_ids["vault"]
  tags                       = local.tags
}

module "foundry" {
  source   = "../../modules/foundry"
  for_each = var.foundry_resources

  name                       = local.names.foundry[each.key]
  resource_group_name        = data.azurerm_resource_group.this.name
  location                   = coalesce(each.value.location, var.location)
  kind                       = each.value.kind
  local_auth_enabled         = each.value.local_auth_enabled
  dynamic_throttling_enabled = each.value.dynamic_throttling_enabled
  deployments                = each.value.deployments
  private_endpoint_subnet_id = module.network.private_endpoint_subnet_id
  private_endpoint_location  = module.network.location
  private_dns_zone_ids       = module.network.private_dns_zone_ids
  tags                       = local.tags
}

module "monitoring" {
  source = "../../modules/monitoring"

  name_suffix                 = local.name_suffix
  resource_group_name         = data.azurerm_resource_group.this.name
  resource_group_id           = data.azurerm_resource_group.this.id
  location                    = var.location
  retention_in_days           = var.log_retention_days
  alert_email_receivers       = var.alert_email_receivers
  signature_failure_threshold = var.signature_failure_threshold
  sweep_lag_threshold_seconds = var.sweep_lag_threshold_seconds
  private_link_enabled        = var.monitor_private_link_enabled
  public_query_enabled        = var.monitor_public_query_enabled
  private_endpoint_subnet_id  = module.network.private_endpoint_subnet_id
  private_endpoint_location   = module.network.location
  private_dns_zone_ids        = module.network.private_dns_zone_ids
  tags                        = local.tags

  diagnostic_targets = merge(
    {
      key-vault     = { resource_id = module.key_vault.id }
      storage-blob  = { resource_id = module.storage.service_ids.blob }
      storage-queue = { resource_id = module.storage.service_ids.queue }
      storage-table = { resource_id = module.storage.service_ids.table }
      function-app  = { resource_id = module.function_app.id }
    },
    { for key in keys(var.foundry_resources) : "foundry-${key}" => { resource_id = module.foundry[key].id } },
  )
}

module "function_app" {
  source = "../../modules/function_app"

  name                                   = local.names.function_app
  resource_group_name                    = data.azurerm_resource_group.this.name
  resource_group_id                      = data.azurerm_resource_group.this.id
  location                               = var.location
  runtime_version                        = var.function_runtime_version
  maximum_instance_count                 = var.function_maximum_instance_count
  instance_memory_in_mb                  = var.function_instance_memory_mb
  storage_account_name                   = module.storage.name
  storage_blob_endpoint                  = module.storage.primary_blob_endpoint
  deployment_container_name              = local.deploy_container
  subnet_id                              = module.network.function_subnet_id
  public_network_access_enabled          = var.function_public_ingress
  webhook_source_service_tags            = var.webhook_source_service_tags
  outbound_route_all                     = var.function_outbound_route_all
  private_endpoint_subnet_id             = module.network.private_endpoint_subnet_id
  private_dns_zone_id                    = module.network.private_dns_zone_ids["sites"]
  application_insights_connection_string = module.monitoring.application_insights_connection_string
  app_settings                           = local.app_settings
  tags                                   = local.tags

  depends_on = [terraform_data.target_guard]
}
