resource "azurerm_user_assigned_identity" "app" {
  name                = "id-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
}

resource "azurerm_service_plan" "this" {
  name                = "asp-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  os_type             = "Linux"
  sku_name            = "FC1"
  tags                = var.tags
}

locals {
  # Identity-based host storage and the queue-trigger connection (BATCH_STORAGE) share the app's
  # user-assigned identity. No setting carries a key.
  platform_settings = {
    "AzureWebJobsStorage__accountName" = var.storage_account_name
    "AzureWebJobsStorage__credential"  = "managedidentity"
    "AzureWebJobsStorage__clientId"    = azurerm_user_assigned_identity.app.client_id
    "AZURE_CLIENT_ID"                  = azurerm_user_assigned_identity.app.client_id
    "BATCH_STORAGE__credential"        = "managedidentity"
    "BATCH_STORAGE__clientId"          = azurerm_user_assigned_identity.app.client_id
    # Entra-only ingestion because local authentication is disabled on Application Insights.
    "APPLICATIONINSIGHTS_AUTHENTICATION_STRING" = "Authorization=AAD;ClientId=${azurerm_user_assigned_identity.app.client_id}"
  }

  # The connection string is marked sensitive by the provider, which makes the merged map
  # sensitive too; setting names are therefore listed from the non-sensitive parts.
  settings = merge(
    var.app_settings,
    local.platform_settings,
    { "APPLICATIONINSIGHTS_CONNECTION_STRING" = var.application_insights_connection_string },
  )
  setting_names = sort(concat(
    keys(var.app_settings),
    keys(local.platform_settings),
    ["APPLICATIONINSIGHTS_CONNECTION_STRING"],
  ))
}

# The app is created through ARM (azapi) instead of azurerm_function_app_flex_consumption: with
# identity-based storage that resource injects a key-less AzureWebJobsStorage connection string
# that the current Functions runtime rejects, and it does not add the __accountName/__credential
# settings (provider issues #33211 and #30732).
resource "azapi_resource" "site" {
  type      = "Microsoft.Web/sites@${var.api_version}"
  name      = var.name
  location  = var.location
  parent_id = var.resource_group_id
  tags      = var.tags

  ignore_casing = true # the service normalizes the casing of serverFarmId

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.app.id]
  }

  body = {
    kind       = "functionapp,linux"
    properties = merge(local.site_properties, local.outbound_routing)
  }

  # VERIFY: Microsoft's azapi Flex sample disables schema validation for functionAppConfig.
  schema_validation_enabled = false
  response_export_values    = ["properties.defaultHostName"]
}

locals {
  # VERIFY on Flex: outboundVnetRouting.allTraffic routes application and configuration traffic
  # (including managed identity token requests) through the VNet. If the Flex resource provider
  # rejects the property, set outbound_route_all = false (Flex routes app traffic to private
  # endpoints through the integration either way).
  outbound_routing = { for key, value in { outboundVnetRouting = { allTraffic = true } } : key => value if var.outbound_route_all }

  site_properties = {
    serverFarmId = azurerm_service_plan.this.id
    httpsOnly    = true
    # Exception NX-1 (ADR 0010): while webhook ingress is on, the app keeps a public endpoint
    # because the provider only delivers to publicly reachable HTTPS URLs (CON-W05, ADR 0001).
    # Every other route needs a function key or a signature, and all internal callers (runner,
    # relay demo route, deployment) use the private endpoint below. Off: private-only app,
    # polling-only reconciliation.
    publicNetworkAccess    = var.public_network_access_enabled ? "Enabled" : "Disabled"
    virtualNetworkSubnetId = var.subnet_id
    siteConfig = {
      minTlsVersion = "1.2"
      ftpsState     = "Disabled"
      http20Enabled = true
      appSettings   = [for name in local.setting_names : { name = name, value = local.settings[name] }]
      # Public callers: everyone, or only the provider's service tags once Q-P01 names them.
      # Access restrictions do not apply to private endpoint traffic.
      ipSecurityRestrictionsDefaultAction = length(var.webhook_source_service_tags) > 0 ? "Deny" : "Allow"
      ipSecurityRestrictions = [
        for index, tag in var.webhook_source_service_tags : {
          name      = "webhook-source-${index}"
          action    = "Allow"
          tag       = "ServiceTag"
          ipAddress = tag
          priority  = 100 + index
        }
      ]
      # The deployment (SCM) endpoint is never public: deployments come from the private runner.
      scmIpSecurityRestrictionsUseMain       = false
      scmIpSecurityRestrictionsDefaultAction = "Deny"
    }
    functionAppConfig = {
      deployment = {
        storage = {
          type  = "blobContainer"
          value = "${var.storage_blob_endpoint}${var.deployment_container_name}"
          authentication = {
            type                           = "UserAssignedIdentity"
            userAssignedIdentityResourceId = azurerm_user_assigned_identity.app.id
          }
        }
      }
      scaleAndConcurrency = {
        maximumInstanceCount = var.maximum_instance_count
        instanceMemoryMB     = var.instance_memory_in_mb
      }
      runtime = {
        name    = "python"
        version = var.runtime_version
      }
    }
  }
}

# Inbound private path: registers <app>.privatelink.azurewebsites.net and the SCM host name, so
# the private runner deploys and tests, and the relay reaches the demo route, without the
# public endpoint.
resource "azurerm_private_endpoint" "site" {
  name                = "pe-${var.name}"
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = var.private_endpoint_subnet_id
  tags                = var.tags

  private_service_connection {
    name                           = "psc-${var.name}"
    private_connection_resource_id = azapi_resource.site.id
    subresource_names              = ["sites"]
    is_manual_connection           = false
  }

  private_dns_zone_group {
    name                 = "default"
    private_dns_zone_ids = [var.private_dns_zone_id]
  }
}

# No basic-auth publishing: deployments use Entra (OneDeploy with the pipeline identity).
resource "azapi_update_resource" "basic_auth" {
  for_each = toset(["ftp", "scm"])

  type      = "Microsoft.Web/sites/basicPublishingCredentialsPolicies@${var.api_version}"
  name      = each.value
  parent_id = azapi_resource.site.id

  body = {
    properties = {
      allow = false
    }
  }
}
