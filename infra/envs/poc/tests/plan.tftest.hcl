# terraform test (plan only, mocked providers): input validation, naming and the app contract.
# Run: terraform -chdir=infra/envs/poc init -backend=false && terraform -chdir=infra/envs/poc test

mock_provider "azurerm" {
  mock_data "azurerm_client_config" {
    defaults = {
      tenant_id       = "11111111-1111-1111-1111-111111111111"
      subscription_id = "22222222-2222-2222-2222-222222222222"
      object_id       = "33333333-3333-3333-3333-333333333333"
      client_id       = "44444444-4444-4444-4444-444444444444"
    }
  }

  mock_data "azurerm_resource_group" {
    defaults = {
      id       = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
      name     = "rg-foundry-batch-poc"
      location = "eastus2"
    }
  }
}

mock_provider "azapi" {}

variables {
  tenant_id       = "11111111-1111-1111-1111-111111111111"
  subscription_id = "22222222-2222-2222-2222-222222222222"
  tags = {
    owner       = "poc-lead"
    cost-center = "cc-test"
  }
  foundry_resources = {
    eus2 = {
      deployments = [
        { name = "batch-default", model_name = "gpt-test", model_version = "2025-01-01", capacity = 1 },
      ]
    }
  }
}

run "valid_configuration_plans" {
  command = plan

  assert {
    condition     = can(regex("^staibatchpoc[0-9a-f]{6}$", output.storage_account_name))
    error_message = "Storage account name does not follow the naming rule."
  }

  assert {
    condition     = can(regex("^func-aibatch-poc-[0-9a-f]{6}$", output.function_app_name))
    error_message = "Function App name does not follow the naming rule."
  }

  assert {
    condition = alltrue([
      for name in [
        "BATCH_ENVIRONMENT", "BATCH_RESOURCES_JSON", "BATCH_KEY_VAULT_URI", "BATCH_STORAGE__queueServiceUri",
        "BATCH_STORAGE__tableServiceUri", "BATCH_STORAGE__credential", "BATCH_STORAGE__clientId",
        "BATCH_INCOMING_QUEUE", "BATCH_RELAY_QUEUE", "BATCH_TABLE_JOBS", "BATCH_TABLE_CONTROL",
        "BATCH_TABLE_QUARANTINE", "BATCH_TABLE_SUBMISSIONS", "BATCH_TABLE_DEMO_RECEIPTS",
        "BATCH_DOWNSTREAM_MODE", "BATCH_RELAY_SIGNING_SECRET_NAME", "BATCH_SWEEP_SCHEDULE",
        "BATCH_OUTBOX_SCHEDULE", "BATCH_RECEIVER_RATE_PER_SECOND", "BATCH_RECEIVER_BURST",
        "AzureWebJobsStorage__accountName", "AzureWebJobsStorage__credential",
        "AZURE_CLIENT_ID", "APPLICATIONINSIGHTS_AUTHENTICATION_STRING", "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "FUNCTIONS_REQUEST_BODY_SIZE_LIMIT",
      ] : contains(output.app_setting_names, name)
    ])
    error_message = "An app setting required by the Python configuration contract is missing."
  }

  assert {
    condition     = !anytrue([for name in output.app_setting_names : can(regex("(?i)(accountkey|access_key|password|^AzureWebJobsStorage$)", name))])
    error_message = "No key-based or connection-string storage setting may be configured."
  }

  assert {
    condition     = jsondecode(output.resources_json)[0].key == "eus2" && startswith(jsondecode(output.resources_json)[0].endpoint, "https://oai-aibatch-poc-eus2-")
    error_message = "BATCH_RESOURCES_JSON must list each resource with its openai.azure.com endpoint."
  }

  assert {
    condition     = module.storage.queue_names == sort(["batch-events", "batch-events-poison", "relay-dispatch", "relay-dispatch-poison"])
    error_message = "Queues (and their poison queues) must match the app contract."
  }

  assert {
    condition     = module.storage.table_names == sort(["batchjobs", "batchcontrol", "batchquarantine", "batchsubmissions", "demoreceipts"])
    error_message = "Tables must match the app contract."
  }

  assert {
    condition     = length(module.monitoring.alert_names) == 8
    error_message = "Every planned alert rule must exist."
  }

  assert {
    condition     = output.environment == "poc"
    error_message = "The environment output feeds the CLI settings (secret names); it must match var.environment."
  }

  assert {
    condition     = length(azurerm_role_assignment.registration) == 0
    error_message = "No registration roles without a registration principal."
  }

  assert {
    condition     = output.runner_vm_id == null && length(azurerm_public_ip.runner_nat) == 0 && length(azurerm_user_assigned_identity.runner) == 0
    error_message = "The runner and its billable resources must be opt-in."
  }
}

run "private_runner_configuration" {
  command = plan
  variables {
    runner_enabled              = true
    runner_admin_ssh_public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUvp2ERuiFq8Z0ZthtsQ9C9Fd+nXsi4tdGOtBnHw4PgCi8k+bTBtbN1cDY0Sksbj5NrAdCBMvGwPiwNzxzhVT5Rlrm8Uhb5egyiZMe3HvldO2srDbwEdAR+AAgQX1+jxqhoZeMDHfFtHOD27Ts0ELX7DLN8ESBUSRFYefsX4nQ/Q1mYjajRzk5hCa6Ly2RgEpRJYqOGBI8iNLLVUfABJeSVUDWehuyuocx3UaSDISzO13vWHRXZTxZ7ShrGMWJsfNopVrxk7FtpVeRYM7KWlK2XodhDG84C4njMs0VIsAskEqX1nVK89GMGE35T0ruhne6VbuWeqO6kjZzStgRuirJ"
  }

  assert {
    condition     = azurerm_subnet.runner[0].address_prefixes == tolist(["10.42.0.96/27"]) && length(azurerm_subnet.runner[0].delegation) == 0
    error_message = "The runner must have its own nondelegated subnet."
  }

  assert {
    condition     = one(azurerm_network_security_group.runner[0].security_rule).access == "Deny" && one(azurerm_network_security_group.runner[0].security_rule).direction == "Inbound" && one(azurerm_network_security_group.runner[0].security_rule).source_address_prefix == "*" && one(azurerm_network_security_group.runner[0].security_rule).destination_port_range == "*"
    error_message = "The runner NSG must deny inbound traffic from every source and port."
  }

  assert {
    condition     = one(azurerm_network_interface.runner[0].ip_configuration).public_ip_address_id == null && azurerm_public_ip.runner_nat[0].sku == "Standard" && azurerm_nat_gateway.runner[0].sku_name == "Standard"
    error_message = "The runner NIC must be private and use a Standard NAT gateway."
  }

  assert {
    condition     = azurerm_linux_virtual_machine.runner[0].size == "Standard_D2s_v7" && azurerm_linux_virtual_machine.runner[0].disk_controller_type == "NVMe" && azurerm_linux_virtual_machine.runner[0].disable_password_authentication && azurerm_linux_virtual_machine.runner[0].provision_vm_agent && one(azurerm_linux_virtual_machine.runner[0].identity).type == "UserAssigned"
    error_message = "The runner VM must use the selected size, NVMe, VM agent and managed identity without password authentication."
  }

  assert {
    condition     = one(azurerm_linux_virtual_machine.runner[0].source_image_reference).version == "24.04.202611170" && length(azurerm_role_assignment.registration) == 0
    error_message = "The runner image must be pinned and registration roles must not be granted implicitly."
  }
}

run "rejects_runner_subnet_overlap" {
  command = plan
  variables {
    runner_enabled              = true
    runner_admin_ssh_public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUvp2ERuiFq8Z0ZthtsQ9C9Fd+nXsi4tdGOtBnHw4PgCi8k+bTBtbN1cDY0Sksbj5NrAdCBMvGwPiwNzxzhVT5Rlrm8Uhb5egyiZMe3HvldO2srDbwEdAR+AAgQX1+jxqhoZeMDHfFtHOD27Ts0ELX7DLN8ESBUSRFYefsX4nQ/Q1mYjajRzk5hCa6Ly2RgEpRJYqOGBI8iNLLVUfABJeSVUDWehuyuocx3UaSDISzO13vWHRXZTxZ7ShrGMWJsfNopVrxk7FtpVeRYM7KWlK2XodhDG84C4njMs0VIsAskEqX1nVK89GMGE35T0ruhne6VbuWeqO6kjZzStgRuirJ"
    runner_subnet_prefix        = "10.42.0.64/27"
  }
  expect_failures = [terraform_data.runner_guard]
}

run "rejects_runner_outside_vnet" {
  command = plan
  variables {
    runner_enabled              = true
    runner_admin_ssh_public_key = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDUvp2ERuiFq8Z0ZthtsQ9C9Fd+nXsi4tdGOtBnHw4PgCi8k+bTBtbN1cDY0Sksbj5NrAdCBMvGwPiwNzxzhVT5Rlrm8Uhb5egyiZMe3HvldO2srDbwEdAR+AAgQX1+jxqhoZeMDHfFtHOD27Ts0ELX7DLN8ESBUSRFYefsX4nQ/Q1mYjajRzk5hCa6Ly2RgEpRJYqOGBI8iNLLVUfABJeSVUDWehuyuocx3UaSDISzO13vWHRXZTxZ7ShrGMWJsfNopVrxk7FtpVeRYM7KWlK2XodhDG84C4njMs0VIsAskEqX1nVK89GMGE35T0ruhne6VbuWeqO6kjZzStgRuirJ"
    runner_subnet_prefix        = "10.42.1.0/27"
  }
  expect_failures = [terraform_data.runner_guard]
}

run "rejects_runner_without_public_key" {
  command = plan
  variables {
    runner_enabled = true
  }
  expect_failures = [terraform_data.runner_guard]
}

run "registration_identity_gets_table_roles" {
  command = plan
  variables {
    registration_principal_object_id = "55555555-5555-5555-5555-555555555555"
  }

  # register (control table lease) and the smoke test (registry, submissions, receipts).
  assert {
    condition = alltrue([
      for table in ["batchcontrol", "batchjobs", "batchsubmissions", "demoreceipts"] :
      azurerm_role_assignment.registration["st-table-${table}"].role_definition_name == "Storage Table Data Contributor"
    ])
    error_message = "The pipeline identity needs Storage Table Data Contributor on the tables it uses."
  }

  assert {
    condition     = length([for key, ra in azurerm_role_assignment.registration : key if strcontains(ra.role_definition_name, "Storage")]) == 4
    error_message = "The pipeline identity gets table roles only, never account-wide storage roles."
  }
}

run "rejects_local_environment_names" {
  command = plan
  variables {
    environment = "dev"
  }
  expect_failures = [var.environment]
}

run "rejects_foundry_region_outside_allowed_locations" {
  command = plan
  variables {
    foundry_resources = {
      weu = {
        location = "westeurope"
        deployments = [
          { name = "batch-default", model_name = "gpt-test", model_version = "2025-01-01", capacity = 1 },
        ]
      }
    }
  }
  expect_failures = [var.foundry_resources]
}

run "accepts_foundry_region_in_allowed_locations" {
  command = plan
  variables {
    allowed_locations = ["eastus2", "swedencentral"]
    foundry_resources = {
      swc = {
        location = "swedencentral"
        deployments = [
          { name = "batch-default", model_name = "gpt-test", model_version = "2025-01-01", capacity = 1 },
        ]
      }
    }
  }

  assert {
    condition     = jsondecode(output.resources_json)[0].region == "swedencentral"
    error_message = "The Foundry override region must reach BATCH_RESOURCES_JSON."
  }
}

run "rejects_sweep_lag_threshold_below_minimum" {
  command = plan
  variables {
    sweep_lag_threshold_seconds = 60
  }
  expect_failures = [var.sweep_lag_threshold_seconds]
}

run "rejects_unapproved_region" {
  command = plan
  variables {
    location = "brazilsouth"
  }
  expect_failures = [var.location]
}

run "rejects_placeholder_model_versions" {
  command = plan
  variables {
    foundry_resources = {
      eus2 = {
        deployments = [
          { name = "batch-default", model_name = "REPLACE-WITH-D-12-MODEL", model_version = "REPLACE-WITH-D-12-VERSION", capacity = 1 },
        ]
      }
    }
  }
  expect_failures = [var.foundry_resources]
}

run "rejects_api_keys_without_exception" {
  command = plan
  variables {
    foundry_resources = {
      eus2 = {
        local_auth_enabled = true
        deployments = [
          { name = "batch-default", model_name = "gpt-test", model_version = "2025-01-01", capacity = 1 },
        ]
      }
    }
  }
  expect_failures = [var.foundry_resources]
}

run "rejects_missing_mandatory_tags" {
  command = plan
  variables {
    tags = { owner = "poc-lead" }
  }
  expect_failures = [var.tags]
}

run "rejects_wrong_subscription" {
  command = plan
  variables {
    subscription_id = "99999999-9999-9999-9999-999999999999"
  }
  expect_failures = [terraform_data.target_guard]
}

# The app refuses these at start-up (config.py), so Terraform must refuse them at plan time.
run "rejects_fractional_body_cap" {
  command = plan
  variables {
    max_body_bytes = 32768.5
  }
  expect_failures = [var.max_body_bytes]
}

run "rejects_tolerance_outside_app_bounds" {
  command = plan
  variables {
    timestamp_tolerance_seconds = 3600
  }
  expect_failures = [var.timestamp_tolerance_seconds]
}

run "rejects_http_mode_without_url" {
  command = plan
  variables {
    downstream_mode = "http"
    downstream_url  = ""
  }
  expect_failures = [var.downstream_url]
}

run "storage_security_controls" {
  command = plan
  module {
    source = "../../modules/storage"
  }
  variables {
    name                       = "stbatchtest"
    resource_group_name        = "rg-foundry-batch-poc"
    resource_group_id          = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
    location                   = "eastus2"
    containers                 = ["app-package"]
    queues                     = ["batch-events"]
    tables                     = ["batchjobs"]
    private_endpoint_subnet_id = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/private"
    private_dns_zone_ids = {
      blob  = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.blob.core.windows.net"
      queue = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.queue.core.windows.net"
      table = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.table.core.windows.net"
    }
  }
  assert {
    condition = (
      azapi_resource.account.body.properties.allowSharedKeyAccess == false &&
      azapi_resource.account.body.properties.publicNetworkAccess == "Disabled" &&
      azapi_resource.account.body.properties.supportsHttpsTrafficOnly == true &&
      azapi_resource.account.body.properties.minimumTlsVersion == "TLS1_2" &&
      azapi_resource.account.body.properties.encryption.requireInfrastructureEncryption == true &&
      azapi_resource.account.body.properties.networkAcls.bypass == "None"
    )
    error_message = "Storage must remain private (no trusted-service bypass), identity-only, HTTPS-only and encrypted."
  }
  assert {
    condition     = length(azurerm_private_endpoint.this) == 3 && azapi_resource.container["app-package"].body.properties.publicAccess == "None"
    error_message = "All three storage services need private endpoints and containers must not be public."
  }
}

run "function_security_controls" {
  command = plan
  module {
    source = "../../modules/function_app"
  }
  variables {
    name                                   = "func-batch-test"
    resource_group_name                    = "rg-foundry-batch-poc"
    resource_group_id                      = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
    location                               = "eastus2"
    storage_account_name                   = "stbatchtest"
    storage_blob_endpoint                  = "https://stbatchtest.blob.core.windows.net/"
    deployment_container_name              = "app-package"
    subnet_id                              = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/functions"
    application_insights_connection_string = "test-observability-endpoint"
    app_settings                           = {}
    private_endpoint_subnet_id             = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/private"
    private_dns_zone_id                    = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.azurewebsites.net"
  }
  assert {
    condition = (
      azapi_resource.site.body.properties.publicNetworkAccess == "Enabled" &&
      azapi_resource.site.body.properties.siteConfig.scmIpSecurityRestrictionsDefaultAction == "Deny" &&
      azapi_resource.site.body.properties.siteConfig.scmIpSecurityRestrictionsUseMain == false &&
      azapi_resource.site.body.properties.outboundVnetRouting.allTraffic == true &&
      azurerm_private_endpoint.site.private_service_connection[0].subresource_names == tolist(["sites"])
    )
    error_message = "Default: webhook ingress public (NX-1) but deployment endpoint private, all egress through the VNet, inbound private endpoint present."
  }
  assert {
    condition = (
      azapi_resource.site.body.properties.httpsOnly == true &&
      azapi_resource.site.body.properties.siteConfig.minTlsVersion == "1.2" &&
      azapi_resource.site.body.properties.functionAppConfig.deployment.storage.authentication.type == "UserAssignedIdentity" &&
      alltrue([for policy in azapi_update_resource.basic_auth : policy.body.properties.allow == false])
    )
    error_message = "Functions must retain TLS, managed-identity deployment storage and disabled publishing passwords."
  }
}

run "function_private_only" {
  command = plan
  module {
    source = "../../modules/function_app"
  }
  variables {
    name                                   = "func-batch-test"
    resource_group_name                    = "rg-foundry-batch-poc"
    resource_group_id                      = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
    location                               = "eastus2"
    storage_account_name                   = "stbatchtest"
    storage_blob_endpoint                  = "https://stbatchtest.blob.core.windows.net/"
    deployment_container_name              = "app-package"
    subnet_id                              = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/functions"
    application_insights_connection_string = "test-observability-endpoint"
    app_settings                           = {}
    private_endpoint_subnet_id             = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/private"
    private_dns_zone_id                    = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.azurewebsites.net"
    public_network_access_enabled          = false
    webhook_source_service_tags            = ["AzureCloud.eastus2"]
    outbound_route_all                     = false
  }
  assert {
    condition = (
      azapi_resource.site.body.properties.publicNetworkAccess == "Disabled" &&
      azapi_resource.site.body.properties.siteConfig.ipSecurityRestrictionsDefaultAction == "Deny" &&
      azapi_resource.site.body.properties.siteConfig.ipSecurityRestrictions[0].ipAddress == "AzureCloud.eastus2" &&
      azapi_resource.site.body.properties.siteConfig.ipSecurityRestrictions[0].tag == "ServiceTag" &&
      !can(azapi_resource.site.body.properties.outboundVnetRouting)
    )
    error_message = "Private-only app: public access off, service-tag allow-list rendered, route-all omitted when disabled."
  }
}

run "foundry_private_endpoint_in_network_region" {
  command = plan
  module {
    source = "../../modules/foundry"
  }
  variables {
    name                       = "oai-aibatch-test-swc"
    resource_group_name        = "rg-foundry-batch-poc"
    location                   = "swedencentral"
    private_endpoint_location  = "eastus2"
    private_endpoint_subnet_id = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/private"
    deployments = [
      { name = "batch-default", model_name = "gpt-test", model_version = "2025-01-01", capacity = 1 },
    ]
    private_dns_zone_ids = {
      cognitiveservices = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.cognitiveservices.azure.com"
      openai            = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.openai.azure.com"
      aiservices        = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.services.ai.azure.com"
    }
    tags = {}
  }
  assert {
    condition     = azurerm_private_endpoint.account.location == "eastus2" && azurerm_cognitive_account.this.location == "swedencentral"
    error_message = "The private endpoint must be in the VNet's region even when the account is elsewhere."
  }
  assert {
    condition     = azurerm_cognitive_account.this.public_network_access_enabled == false && azurerm_cognitive_account.this.network_acls[0].bypass == "None"
    error_message = "The Foundry account must be private with no trusted-service bypass."
  }
}

run "monitoring_security_controls" {
  command = plan
  module {
    source = "../../modules/monitoring"
  }
  variables {
    name_suffix                = "aibatch-test-eus2"
    resource_group_name        = "rg-foundry-batch-poc"
    resource_group_id          = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
    location                   = "eastus2"
    diagnostic_targets         = {}
    tags                       = {}
    private_endpoint_subnet_id = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/virtualNetworks/vnet-test/subnets/private"
    private_dns_zone_ids = {
      monitor  = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.monitor.azure.com"
      oms      = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.oms.opinsights.azure.com"
      ods      = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.ods.opinsights.azure.com"
      agentsvc = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.agentsvc.azure-automation.net"
      blob     = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc/providers/Microsoft.Network/privateDnsZones/privatelink.blob.core.windows.net"
    }
  }
  assert {
    condition = (
      azapi_resource.workspace.body.properties.publicNetworkAccessForIngestion == "Disabled" &&
      azapi_resource.workspace.body.properties.publicNetworkAccessForQuery == "Enabled" &&
      azurerm_application_insights.this.internet_ingestion_enabled == false &&
      azurerm_application_insights.this.internet_query_enabled == true
    )
    error_message = "Default: telemetry ingestion private only; public queries kept as exception NX-2."
  }
  assert {
    condition = (
      azapi_resource.private_link_scope[0].body.properties.accessModeSettings.ingestionAccessMode == "PrivateOnly" &&
      azapi_resource.private_link_scope[0].body.properties.accessModeSettings.queryAccessMode == "PrivateOnly" &&
      length(azapi_resource.scoped_resource) == 2 &&
      azurerm_private_endpoint.monitor[0].private_service_connection[0].subresource_names == tolist(["azuremonitor"]) &&
      length(azurerm_private_endpoint.monitor[0].private_dns_zone_group[0].private_dns_zone_ids) == 5
    )
    error_message = "The AMPLS must scope the workspace and Application Insights, run Private Only and register all five zones."
  }
  assert {
    condition = (
      azapi_resource.workspace.body.properties.features.disableLocalAuth == true &&
      azurerm_application_insights.this.local_authentication_enabled == false
    )
    error_message = "Log Analytics and Application Insights must accept Entra ID only (no shared keys)."
  }
  assert {
    condition = (
      strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.this["sweep-lagging"].criteria[0].query, "lag_s > 1200") &&
      strcontains(azurerm_monitor_scheduled_query_rules_alert_v2.this["sweep-lagging"].criteria[0].query, "traversal_complete")
    )
    error_message = "The sweep-lagging alert must use the lag threshold and the incomplete-traversal rule."
  }
}

run "monitoring_rejects_private_queries_without_private_link" {
  command = plan
  module {
    source = "../../modules/monitoring"
  }
  variables {
    name_suffix          = "aibatch-test-eus2"
    resource_group_name  = "rg-foundry-batch-poc"
    resource_group_id    = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg-foundry-batch-poc"
    location             = "eastus2"
    diagnostic_targets   = {}
    tags                 = {}
    private_link_enabled = false
    public_query_enabled = false
  }
  expect_failures = [azapi_resource.workspace]
}

# ---- Private networking (REQ-I13, ADR 0010) ----------------------------------------------------

run "private_networking_defaults" {
  command = plan

  assert {
    condition     = output.webhook_ingress_enabled && contains(keys(output.webhook_receiver_urls), "eus2")
    error_message = "Default keeps webhook ingress (NX-1) and lists a receiver URL per resource."
  }

  assert {
    condition     = sort([for e in output.network_exceptions : e.id]) == tolist(["NX-1", "NX-2"])
    error_message = "Only the declared exceptions NX-1 and NX-2 may be public by default."
  }

  assert {
    condition     = module.network.function_egress_rule_names == tolist(["allow-entra-outbound", "deny-internet-outbound"])
    error_message = "With the AMPLS, the Function App subnet allows only Entra ID to the internet."
  }

  assert {
    condition = alltrue([
      for zone in [
        "privatelink.azurewebsites.net", "privatelink.monitor.azure.com", "privatelink.oms.opinsights.azure.com",
        "privatelink.ods.opinsights.azure.com", "privatelink.agentsvc.azure-automation.net",
        "privatelink.blob.core.windows.net", "privatelink.queue.core.windows.net", "privatelink.table.core.windows.net",
        "privatelink.vaultcore.azure.net", "privatelink.openai.azure.com",
      ] : contains(module.network.created_private_dns_zone_names, zone)
    ])
    error_message = "Every private endpoint needs its private DNS zone."
  }
}

run "polling_only_private_app" {
  command = plan
  variables {
    function_public_ingress      = false
    monitor_public_query_enabled = false
  }

  assert {
    condition     = !output.webhook_ingress_enabled && length(output.webhook_receiver_urls) == 0
    error_message = "Without public ingress the pipeline must register no webhook (polling-only)."
  }

  assert {
    condition     = length(output.network_exceptions) == 0
    error_message = "Fully private configuration keeps no public path."
  }
}

run "rejects_private_queries_without_monitor_private_link" {
  command = plan
  variables {
    monitor_private_link_enabled = false
    monitor_public_query_enabled = false
  }
  expect_failures = [var.monitor_public_query_enabled]
}

run "without_monitor_private_link_egress_allows_azure_monitor" {
  command = plan
  variables {
    monitor_private_link_enabled = false
  }

  assert {
    condition     = contains(module.network.function_egress_rule_names, "allow-monitor-outbound") && !contains(module.network.created_private_dns_zone_names, "privatelink.monitor.azure.com")
    error_message = "Without the AMPLS, telemetry needs the AzureMonitor egress rule and no monitor zones are created."
  }

  assert {
    condition     = contains([for e in output.network_exceptions : e.id], "NX-2b")
    error_message = "Public ingestion without the AMPLS must be listed as an exception."
  }
}

run "rejects_firewall_route_with_nsg_lockdown" {
  command = plan
  variables {
    egress_next_hop_ip = "10.0.0.4"
  }
  expect_failures = [var.egress_next_hop_ip]
}

run "firewall_route_replaces_nsg_lockdown" {
  command = plan
  variables {
    function_egress_lockdown = false
    egress_next_hop_ip       = "10.0.0.4"
  }

  assert {
    condition     = length(module.network.function_egress_rule_names) == 0
    error_message = "With the landing zone firewall route, the NSG must not drop internet-bound packets."
  }
}

run "rejects_invalid_firewall_address" {
  command = plan
  variables {
    function_egress_lockdown = false
    egress_next_hop_ip       = "firewall.local"
  }
  expect_failures = [var.egress_next_hop_ip]
}

run "landing_zone_dns_zones_are_referenced_not_created" {
  command = plan
  variables {
    existing_private_dns_zone_ids = {
      blob = "/subscriptions/99999999-9999-9999-9999-999999999999/resourceGroups/rg-hub-dns/providers/Microsoft.Network/privateDnsZones/privatelink.blob.core.windows.net"
    }
  }

  assert {
    condition     = !contains(module.network.created_private_dns_zone_names, "privatelink.blob.core.windows.net") && output.private_dns_zone_ids["blob"] == "/subscriptions/99999999-9999-9999-9999-999999999999/resourceGroups/rg-hub-dns/providers/Microsoft.Network/privateDnsZones/privatelink.blob.core.windows.net"
    error_message = "A landing zone DNS zone replaces the module's own zone."
  }
}
