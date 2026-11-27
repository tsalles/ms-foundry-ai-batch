# Least privilege, scoped to the individual resource or table (never the RG or subscription).
locals {
  app_principal = module.function_app.identity_principal_id

  app_roles = merge(
    {
      st-blob-owner   = { scope = module.storage.id, role = "Storage Blob Data Owner" }
      st-queue        = { scope = module.storage.id, role = "Storage Queue Data Contributor" }
      st-table        = { scope = module.storage.id, role = "Storage Table Data Contributor" }
      kv-secrets-read = { scope = module.key_vault.id, role = "Key Vault Secrets User" }
      appi-publish    = { scope = module.monitoring.application_insights_id, role = "Monitoring Metrics Publisher" }
    },
    { for key in keys(var.foundry_resources) : "oai-user-${key}" => { scope = module.foundry[key].id, role = "Cognitive Services OpenAI User" } },
  )

  # Tables the pipeline identity uses: `register` (registrations and its lease in the control
  # table) and the smoke test (submissions, job registry, demo receipts). Scoped per table.
  registration_tables = ["batchcontrol", "batchjobs", "batchsubmissions", "demoreceipts"]

  registration_roles = var.registration_principal_object_id == null ? {} : merge(
    {
      kv-secrets-officer = { scope = module.key_vault.id, role = "Key Vault Secrets Officer" }
      app-deploy         = { scope = module.function_app.id, role = "Website Contributor" }
    },
    { for key in keys(var.foundry_resources) : "oai-registration-${key}" => { scope = module.foundry[key].id, role = var.registration_foundry_role_name } },
    { for table in local.registration_tables : "st-table-${table}" => { scope = module.storage.table_ids[table], role = "Storage Table Data Contributor" } },
  )

  operator_roles = merge([
    for id in var.operator_principal_object_ids : {
      "${id}-queue" = { principal = id, scope = module.storage.id, role = "Storage Queue Data Contributor" }
      "${id}-table" = { principal = id, scope = module.storage.id, role = "Storage Table Data Contributor" }
    }
  ]...)
}

resource "azurerm_role_assignment" "app" {
  for_each = local.app_roles

  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = local.app_principal
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "registration" {
  for_each = local.registration_roles

  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = var.registration_principal_object_id
  principal_type       = "ServicePrincipal"
}

resource "azurerm_role_assignment" "operators" {
  for_each = local.operator_roles

  scope                = each.value.scope
  role_definition_name = each.value.role
  principal_id         = each.value.principal
}
