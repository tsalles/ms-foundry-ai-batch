mock_provider "azurerm" {}
mock_provider "azapi" {}

variables {
  tenant_id       = "11111111-1111-1111-1111-111111111111"
  subscription_id = "22222222-2222-2222-2222-222222222222"
  tags = {
    owner       = "poc-lead"
    cost-center = "cc-test"
  }
}

run "artifacts_disabled_by_default" {
  command = plan

  assert {
    condition     = output.artifact_container_name == null && length(azapi_resource.artifacts) == 0
    error_message = "The artifact container must be opt-in."
  }
}

run "artifacts_have_scoped_roles" {
  command = plan

  variables {
    artifact_container_enabled   = true
    artifact_uploader_object_ids = ["33333333-3333-3333-3333-333333333333"]
    artifact_reader_object_ids   = ["44444444-4444-4444-4444-444444444444"]
  }

  override_resource {
    target          = azapi_resource.artifacts[0]
    override_during = plan
    values = {
      id = "/subscriptions/22222222-2222-2222-2222-222222222222/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/state/blobServices/default/containers/artifacts"
    }
  }

  assert {
    condition     = output.artifact_container_name == "artifacts" && azapi_resource.artifacts[0].body.properties.publicAccess == "None"
    error_message = "The artifact container must be private."
  }

  assert {
    condition = alltrue([
      for assignment in concat(values(azurerm_role_assignment.artifact_uploader), values(azurerm_role_assignment.artifact_reader)) :
      assignment.scope == azapi_resource.artifacts[0].id
    ])
    error_message = "Both artifact roles must be scoped to the artifact container."
  }

  assert {
    condition = (
      azurerm_role_assignment.artifact_uploader["33333333-3333-3333-3333-333333333333"].role_definition_name == "Storage Blob Data Contributor" &&
      azurerm_role_assignment.artifact_reader["44444444-4444-4444-4444-444444444444"].role_definition_name == "Storage Blob Data Reader"
    )
    error_message = "The uploader needs contributor access and the runner needs reader access."
  }
}

run "reader_cannot_be_a_state_contributor" {
  command = plan
  variables {
    artifact_reader_object_ids   = ["44444444-4444-4444-4444-444444444444"]
    state_contributor_object_ids = ["44444444-4444-4444-4444-444444444444"]
  }
  expect_failures = [var.artifact_reader_object_ids]
}

run "reader_cannot_be_a_bootstrap_state_contributor" {
  command = plan
  variables {
    artifact_reader_object_ids             = ["44444444-4444-4444-4444-444444444444"]
    bootstrap_state_contributor_object_ids = ["44444444-4444-4444-4444-444444444444"]
  }
  expect_failures = [var.artifact_reader_object_ids]
}

run "reader_cannot_be_an_uploader" {
  command = plan
  variables {
    artifact_reader_object_ids   = ["44444444-4444-4444-4444-444444444444"]
    artifact_uploader_object_ids = ["44444444-4444-4444-4444-444444444444"]
  }
  expect_failures = [var.artifact_reader_object_ids]
}

run "reader_isolation_is_case_insensitive" {
  command = plan
  variables {
    artifact_reader_object_ids             = ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"]
    bootstrap_state_contributor_object_ids = ["AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA"]
  }
  expect_failures = [var.artifact_reader_object_ids]
}