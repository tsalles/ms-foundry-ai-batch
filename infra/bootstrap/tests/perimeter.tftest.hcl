mock_provider "azurerm" {}
mock_provider "azapi" {}

variables {
  tenant_id       = "11111111-1111-1111-1111-111111111111"
  subscription_id = "22222222-2222-2222-2222-222222222222"
  tags = {
    owner       = "poc-lead"
    cost-center = "cc-test"
  }
  allowed_ip_ranges = ["203.0.113.7"]
}

run "perimeter_is_opt_in" {
  command = plan

  assert {
    condition     = length(azapi_resource.perimeter) == 0 && azapi_resource.state.body.properties.publicNetworkAccess == "Enabled"
    error_message = "Without the perimeter the state account keeps the IP-restricted public endpoint."
  }
}

run "perimeter_replaces_ip_rules" {
  command = plan

  variables {
    state_network_security_perimeter_enabled = true
  }

  assert {
    condition = (
      azapi_resource.state.body.properties.publicNetworkAccess == "SecuredByPerimeter" &&
      length(azapi_resource.state.body.properties.networkAcls.ipRules) == 0 &&
      azapi_resource.perimeter_state[0].body.properties.accessMode == "Enforced"
    )
    error_message = "The perimeter must be enforced and the storage IP rules dropped."
  }

  assert {
    condition     = join(",", azapi_resource.perimeter_desktop_inbound[0].body.properties.addressPrefixes) == "203.0.113.7/32"
    error_message = "A bare IPv4 address must become a /32 inbound prefix."
  }
}

run "perimeter_needs_an_allowed_ip" {
  command = plan

  variables {
    state_network_security_perimeter_enabled = true
    allowed_ip_ranges                        = []
  }

  expect_failures = [var.state_network_security_perimeter_enabled]
}
