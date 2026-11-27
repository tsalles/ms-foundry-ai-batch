locals {
  runner_cidrs = concat(var.vnet_address_space, [var.function_subnet_prefix, var.private_endpoint_subnet_prefix, var.runner_subnet_prefix])
  runner_ranges = [for cidr in local.runner_cidrs : try({
    first = sum([for index, octet in split(".", cidrhost(cidr, 0)) : tonumber(octet) * pow(256, 3 - index)])
    last  = sum([for index, octet in split(".", cidrhost(cidr, -1)) : tonumber(octet) * pow(256, 3 - index)])
    ipv4  = length(split(".", cidrhost(cidr, 0))) == 4
  }, { first = 0, last = 0, ipv4 = false })]
  runner_range   = local.runner_ranges[length(local.runner_ranges) - 1]
  function_range = local.runner_ranges[length(local.runner_ranges) - 3]
  endpoint_range = local.runner_ranges[length(local.runner_ranges) - 2]
}

resource "terraform_data" "runner_guard" {
  count = var.runner_enabled ? 1 : 0

  lifecycle {
    precondition {
      condition = alltrue([for range in local.runner_ranges : range.ipv4]) && anytrue([
        for range in slice(local.runner_ranges, 0, length(var.vnet_address_space)) :
        local.runner_range.first >= range.first && local.runner_range.last <= range.last
        ]) && alltrue([
        for range in [local.function_range, local.endpoint_range] :
        local.runner_range.last < range.first || local.runner_range.first > range.last
      ]) && try(tonumber(split("/", var.runner_subnet_prefix)[1]) <= 29, false)
      error_message = "The runner subnet must be a valid IPv4 /29 or larger inside a VNet address range and must not overlap the function or private endpoint subnet."
    }

    precondition {
      condition     = var.runner_admin_ssh_public_key != null && can(regex("^ssh-rsa [A-Za-z0-9+/]+={0,3}( .*)?$", coalesce(var.runner_admin_ssh_public_key, "")))
      error_message = "runner_admin_ssh_public_key must contain an RSA SSH public key when the runner is enabled."
    }
  }
}

resource "azurerm_subnet" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                 = "snet-runner"
  resource_group_name  = data.azurerm_resource_group.this.name
  virtual_network_name = "vnet-${local.name_suffix}"
  address_prefixes     = [var.runner_subnet_prefix]

  depends_on = [module.network, terraform_data.target_guard, terraform_data.runner_guard]
}

resource "azurerm_network_security_group" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                = "nsg-runner-${local.name_suffix}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.location
  tags                = local.tags

  security_rule {
    name                       = "deny-all-inbound"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "runner" {
  count = var.runner_enabled ? 1 : 0

  subnet_id                 = azurerm_subnet.runner[0].id
  network_security_group_id = azurerm_network_security_group.runner[0].id
}

resource "azurerm_public_ip" "runner_nat" {
  count = var.runner_enabled ? 1 : 0

  name                = "pip-runner-nat-${local.name_suffix}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.location
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags

  # The tenant adds FirstPartyUsage=/Unprivileged after creation; without this the next plan
  # wants to replace the address (and the NAT association).
  lifecycle {
    ignore_changes = [ip_tags]
  }
}

resource "azurerm_nat_gateway" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                = "nat-runner-${local.name_suffix}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.location
  sku_name            = "Standard"
  tags                = local.tags
}

resource "azurerm_nat_gateway_public_ip_association" "runner" {
  count = var.runner_enabled ? 1 : 0

  nat_gateway_id       = azurerm_nat_gateway.runner[0].id
  public_ip_address_id = azurerm_public_ip.runner_nat[0].id
}

resource "azurerm_subnet_nat_gateway_association" "runner" {
  count = var.runner_enabled ? 1 : 0

  subnet_id      = azurerm_subnet.runner[0].id
  nat_gateway_id = azurerm_nat_gateway.runner[0].id
}

resource "azurerm_network_interface" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                = "nic-runner-${local.name_suffix}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.location
  tags                = local.tags

  ip_configuration {
    name                          = "private"
    subnet_id                     = azurerm_subnet.runner[0].id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_user_assigned_identity" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                = "id-runner-${local.name_suffix}"
  resource_group_name = data.azurerm_resource_group.this.name
  location            = var.location
  tags                = local.tags
}

resource "azurerm_linux_virtual_machine" "runner" {
  count = var.runner_enabled ? 1 : 0

  name                            = "vm-runner-${local.name_suffix}"
  resource_group_name             = data.azurerm_resource_group.this.name
  location                        = var.location
  size                            = "Standard_D2s_v7"
  disk_controller_type            = "NVMe"
  admin_username                  = "batchoperator"
  disable_password_authentication = true
  provision_vm_agent              = true
  network_interface_ids           = [azurerm_network_interface.runner[0].id]
  tags                            = local.tags

  admin_ssh_key {
    username   = "batchoperator"
    public_key = var.runner_admin_ssh_public_key
  }

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.runner[0].id]
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "24.04.202611170"
  }

  depends_on = [azurerm_subnet_network_security_group_association.runner, azurerm_subnet_nat_gateway_association.runner, azurerm_nat_gateway_public_ip_association.runner]
}