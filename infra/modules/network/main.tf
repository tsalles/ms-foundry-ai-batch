locals {
  # Private DNS zones for every data-plane service the workload reaches privately.
  # AI Services accounts register in all three AI zones through one "account" endpoint; the
  # Function App's "sites" endpoint registers the app and its deployment (SCM) host names.
  service_zones = {
    blob              = "privatelink.blob.core.windows.net"
    queue             = "privatelink.queue.core.windows.net"
    table             = "privatelink.table.core.windows.net"
    vault             = "privatelink.vaultcore.azure.net"
    cognitiveservices = "privatelink.cognitiveservices.azure.com"
    openai            = "privatelink.openai.azure.com"
    aiservices        = "privatelink.services.ai.azure.com"
    sites             = "privatelink.azurewebsites.net"
  }

  # Azure Monitor Private Link Scope zones (the AMPLS endpoint also uses the blob zone).
  # Azure Monitor endpoints are global: whatever VNet resolves these zones sends all its Azure
  # Monitor traffic to this AMPLS, so they are linked to the workload VNet only.
  monitor_zone_catalogue = {
    monitor  = "privatelink.monitor.azure.com"
    oms      = "privatelink.oms.opinsights.azure.com"
    ods      = "privatelink.ods.opinsights.azure.com"
    agentsvc = "privatelink.agentsvc.azure-automation.net"
  }
  monitor_zones = { for key, name in local.monitor_zone_catalogue : key => name if var.monitor_private_link_enabled }

  all_zones = merge(local.service_zones, local.monitor_zones)

  # Zones supplied by the landing zone (central hub DNS) are referenced, not created or linked.
  created_zones = { for key, name in local.all_zones : key => name if !contains(keys(var.existing_private_dns_zone_ids), key) }

  dns_links = merge(
    { workload = azurerm_virtual_network.this.id },
    var.additional_dns_link_vnet_ids,
  )

  zone_links = {
    for pair in setproduct(keys(local.created_zones), keys(local.dns_links)) :
    "${pair[0]}-${pair[1]}" => { zone = pair[0], vnet = pair[1] }
    if pair[1] == "workload" || !contains(keys(local.monitor_zones), pair[0])
  }

  # Function subnet egress: deny the internet except Entra ID (managed identity tokens when all
  # traffic is routed through the VNet) and, without the AMPLS, Azure Monitor ingestion.
  egress_rule_catalogue = {
    allow-entra-outbound   = { priority = 200, access = "Allow", protocol = "Tcp", port = "443", destination = "AzureActiveDirectory" }
    allow-monitor-outbound = { priority = 210, access = "Allow", protocol = "Tcp", port = "443", destination = "AzureMonitor" }
    deny-internet-outbound = { priority = 4000, access = "Deny", protocol = "*", port = "*", destination = "Internet" }
  }
  egress_rules = {
    for name, rule in local.egress_rule_catalogue : name => rule
    if var.function_egress_lockdown && (name != "allow-monitor-outbound" || !var.monitor_private_link_enabled)
  }
}

resource "azurerm_virtual_network" "this" {
  name                = "vnet-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  address_space       = var.address_space
  tags                = var.tags
}

resource "azurerm_subnet" "function" {
  name                 = "snet-func-integration"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.this.name
  address_prefixes     = [var.function_subnet_prefix]

  # Flex Consumption VNet integration uses the Microsoft.App/environments delegation.
  delegation {
    name = "flex-consumption"

    service_delegation {
      name    = "Microsoft.App/environments"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "private_endpoints" {
  name                              = "snet-private-endpoints"
  resource_group_name               = var.resource_group_name
  virtual_network_name              = azurerm_virtual_network.this.name
  address_prefixes                  = [var.private_endpoint_subnet_prefix]
  private_endpoint_network_policies = "Enabled"
}

resource "azurerm_network_security_group" "function" {
  name                = "nsg-func-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
}

# Separate rule resources (not inline blocks) so rules can be added or removed without
# replacing the group. VNet destinations (private endpoints, peered networks, networks learned
# through a gateway) stay allowed by the default AllowVnetOutBound rule.
resource "azurerm_network_security_rule" "function_egress" {
  for_each = local.egress_rules

  name                        = each.key
  resource_group_name         = var.resource_group_name
  network_security_group_name = azurerm_network_security_group.function.name
  priority                    = each.value.priority
  direction                   = "Outbound"
  access                      = each.value.access
  protocol                    = each.value.protocol
  source_port_range           = "*"
  destination_port_range      = each.value.port
  source_address_prefix       = "VirtualNetwork"
  destination_address_prefix  = each.value.destination
}

# Optional forced tunnelling to a configured hub firewall. Mutually exclusive with
# the NSG lockdown, which would drop internet-bound packets before they reach the firewall.
resource "azurerm_route_table" "function" {
  count = var.egress_next_hop_ip == null ? 0 : 1

  name                          = "rt-func-${var.name_suffix}"
  resource_group_name           = var.resource_group_name
  location                      = var.location
  bgp_route_propagation_enabled = false
  tags                          = var.tags

  route {
    name                   = "default-to-firewall"
    address_prefix         = "0.0.0.0/0"
    next_hop_type          = "VirtualAppliance"
    next_hop_in_ip_address = var.egress_next_hop_ip
  }
}

resource "azurerm_subnet_route_table_association" "function" {
  count = var.egress_next_hop_ip == null ? 0 : 1

  subnet_id      = azurerm_subnet.function.id
  route_table_id = azurerm_route_table.function[0].id
}

resource "azurerm_network_security_group" "private_endpoints" {
  name                = "nsg-pe-${var.name_suffix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags

  # Only the workload VNet (function integration subnet, linked runners) reaches the endpoints.
  security_rule {
    name                       = "allow-vnet-https"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "deny-all-inbound"
    priority                   = 4096
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "function" {
  subnet_id                 = azurerm_subnet.function.id
  network_security_group_id = azurerm_network_security_group.function.id
}

resource "azurerm_subnet_network_security_group_association" "private_endpoints" {
  subnet_id                 = azurerm_subnet.private_endpoints.id
  network_security_group_id = azurerm_network_security_group.private_endpoints.id
}

resource "azurerm_private_dns_zone" "this" {
  for_each = local.created_zones

  name                = each.value
  resource_group_name = var.resource_group_name
  tags                = var.tags
}

# azurerm 5.x links reference the zone by id.
# VERIFY: argument set of azurerm_private_dns_zone_virtual_network_link in the pinned 5.x release.
resource "azurerm_private_dns_zone_virtual_network_link" "this" {
  for_each = local.zone_links

  name                 = "link-${each.value.vnet}"
  private_dns_zone_id  = azurerm_private_dns_zone.this[each.value.zone].id
  virtual_network_id   = local.dns_links[each.value.vnet]
  registration_enabled = false
  tags                 = var.tags
}
