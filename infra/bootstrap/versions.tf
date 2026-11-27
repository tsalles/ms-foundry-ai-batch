terraform {
  required_version = ">= 1.11.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.14"
    }
    azapi = {
      source  = "Azure/azapi"
      version = "~> 2.13"
    }
  }

  # Local state first (protected, never committed). After the first apply, uncomment, fill
  # bootstrap.backend.hcl and run `terraform init -migrate-state` (see infra/README.md).
  # backend "azurerm" {}
}
