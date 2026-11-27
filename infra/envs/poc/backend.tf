# Partial configuration: values come from backend.hcl (see backend.hcl.example), never secrets.
# Entra authentication to the state account; shared keys are disabled there too.
terraform {
  backend "azurerm" {}
}
