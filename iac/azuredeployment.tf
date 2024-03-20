provider "azurerm" {
  features {}
}

# Create a resource group
resource "azurerm_resource_group" "rg" {
  name     = "parloaint1"
  location = "westeurope"
}

# Create a storage account
resource "azurerm_storage_account" "sa" {
  name                     = "parloaintsa1"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create a blob container
resource "azurerm_storage_container" "sc" {
  name                  = "lakehouse"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
}

# Create an Azure Container Registry
resource "azurerm_container_registry" "acr" {
  name                = "parloaintacr1"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Execute this command to push the local image to the remote registry
resource "null_resource" "docker_push" {
  depends_on = [azurerm_container_registry.acr]

  provisioner "local-exec" {
    command = <<EOT
      az acr login --name ${azurerm_container_registry.acr.name}
      docker tag parloa-data-engineering-challenge:0.0.1 ${azurerm_container_registry.acr.login_server}/parloa-data-engineering-challenge:0.0.1
      docker push ${azurerm_container_registry.acr.login_server}/parloa-data-engineering-challenge:0.0.1
    EOT
  }
}


# Create an Azure Container Instance
resource "azurerm_container_group" "aci" {
  depends_on = [null_resource.docker_push]

  name                = "parloaaci1"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  ip_address_type     = "Public"
  dns_name_label      = "parloaaci1"
  os_type             = "Linux"

  container {
    name   = "parloa-data-engineering-challenge"
    image  = "${azurerm_container_registry.acr.login_server}/parloa-data-engineering-challenge:0.0.1"
    cpu    = "1"
    memory = "1"

    ports {
      port     = 80
      protocol = "TCP"
    }

    environment_variables = {
      "REGISTRY_LOGIN_SERVER" = azurerm_container_registry.acr.login_server
      "REGISTRY_USERNAME"     = azurerm_container_registry.acr.admin_username
      "REGISTRY_PASSWORD"     = azurerm_container_registry.acr.admin_password
    }
  }

  image_registry_credential {
    server   = azurerm_container_registry.acr.login_server
    username = azurerm_container_registry.acr.admin_username
    password = azurerm_container_registry.acr.admin_password
  }
}
