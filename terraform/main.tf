terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "ecosync" {
  name     = "rg-ecosync"
  location = "Korea Central"
}

resource "azurerm_eventhub_namespace" "ecosync" {
  name                = "ecosync-eventhub"
  location            = azurerm_resource_group.ecosync.location
  resource_group_name = azurerm_resource_group.ecosync.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "generation" {
  name                = "generation"
  namespace_name      = azurerm_eventhub_namespace.ecosync.name
  resource_group_name = azurerm_resource_group.ecosync.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub" "demand" {
  name                = "demand"
  namespace_name      = azurerm_eventhub_namespace.ecosync.name
  resource_group_name = azurerm_resource_group.ecosync.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_eventhub" "dead_letter" {
  name                = "dead-letter"
  namespace_name      = azurerm_eventhub_namespace.ecosync.name
  resource_group_name = azurerm_resource_group.ecosync.name
  partition_count     = 2
  message_retention   = 1
}

resource "azurerm_storage_account" "ecosync" {
  name                     = "ecosyncstorage"
  resource_group_name      = azurerm_resource_group.ecosync.name
  location                 = azurerm_resource_group.ecosync.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "raw" {
  name                  = "ecosync-raw"
  storage_account_name  = azurerm_storage_account.ecosync.name
  container_access_type = "private"
}

resource "azurerm_postgresql_flexible_server" "ecosync" {
  name                   = "ecosync-postgres"
  resource_group_name    = azurerm_resource_group.ecosync.name
  location               = azurerm_resource_group.ecosync.location
  version                = "16"
  administrator_login    = "ecosync_admin"
  administrator_password = var.db_password
  sku_name               = "B_Standard_B2s"
  storage_mb             = 32768
}

resource "azurerm_postgresql_flexible_server_database" "ecosync" {
  name      = "ecosync_db"
  server_id = azurerm_postgresql_flexible_server.ecosync.id
}