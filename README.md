# README

## Introduction

This document contains the solutions to the challenges presented in the INSTRUCTIONS.md file.

## Solution

The solution we propose is to create a data pipeline that simulates the ingestion, processing, and storage of data from a data source
(provided in the data/customers.json file) into a medallion architecture, that serves as structured data lake.

The pipeline is compose of the following steps:

1. ### Data Ingestion
    The data is loaded from the source data file using scripts/main.py. For simplicity, all methods are contained within the main.py, and
    as recommended, I don't use any architectural design patterns or layers of abstraction. I rather focus on writing clean and simple code, and demostrating the ETL process with a TDD driven approach.

    The data is first loaded from the source data file, data/customers.json, and then it is transformed into a pandas DataFrame. The data is then stored in the bronze layer. In this way we have in tabular format the raw data, and we can apply transformations to it.

    Daily imports are supported. The bronze layer is organized by date, and the data is stored in CSV files.

    The data present here, could be updated using CDC (Change Data Capture) or other methods, but for simplicity, we are only considering the initial load.
2. ### Data Processing
    The data is now loaded in the silver layer, which is the layer where the data is cleaned and structured. Here the data is also stored according the ingestion date, and 5 tables are created:
    - customers : Has the customers as primery keys
    - products : Has the products as primery keys
    - transactions : Has the transactions as primery keys
    - denormalized : Data is denormalized to facilitate analytics and machine learning purpose
    - sanitation : Checks for errors in the data, make sures that dates are correct, and amounts are calculated correctly

    The data in this layer can be used for machine learning and analytics purposes. Every daily import will create a new folder with the date of the import, and the data will be stored in CSV files.
    
3. ### Data Storage
    The data is now loaded in the gold layer, which is the layer where the data is stored in a structured format suitable for Data WareHousing and Business Intelligence purposes. 

    The data locatd here is not versioned by date, instead, is separated in Dimension and Facts tables, that can be queried by BI systems.


## Instructions

The have added in the Pipfile pandas as the only external dependency, and the tests are written using the unittest module.

In a productive system, pandas would have to be substituted by a more robust library, that would support reading the dataframes from a persistent storage, and that would support the dataframes to be stored in a persistent storage. PySpark is a good candidate for this.

Alternatively the data could be stored in a database, and the ETL process could be done using SQL.

The tests are written using the unittest module, and the TDD approach is used. The tests are located in the test_main.py file contains the 
tests for the main.py file.

There is infrascture as code, that is optional, and is located in the iac folder. The terraform folder contains the code to create an Azure Blob Storage, and the instructions to deploy the Terraform script are located in the README.md file.

## Execution

The make file has the following targets:

- help: Shows the help message
- test: Runs the tests
- execute: Runs the main.py file
- build-docker-image : Builds the docker image
- tidy : Formats the code using black
- lint: Lints the code using flake8
- azure-login : Logs in to Azure
- init-terraform : Initializes terraform
- plan-terraform : Plans the terraform
- apply-terraform : Applies the terraform
- deploy : Deploys the docker image to Azure Container Registry and creates an Azure Container Instance

Here you have some examples of how to use the make file:

```bash
# Run the tests
make test

# Run the main.py file
make execute

# Build the docker image
make build-docker-image

# Format the code using black
make tidy

# Lint the code using flake8
make lint

# Log in to Azure
make azure-login

# Initialize terraform
make init-terraform

# Plan the terraform
make plan-terraform

# Apply the terraform
make apply-terraform

# Deploy the docker image to Azure Container Registry and create an Azure Container Instance
make deploy
```

## Infrastucture as Code

I am showing below the commands necessary to create the infrastructure in Azure using Azure CLI. In hte iac folder, you can also find the terraform code to create the infrastructure.

The general idea is to use the following components in Azure:

- Azure Data Lake Storage: To store the data in the bronze, silver and gold layers, since it allows to store a hierarchical file system, in constract with a plan storage account.
- Azure Container Registry: To store the docker image
- Azure Container Instance: To run the docker image, this is serverless and is a good option for a simple deployment
- Azure Logic App: To trigger the container instance when data is uploaded to the blob storage. This is a serverless no-code solution, that is easy to use and is a good option for a simple deployment.

Please note that Logic Apps as of now, cannot be deployed with the terraform script, or using the Azure CLI, so the logic app has to be created manually.

The following commands are necessary to create the infrastructure in Azure using Azure CLI:

```bash
# Login to Azure using Azure CLI
az login

# Create a resource group
az group create --name parloaint --location westeurope

# Create a storage account
az storage account create --name parloaintsa --resource-group parloaint --location westeurope --kind StorageV2 --sku Standard_LRS --hierarchical-namespace true

# Get the connection string for the storage account
connectionString=$(az storage account show-connection-string --name parloaintsa --resource-group parloaint --query connectionString --output tsv)

# Create a container in the blob storage
az storage container create --name lakehouse --connection-string $connectionString

# Create an Azure Container Registry
az acr create --name parloaintacr --resource-group parloaint --location westeurope --sku Basic --admin-enabled true

# Log in to Azure Container Registry
az acr login --name parloaintacr

# Tag your local image
docker tag parloa-data-engineering-challenge:0.0.1 parloaintacr.azurecr.io/parloa-data-engineering-challenge:0.0.1


# Push the image to ACR
docker push parloaintacr.azurecr.io/parloa-data-engineering-challenge:0.0.1

# Show credentials, after creating the ACR
az acr credential show --name parloaintacr

# Create an Azure Container Instance
az container create --name parloaaci --resource-group parloaint --location westeurope --image parloaintacr.azurecr.io/parloa-data-engineering-challenge:0.0.1 --cpu 1 --memory 1 --restart-policy Never --registry-login-server parloaintacr.azurecr.io --registry-username parloaintacr --registry-password KaizeXQESpMn/4sBjyZ0KI7qV5PDRz39X0InCS3Sdo+ACRBK0yo+ --dns-name-label parloaaci --ports 80

# Show the logs of the container
az container logs --name parloaaci --resource-group parloaint

# Create a logic app
az logic workflow create --resource-group parloaint --location westeurope --name parloaint-logic-app

# Create a logic app that triggers the container instance when data is uploaded to the blob storage
az logic workflow create --resource-group parloaint --location westeurope --name parloa-logic-app --definition "@logic-app-definition.json"
```



# @logic-app-definition.json
Generate the @logic-app-definition.json, necessary to trigger the container instance when data is uploaded to the blob storage
```json
{
    "definition": {
        "$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
        "actions": {},
        "contentVersion": "1.0.0.0",
        "outputs": {},
        "triggers": {
            "When_a_blob_is_added_or_updated": {
                "inputs": {
                    "parameters": {
                        "path": "lakehouse/data/"
                    },
                    "serviceProviderConfiguration": {
                        "connectionName": "AzureBlob",
                        "operationId": "whenABlobIsAddedOrModified",
                        "serviceProviderId": "/serviceProviders/AzureBlob"
                    }
                },
                "type": "ServiceProvider"
            }
        }
    },
    "kind": "Stateful"
}
```

## Other requirements

1. What kind of data quality measures would you apply to your solution in production?

In a production system that is close to Azure, I would adopt services like Azure Data Factory, Azure Databricks, and Azure Synapse Analytics, that would allow me to create a more robust ETL process, and that would allow me to create a more robust data quality measures.

Alternatively, I could use a more robust library like PySpark, that would allow me to create a more robust ETL process, and that would allow me to create a more robust data quality measures.

It is also possible to drive this process with Open Source tools like Apache Airflow, Apache NiFi, and Apache Kafka, that would allow me to create a more robust ETL process, and that would allow me to create a more robust data quality measures.

Additionally, it would make sense to have different environments to test the ETL process, and to have a CI/CD pipeline to deploy the ETL process.

On top of this there is governance and security measures that have to be taken into account, and that are not covered in this solution. Topics like lineage and data catalog, are critical to a robust solution, and are partially addressed by the medallion architecture, but are not covered in this solution.

2. What would need to change for the solution scale to work with a 10TB dataset with 5GB new data arriving each day?

To handle extra 5 Gb per day, it is possible to be done partially the same solution, since this amount of data would fit an in-memory process, like pandas. However, to have more robustness, it would be necessary to use a more robust library like PySpark, that would allow me to create a more robust ETL process, and that would allow me to create a more robust data quality measures.

Additionally, adding parallelism to the solution could be necessary if we have constraints regarding the time to process the data. Here leveraging Spark clusters, and using the Azure Databricks service, would be a good option. Other options that are Azure native are Azure Synapse Analytics, and Azure Data Factory or Microsoft Fabric, all of which enable Spark clusters, handle delta tables, and integrate with the governance mechanisms of Azure.

Last but not least, there is th observability aspect of the solution, that is not covered in this solution and that would be necessary to handle a 10TB dataset with 5GB new data arriving each day. This would include monitoring, logging, and alerting, and would be necessary to have a robust solution.

# Conclusion

In this project I've create a simple ETL process that leverages a medallion architecture, that ingest raw data, processes it, sanitized it and then sets it reading for consumption in ML workloads (silver layer) and BI workloads (gold layer)

The solution is simple, and is not meant to be used in production, but it is a good starting point to understand the ETL process, and to understand the medallion architecture. It also displays have to deploy a docker image to Azure Container Registry, and how to create an Azure Container Instance, both with Azure CLI, and Terraform.

Finally the data, that is generated is also bundled with the solution in the data directory
