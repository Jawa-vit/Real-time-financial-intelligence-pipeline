#!/bin/bash
# azure/deploy_infra.sh
# -----------------------------------------------------------------------------
# Provisions every Azure resource this pipeline needs:
#   - Resource Group
#   - ADLS Gen2 storage account with bronze/silver/gold containers
#   - Azure Database for PostgreSQL Flexible Server
#   - Azure Data Factory instance
#   - Azure Databricks workspace
#   - Azure Container Registry (for the producer Docker image)
#   - Key Vault (for secrets ADF/Databricks read at runtime)
#
# Prereqs: az CLI installed and `az login` already run.
# Usage:
#   chmod +x azure/deploy_infra.sh
#   ./azure/deploy_infra.sh
# -----------------------------------------------------------------------------
set -euo pipefail

# ---- Configuration (edit these) --------------------------------------------
RESOURCE_GROUP="rg-finance-pipeline"
LOCATION="eastus"
STORAGE_ACCOUNT="stfinancepipeline$RANDOM"   # must be globally unique, lowercase, no dashes
POSTGRES_SERVER="psql-finance-pipeline"
POSTGRES_ADMIN_USER="financeadmin"
POSTGRES_ADMIN_PASSWORD="ChangeMe_$(openssl rand -hex 6)!"   # rotate this immediately after creation
ADF_NAME="adf-finance-pipeline"
DATABRICKS_WORKSPACE="dbx-finance-pipeline"
ACR_NAME="acrfinancepipeline$RANDOM"
KEYVAULT_NAME="kv-finance-$RANDOM"
# -----------------------------------------------------------------------------

echo "==> Creating resource group: $RESOURCE_GROUP"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

echo "==> Creating ADLS Gen2 storage account: $STORAGE_ACCOUNT"
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

for container in bronze silver gold landing; do
  echo "    creating container: $container"
  az storage container create \
    --account-name "$STORAGE_ACCOUNT" \
    --name "$container" \
    --auth-mode login
done

echo "==> Creating PostgreSQL Flexible Server: $POSTGRES_SERVER"
az postgres flexible-server create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$POSTGRES_SERVER" \
  --location "$LOCATION" \
  --admin-user "$POSTGRES_ADMIN_USER" \
  --admin-password "$POSTGRES_ADMIN_PASSWORD" \
  --sku-name Standard_B1ms \
  --tier Burstable \
  --storage-size 32 \
  --version 16 \
  --public-access 0.0.0.0-255.255.255.255   # tighten this to ADF/your IP only in real use

az postgres flexible-server db create \
  --resource-group "$RESOURCE_GROUP" \
  --server-name "$POSTGRES_SERVER" \
  --database-name finance

echo "==> Creating Azure Data Factory: $ADF_NAME"
az datafactory create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ADF_NAME" \
  --location "$LOCATION"

echo "==> Creating Azure Databricks workspace: $DATABRICKS_WORKSPACE"
az databricks workspace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$DATABRICKS_WORKSPACE" \
  --location "$LOCATION" \
  --sku standard

echo "==> Creating Azure Container Registry: $ACR_NAME"
az acr create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ACR_NAME" \
  --sku Basic \
  --admin-enabled true

echo "==> Creating Key Vault: $KEYVAULT_NAME"
az keyvault create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$KEYVAULT_NAME" \
  --location "$LOCATION"

az keyvault secret set --vault-name "$KEYVAULT_NAME" --name "postgres-admin-password" --value "$POSTGRES_ADMIN_PASSWORD"

cat <<EOF

=================================================================
Provisioning complete. Save these values -- you'll need them for
GitHub Actions secrets, ADF linked services, and the .env file:

  RESOURCE_GROUP      = $RESOURCE_GROUP
  STORAGE_ACCOUNT      = $STORAGE_ACCOUNT
  POSTGRES_SERVER       = $POSTGRES_SERVER.postgres.database.azure.com
  POSTGRES_ADMIN_USER   = $POSTGRES_ADMIN_USER
  POSTGRES_ADMIN_PASSWORD (stored in Key Vault: $KEYVAULT_NAME, secret 'postgres-admin-password')
  ADF_NAME              = $ADF_NAME
  DATABRICKS_WORKSPACE  = $DATABRICKS_WORKSPACE
  ACR_NAME              = $ACR_NAME
  KEYVAULT_NAME         = $KEYVAULT_NAME

Next steps:
  1. Generate a Databricks PAT token from the workspace UI, store it in Key Vault
     as secret 'databricks-pat-token'.
  2. Import azure/adf/*.json into ADF Studio (or deploy via the ADF CLI/Terraform).
  3. Run postgres/schema.sql against the new Postgres database.
  4. Push the producer image to $ACR_NAME via the GitHub Actions workflow.
=================================================================
EOF
