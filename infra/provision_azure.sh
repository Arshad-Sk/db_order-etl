#!/usr/bin/env bash
# Provision ADLS Gen2 + Access Connector for Unity Catalog. Usage: ./provision_azure.sh dev
set -euo pipefail
ENV="${1:-dev}"
RG="rg-orders-etl-${ENV}"; LOC="uaenorth"; SA="REPLACEorders${ENV}sa"   # 3-24 chars, lowercase, globally unique
AC="ac-orders-etl-${ENV}"

az group create -n "$RG" -l "$LOC"
az storage account create -n "$SA" -g "$RG" -l "$LOC" --sku Standard_LRS --kind StorageV2 \
  --enable-hierarchical-namespace true --min-tls-version TLS1_2 --allow-blob-public-access false
for c in landing lakehouse; do az storage container create --account-name "$SA" -n "$c" --auth-mode login; done

az extension add --name databricks --upgrade -y
az databricks access-connector create -g "$RG" -n "$AC" -l "$LOC" --identity-type SystemAssigned
PRINCIPAL=$(az databricks access-connector show -g "$RG" -n "$AC" --query identity.principalId -o tsv)
SCOPE=$(az storage account show -n "$SA" -g "$RG" --query id -o tsv)
az role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$SCOPE"

echo "Access connector resource id (use it to create the Storage Credential in Databricks):"
az databricks access-connector show -g "$RG" -n "$AC" --query id -o tsv
