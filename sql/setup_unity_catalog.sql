-- Run ONCE per environment by a metastore admin (replace placeholders). Dev shown; repeat for prod.
-- Prereq: a Storage Credential (e.g. `adls_mi_credential`) based on the Access Connector managed identity
--         (Catalog Explorer > External Data > Credentials), see infra/provision_azure.sh.

CREATE EXTERNAL LOCATION IF NOT EXISTS lakehouse_dev
  URL 'abfss://lakehouse@REPLACE_devstorageacct.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL adls_mi_credential);

CREATE EXTERNAL LOCATION IF NOT EXISTS landing_dev
  URL 'abfss://landing@REPLACE_devstorageacct.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL adls_mi_credential);

CREATE CATALOG IF NOT EXISTS dev_catalog
  MANAGED LOCATION 'abfss://lakehouse@REPLACE_devstorageacct.dfs.core.windows.net/dev_catalog';

-- Grants for the identity that runs the job (user in dev, service principal in prod)
GRANT USE CATALOG, CREATE SCHEMA, USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON CATALOG dev_catalog TO `REPLACE-principal`;
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION lakehouse_dev TO `REPLACE-principal`;
GRANT READ FILES ON EXTERNAL LOCATION landing_dev TO `REPLACE-principal`;
-- (the sample-data notebook also needs WRITE FILES on landing_dev)

-- Read access for analysts
-- GRANT USE CATALOG ON CATALOG prod_catalog TO `analysts`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA prod_catalog.gold TO `analysts`;
