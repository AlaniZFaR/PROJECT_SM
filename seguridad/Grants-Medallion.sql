-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Grants

-- COMMAND ----------

GRANT SELECT ON TABLE catalog_dev.bronze.aisles TO `devs`;
GRANT SELECT ON TABLE catalog_dev.bronze.products TO `devs`;

-- COMMAND ----------

GRANT USE CATALOG ON CATALOG catalog_dev TO `alanis@gmail.com`;

-- COMMAND ----------

GRANT USE SCHEMA ON SCHEMA catalog_dev.bronze TO `devs`;
GRANT USE SCHEMA ON SCHEMA catalog_dev.bronze TO `QAs`;
GRANT CREATE ON SCHEMA catalog_dev.bronze TO `devs`;

-- COMMAND ----------

GRANT CREATE TABLE ON SCHEMA catalog_dev.bronze TO `alanis@gmail.com`;

-- COMMAND ----------

GRANT SELECT ON TABLE catalog_dev.bronze.orders TO `devs`;
GRANT SELECT ON TABLE catalog_dev.silver.orders_transformed TO `QAs`;

-- COMMAND ----------

GRANT READ FILES
ON EXTERNAL LOCATION `exlt-raw`
TO `alanis@gmail.com`;

GRANT READ FILES
ON EXTERNAL LOCATION `exlt-raw`
TO `devs`;

-- COMMAND ----------

GRANT WRITE FILES
ON EXTERNAL LOCATION `exlt-bronze`
TO `devs`;

-- COMMAND ----------

GRANT ALL PRIVILEGES
ON EXTERNAL LOCATION `exlt-bronze`
TO `alanis@gmail.com`;