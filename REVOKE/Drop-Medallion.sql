-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC No estoy usando eliminación de rutas físicas porque solo trabajo con tablas managed (por lo que solo basta con eliminar las lógicas)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("nameContainer","unit-catalog")
-- MAGIC dbutils.widgets.text("nameStorage","adlsprojectsm")
-- MAGIC dbutils.widgets.text("catalogo","catalog_dev")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC nameContainer = dbutils.widgets.get("nameContainer")
-- MAGIC nameStorage = dbutils.widgets.get("nameStorage")
-- MAGIC catalogo = dbutils.widgets.get("catalogo")
-- MAGIC
-- MAGIC ruta = f"abfss://{nameContainer}@{nameStorage}.dfs.core.windows.net"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Bronze

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.bronze.aisles;
DROP TABLE IF EXISTS ${catalogo}.bronze.departments;
DROP TABLE IF EXISTS ${catalogo}.bronze.orders;
DROP TABLE IF EXISTS ${catalogo}.bronze.products;

-- COMMAND ----------

DESCRIBE EXTENDED catalog_dev.silver.orders_transformed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Silver

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.silver.orders_transformed;
DROP TABLE IF EXISTS ${catalogo}.silver.table_transformed;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Golden

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.golden.golden_orders_partitioned;
DROP TABLE IF EXISTS ${catalogo}.golden.golden_parameters_partitioned;
DROP TABLE IF EXISTS ${catalogo}.golden.golden_products_partitioned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Eliminacion tablas Exploratory

-- COMMAND ----------

-- DROP TABLES
DROP TABLE IF EXISTS ${catalogo}.exploratory.EXPERIMENTOS;
DROP TABLE IF EXISTS ${catalogo}.exploratory.PRUEBAS_MODELOS_EXT;