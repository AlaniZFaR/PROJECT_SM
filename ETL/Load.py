# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "golden")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

df_joined = spark.table(f"{catalogo}.{esquema_source}.table_transformed")
df_orders = spark.table(f"{catalogo}.{esquema_source}.orders_transformed")

# COMMAND ----------

orders_transformed_df = (df_orders.groupBy(col("hora_categoria"))
    .agg(
        count(col("ID_Orden")).alias("total_ordenes"),
        avg(col("Orden_dias")).alias("avg_dias_entre_ordenes"),
        max(col("Orden_dias")).alias("max_dias_entre_ordenes"),
        min(col("Orden_dias")).alias("min_dias_entre_ordenes"),
        max(col("Orden_hora")).alias("hora_maxima"),
        min(col("Orden_hora")).alias("hora_minima")
    )
    .orderBy(col("hora_categoria").asc())
)

# COMMAND ----------

orders_transformed_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_parameters_partitioned")

df_joined.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_products_partitioned")

df_orders.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.golden_orders_partitioned")