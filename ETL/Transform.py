# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

def hora_categoria(hour):
    if hour < 6:
        return "Madrugada"
    elif 6 <= hour < 12:
        return "Mañana"
    elif 12 <= hour < 18:
        return "Tarde"
    else:
        return "Noche"

# COMMAND ----------

hour_udf = F.udf(hora_categoria, StringType())

# COMMAND ----------

df_aisles = spark.table(f"{catalogo}.{esquema_source}.aisles")
df_departments = spark.table(f"{catalogo}.{esquema_source}.departments")
df_orders = spark.table(f"{catalogo}.{esquema_source}.orders")
df_products = spark.table(f"{catalogo}.{esquema_source}.products")

# COMMAND ----------

df_aisles = df_aisles.dropna(how="all")\
                        .filter((col("ID_aisle").isNotNull()) | (col("Aisle")).isNotNull())

df_departments = df_departments.dropna(how="all")\
                    .filter((col("ID_Department").isNotNull()) | (col("Department")).isNotNull())

df_orders = df_orders.dropna(how="all")\
                    .filter((col("ID_Orden").isNotNull()) | (col("ID_Usuario")).isNotNull())

df_products = df_products.dropna(how="all")\
                    .filter((col("ID_producto").isNotNull()) | (col("Nombre_producto")).isNotNull())

# COMMAND ----------

df_products.show()

# COMMAND ----------

df_orders = df_orders.withColumn("hora_categoria", hour_udf("Orden_hora"))

# COMMAND ----------

df_orders.show()

# COMMAND ----------

df_joined = df_products.alias("x").join(df_aisles.alias("y"), col("x.ID_aisle") == col("y.ID_aisle"), "inner").drop(df_aisles.ID_aisle)

df_joined.show()

# COMMAND ----------

df_joined.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.table_transformed")
df_orders.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.orders_transformed")