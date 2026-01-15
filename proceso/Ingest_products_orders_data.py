# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

dbutils.widgets.text("storage_name", "adlsprojectsm")
dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "catalog_dev")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

storage_name = dbutils.widgets.get("storage_name")
container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/products.csv"
ruta_orders = f"abfss://{container}@{storage_name}.dfs.core.windows.net/orders.csv"

# COMMAND ----------

products_schema = StructType(fields=[StructField("product_id", IntegerType(), False),
                                  StructField("product_name", StringType(), True),
                                  StructField("aisle_id", IntegerType(), True),
                                  StructField("department_id", IntegerType(), True)
])

oders_schema = StructType(fields=[StructField("order_id", IntegerType(), False),
                                  StructField("user_id", IntegerType(), True),
                                  StructField("eval_set", StringType(), True),
                                  StructField("order_number", IntegerType(), True),
                                  StructField("order_row", IntegerType(), True),
                                  StructField("order_hour_of_day", IntegerType(), True),
                                  StructField("days_since_prior_order", DoubleType(), True)
])

# COMMAND ----------

products_df = spark.read \
            .option("header", True) \
            .schema(products_schema) \
            .csv(ruta)

orders_df = spark.read \
            .option("header", True) \
            .schema(oders_schema) \
            .csv(ruta_orders)

# COMMAND ----------

products_with_timestamp_df = products_df.withColumn("ingestion_date", current_timestamp())
orders_with_timestamp_df = orders_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

products_with_timestamp_df.show()

# COMMAND ----------

products_selected_df = products_with_timestamp_df.select(col('product_id').alias('ID_Producto'), 
                                                   col('product_name').alias('Nombre_producto'), 
                                                   col('aisle_id').alias('ID_aisle'),
                                                   col('department_id').alias('ID_departamento'))

orders_selected_df = orders_with_timestamp_df.select(col('order_id').alias('ID_Orden'), 
                                                   col('user_id').alias('ID_Usuario'), 
                                                   col('eval_set').alias('Evaluacion'),
                                                   col('order_number').alias('Numero_de_orden'),
                                                   col('order_row').alias('Orden_fila'),
                                                   col('order_hour_of_day').alias('Orden_hora'),
                                                   col('days_since_prior_order').alias('Orden_dias'))

# COMMAND ----------

products_selected_df.show()

# COMMAND ----------

products_selected_df.write.mode('overwrite').saveAsTable(f'{catalogo}.{esquema}.products')

orders_selected_df.write.mode('overwrite').partitionBy('Orden_hora').saveAsTable(f'{catalogo}.{esquema}.orders')