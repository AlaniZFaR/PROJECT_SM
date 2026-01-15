# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

ruta = f"abfss://{container}@{storage_name}.dfs.core.windows.net/aisles.csv"
ruta_d = f"abfss://{container}@{storage_name}.dfs.core.windows.net/departments.csv"

# COMMAND ----------

df_aisle = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta)

df_department = spark.read.option('header', True)\
                        .option('inferSchema', True)\
                        .csv(ruta_d)

# COMMAND ----------



# COMMAND ----------

aisle_schema = StructType(fields=[StructField("aisle_id", IntegerType(), True),
                                     StructField("aisle", StringType(), True)])

department_schema = StructType(fields=[StructField("department_id", IntegerType(), True),
                                     StructField("department", StringType(), True)])

# COMMAND ----------

# DBTITLE 1,Use user specified schema to load df with correct types
df_aisle_final = spark.read\
.option('header', True)\
.schema(aisle_schema)\
.csv(ruta)

df_department_final = spark.read\
.option('header', True)\
.schema(department_schema)\
.csv(ruta_d)


# COMMAND ----------

# DBTITLE 1,select only specific cols
aisle_selected_df = df_aisle_final.select(col("aisle_id"), col("aisle"))
department_selected_df = df_department_final.select(col("department_id"), col("department"))

# COMMAND ----------

aisle_renamed_df = aisle_selected_df.withColumnRenamed("aisle_id", "ID_aisle") \
    .withColumnRenamed("aisle", "Aisle")

department_renamed_df = department_selected_df.withColumnRenamed("department_id", "ID_Department") \
    .withColumnRenamed("department", "Department")

# COMMAND ----------

# DBTITLE 1,Add col with current timestamp 
aisle_final_df = aisle_renamed_df.withColumn("ingestion_date", current_timestamp())
department_final_df = department_renamed_df.withColumn("ingestion_date", current_timestamp())

aisle_final_df.show()

# COMMAND ----------

aisle_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.aisles")
department_final_df.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.departments")