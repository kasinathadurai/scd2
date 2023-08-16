# Databricks notebook source
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark.catalog.setCurrentDatabase("scd")


# COMMAND ----------

scdType2DF = spark.table("scd.scdType2")

# COMMAND ----------

display(scdType2DF.orderBy("employee_id"))

# COMMAND ----------

display(scdType2DF.where("isnotnull(end_date)").orderBy("employee_id"))
display(scdType2DF.where("isnull(end_date)").orderBy("employee_id"))

# COMMAND ----------

dataForDF = [
(None, 6, 'Fred', 'Flintoff', 'Male', '3686 Dogwood Road', 'Phoenix', 'United States', 'fflintoft5@unblog.fr', 'Financial Advisor'),
(None, 21, 'Hilary', 'Thompson', 'Female', '4 Oxford Pass', 'San Diego', 'United States', 'hcasillisk@washington.edu', 'Senior Sales Associate')
]

# Create Schema structure
schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("employee_id", IntegerType(), True),
  StructField("first_name", StringType(), True),
  StructField("last_name", StringType(), True),
  StructField("gender", StringType(), True),
  StructField("address_street", StringType(), True),
  StructField("address_city", StringType(), True),
  StructField("address_country", StringType(), True),
  StructField("email", StringType(), True),
  StructField("job_title", StringType(), True)
])

# Create as Dataframe
scd2Temp = spark.createDataFrame(dataForDF, schema)

# Preview dataset
display(scd2Temp)

# COMMAND ----------

empList = scd2Temp.select(collect_list(scd2Temp['employee_id'])).collect()[0][0]
empList


# COMMAND ----------

scdChangeRows = scd2Temp.selectExpr(
  "null AS id", "employee_id", "first_name", "last_name", "gender", "address_street", "address_city", "address_country", "email", "job_title", "current_date AS start_date", "null AS end_date"
)

scdChangeRows.show()

# COMMAND ----------

scdChangeRows = scdChangeRows.unionByName(
  scdType2DF  
  .where(col("employee_id").isin(empList)), allowMissingColumns=True
)

display(scdChangeRows)


# COMMAND ----------

# Convert table to Delta
deltaTable = DeltaTable.forName(spark, "scdType2")

# Merge Delta table with new dataset
(
  deltaTable
    .alias("original2")
    # Merge using the following conditions
    .merge( 
      scdChangeRows.alias("updates2"),
      "original2.id = updates2.id"
    )
    # When matched UPDATE ALL values
    .whenMatchedUpdate(
    set={
      "original2.end_date" : current_date()
    }
    )
    # When not matched INSERT ALL rows
    .whenNotMatchedInsertAll()
    # Execute
    .execute()
)

# COMMAND ----------

scdType2DF.selectExpr( 
  "ROW_NUMBER() OVER (ORDER BY id NULLS LAST) - 1 AS id", "employee_id", "first_name", "last_name", "gender", "address_street", "address_city", "address_country", 
  "email", "job_title", "start_date", "end_date"
).write.insertInto(tableName="scdType2", overwrite=True)

# COMMAND ----------

display(
  sql("SELECT * FROM scdType2 WHERE employee_id = 6 OR employee_id = 21")
)
