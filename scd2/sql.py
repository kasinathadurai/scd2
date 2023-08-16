# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists scd
# MAGIC

# COMMAND ----------

# MAGIC %run ./init.py

# COMMAND ----------

# MAGIC %sql
# MAGIC use scd;
# MAGIC
# MAGIC select * from employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS scd.scdType2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (employee_id - 1) AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date FROM scd.employees
# MAGIC ORDER BY employee_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE scd.scdType2 USING delta AS
# MAGIC SELECT (employee_id - 1) AS id, employee_id, first_name, last_name, gender, address_street, address_city, address_country, email, job_title, start_date, end_date FROM scd.employees
# MAGIC ORDER BY employee_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from scd.scdType2
