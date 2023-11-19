# Databricks notebook source
dbutils.widgets.text("db_name","","DataBase_Name")

# COMMAND ----------

db = dbutils.widgets.get("db_name")
if db  == "":
  raise Exception("empty database name not allower")

# COMMAND ----------

#########################################
#CREATE DATABASE
#########################################
def add_config_values(db):
  qry = """
  MERGE INTO configurations.configvalues AS T
  USING (
    SELECT 
      "system settings" AS group_name,
      "{0} db" AS config_name,
      "{0}" AS config_value,
      TRUE AS is_active,
      CURRENT_TIMESTAMP AS created_ts,
      CURRENT_TIMESTAMP AS last_modified_ts
    UNION ALL
      SELECT 
      "system settings" AS group_name,
      "{0} db folder path" AS config_name,
      "dbfs:/mnt/databases/{0}" AS config_value,
      TRUE AS is_active,
      CURRENT_TIMESTAMP AS created_ts,
      CURRENT_TIMESTAMP AS last_modified_ts
  ) AS S
  ON T.group_name = S.group_name
  AND T.config_name = S.config_name
  WHEN MATCHED THEN
  UPDATE SET
    T.config_value = S.config_value,
    T.last_modified_ts = S.last_modified_ts
  WHEN NOT MATCHED THEN
  INSERT(
    T.group_name,
    T.config_name,
    T.config_value,
    T.is_active,
    T.created_ts,
    T.last_modified_ts
  )
  VALUES(
    S.group_name,
    S.config_name,
    S.config_value,
    S.is_active,
    S.created_ts,
    S.last_modified_ts
  )
  """.format(db)
  print(qry)
  spark.sql(qry)
  print('--------------------------------------- \n')

# COMMAND ----------

#########################################
#CREATE DATABASE
#########################################
def create_db(db):
  qry = "CREATE DATABASE IF NOT EXISTS `{0}`".format(db)
  print(qry)
  spark.sql(qry)
  print('--------------------------------------- \n')

# COMMAND ----------

#########################################
#CREATE FOLDERS
#########################################
def create_folders(db):
  stg_folder =  f"""/mnt/databases/{db}/stage/"""
  data_folder =  f"""/mnt/databases/{db}/data/"""
  dbutils.fs.mkdirs(stg_folder)
  dbutils.fs.mkdirs(data_folder)
  print('--------------------------------------- \n')

# COMMAND ----------

def add_db(db):
  print("#########################################")
  
  #add config values
  print("Start: Adding configurations for database {0}".format(db))
  #add_config_values(db)

  #create database
  print("Start: Creating database {0}".format(db))
  create_db(db)
  
  #create folders
  print("Start: Creating folder for database {0}".format(db))
  create_folders(db)
  
  print("#########################################")

# COMMAND ----------

try:
  #prepare databases
  dbs = []
  if db.strip() not in dbs:
    dbs.append(db.strip())
  if db.strip() + '_history' not in dbs:
    dbs.append(db.strip() + '_history')
  print('databases to create --> ', dbs)

  #add dbs
  l = list(map(add_db, dbs))

  #optimize config
  #spark.sql("OPTIMIZE configurations.configvalues ")
except:
  raise

# COMMAND ----------

dbutils.fs.ls(f'/mnt/databases/{db}/')