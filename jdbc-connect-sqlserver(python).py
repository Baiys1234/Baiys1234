# Databricks notebook source
jdbcHostname = "cicisqlserver.database.windows.net"
jdbcDatabase = "cicisqldatabase"
jdbcPort = 1433
jdbcUsername = "dbuser"
jdbcPassword = "Zaq12wsxcde#"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

pushdown_query = "(select * from dbo.hosts) hosts"
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------


