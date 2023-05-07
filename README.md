# Metastore in Apache Spark

Hive provides with a SQL-compatible language on Hadoop using. But it is very difficult to understand even difference between Hive and Presto / Spark. 
I learn how the Hive can preserve the processing result in storage via enableHiveSupport option at initiation of Spark Session, because I don't have any pure Hive environments.

| |	Apache Hive |	Presto / Spark |
| :--- | :--- | :--- |
| Intermediate result |	Write to Storage |	Write to Memory |
| Performance |	Slow |	Fast |
| Use cases |	Large data aggregations | Interactive queries and quick data exploration |
| Checkpoint of Failure |	Resume from intermediate data saved in storage | Start over |

See also URL below:
> https://github.com/developer-onizuka/AzureDataFactory#4-difference-between-hive-presto-and-spark
> https://medium.com/@sarfarazhussain211/metastore-in-apache-spark-9286097180a4

# 0. Create Virtual Machine and Run mongoDB
See URL below:
> https://github.com/developer-onizuka/mongo-Spark
 
# 1. Create Spark Session with Hive
Enabling hive support, allows Spark to seamlessly integrate with existing Hive installations, and leverage Hive’s metadata and storage capabilities.
When using Spark with Hive, you can read and write data stored in Hive tables using Spark APIs. This allows you to take advantage of **the performance optimizations and scalability benefits of Spark while still being able to leverage the features and benefits of Hive**.

```
from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName("myapp") \
        .master("local") \
        .config("spark.executor.memory", "1g") \
        .config("spark.mongodb.input.uri","mongodb://172.17.0.3:27017") \
        .config("spark.mongodb.output.uri","mongodb://172.17.0.3:27017") \
        .config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector_2.12:3.0.0") \
        .enableHiveSupport() \
        .getOrCreate()
```

# 2. Get config and Confirm Catalog Implementation
If you create the spark session without enableHiveSupport(), then the output of spark.sql.catalogImplementation must be None. Spark SQL defaults is in-memory (non-Hive) catalog.
```
conf = spark.sparkContext.getConf()
print("# spark.app.name = ", conf.get("spark.app.name"))
print("# spark.master = ", conf.get("spark.master"))
print("# spark.executor.memory = ", conf.get("spark.executor.memory"))
print("# spark.sql.warehouse.dir = ", conf.get("spark.sql.warehouse.dir"))
print("# spark.sql.catalogImplementation = ", conf.get("spark.sql.catalogImplementation"))

# spark.app.name =  myapp
# spark.master =  local
# spark.executor.memory =  1g
# spark.sql.warehouse.dir =  file:/home/jovyan/spark-warehouse
# spark.sql.catalogImplementation =  hive
```

# 3. Extract data from mongoDB to DataFrame in Spark
```
df = spark.read.format("mongo") \
               .option("database","test") \
               .option("collection","products") \
               .load()
```

# 4. Save the DataFrame as a persistent table
DataFrames can also be saved as persistent tables into Hive metastore using the saveAsTable command which will materialize the contents of the DataFrame and create a pointer to the data in the Hive metastore. If the spark session is restarted, then DataFrames have gone. But the Hive metastore allows to query to the persistent table even after spark session is restared.
```
df.write.mode("overwrite").saveAsTable("products_new")
```

# 5. Find the Hive Metastore and Parquet files
Spark automatically creates metastore (metastore_db) in the current directory, deployed with default Apache Derby and also creates a directory configured by spark.sql.warehouse.dir to store the Spark tables (essentially it's a collection of parquet files), which defaults to the directory spark-warehouse in the current directory. The default format is "parquet" so if you don’t specify it, it will be assumed. 
> https://towardsdatascience.com/notes-about-saving-data-with-spark-3-0-86ba85ca2b71
<br>
The Hive metastore preserves _**an association between the parquet file and a database**_ created with saveAsTable(), even if a spark session is restarted.

```
%ls -l
total 24
-rw-r--r-- 1 jovyan users  672 May  7 05:10 derby.log
drwxr-sr-x 5 jovyan users 4096 May  7 05:10 metastore_db/
drwxr-sr-x 3 jovyan users 4096 May  7 05:08 spark-warehouse/
-rw-r--r-- 1 jovyan users 5891 May  7 05:22 Untitled.ipynb
drwsrwsr-x 1 jovyan users 4096 May  7 05:10 work/
```
```
%ls -l spark-warehouse/products_new
total 16
-rw-r--r-- 1 jovyan users 13401 May  7 06:07 part-00000-ff9a9aac-0f2a-4b4d-a856-417c2cd411fd-c000.snappy.parquet
-rw-r--r-- 1 jovyan users     0 May  7 06:07 _SUCCESS
```
```
%cat derby.log
----------------------------------------------------------------
Sun May 07 05:10:06 UTC 2023:
Booting Derby version The Apache Software Foundation - Apache Derby - 10.14.2.0 - (1828579): instance a816c00e-0187-f49d-f849-0000042485f8 
on database directory /home/jovyan/metastore_db with class loader jdk.internal.loader.ClassLoaders$AppClassLoader@5ffd2b27 
Loaded from file:/usr/local/spark-3.2.1-bin-hadoop3.2/jars/derby-10.14.2.0.jar
java.vendor=Ubuntu
java.runtime.version=11.0.13+8-Ubuntu-0ubuntu1.20.04
user.dir=/home/jovyan
os.name=Linux
os.arch=amd64
os.version=5.4.0-139-generic
derby.system.home=null
Database Class Loader started - derby.database.classpath=''
```

# 6. Query the persistent table after restarting Spark Session
You can query again even after spark.stop() and creating the session again. 
```
spark.sql("SELECT * FROM products_new WHERE StandardCost > 2000").show()
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
|ListPrice|MakeFlag|ModelName|ProductID|     ProductName|ProductNumber|StandardCost|SubCategoryID|                 _id|
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
|  3578.27|       1| Road-150|      749|Road-150 Red, 62|   BK-R93R-62|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      750|Road-150 Red, 44|   BK-R93R-44|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      751|Road-150 Red, 48|   BK-R93R-48|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      753|Road-150 Red, 56|   BK-R93R-56|   2171.2942|            2|{6456f3d06fcaf22f...|
|  3578.27|       1| Road-150|      752|Road-150 Red, 52|   BK-R93R-52|   2171.2942|            2|{6456f3d06fcaf22f...|
+---------+--------+---------+---------+----------------+-------------+------------+-------------+--------------------+
```
