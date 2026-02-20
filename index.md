# CYT180 — Lab 6: Getting Started with PySpark in Google Colab
**Weight:** 3% <br>
**Work Type:** Individual <br>
**Submission Format:** 2 minutes 30 seconds video, see submission instructions.

----

## Introduction
Introduction
PySpark is the Python interface for Apache Spark, useful for distributed data processing and pipelines. Even on modest data, Spark’s DataFrame API and Spark SQL can make many data‑wrangling tasks shorter, clearer, and easier to maintain than pandas.

Docs: <http://spark.apache.org/docs/latest/api/python/>

----

## Learning Objectives
By the end of this lab, you will be able to:

- Initialize a Spark session in Colab and verify the environment.
- Load CSV data into a Spark DataFrame and inspect schema & summary stats.
- Perform common transformations (select, filter, withColumn, groupBy).
- Run Spark SQL queries using temporary views.
- Explain the difference between transformations and actions.
- Observe performance concepts: lazy evaluation, caching, and partitions.

  ---
## Section 1 — Getting Started
PySpark is the Python interface for Apache Spark, allowing scalable distributed data processing. In this lab, we will run Spark inside Google Colab. PySpark requires a Java runtime, the Spark framework, and a Python interface layer. Google Colab does not include Spark by default, so in this section we prepare the environment so Spark can run inside the notebook.

### 1. Install Java 11 and PySpark
Spark requires **Java 8+, but Spark 3.5.x recommends Java 11**, so we install it manually.
We also install pyspark and findspark so Python can initialize Spark correctly. **pyspark** provides the Python interface, and **findspark** allows Python to locate your Spark installation inside Colab

```
# Install Java 11 (required for Apache Spark)
!apt-get update -qq
!apt-get install -qq openjdk-11-jdk-headless

# Install PySpark and findspark (version pinned for compatibility)
!pip -q install pyspark==3.5.2 findspark
```

### 2. Download Spark 3.5.2 (Hadoop 3)
Although pip install pyspark includes a bundled Spark runtime, downloading the actual Spark distribution allows you to:

- explore Spark’s directory structure
- experiment with configuration files later
- match the official Spark/Hadoop version numbers used in production systems
  
```
# Download and extract Spark 3.5.2 (Hadoop 3)
!wget -q https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz
!tar xf spark-3.5.2-bin-hadoop3.tgz
```

### 3. Initialize Spark
Now we create the **SparkSession**, which is the entry point to all Spark functionality. Every Spark program starts with a **SparkSession**. It connects the notebook to the Spark engine and allows creation of DataFrames, SQL queries, and distributed operations.

```
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CYT180 - Colab Spark 3.5.2 Lab") \
    .getOrCreate()

spark
```

### 4. Verify Your Environment
Before continuing, confirm that Spark is installed and configured correctly.
If anything failed during installation (Java mismatch, corrupted download, missing environment variables), this check will reveal it immediately.

```
import pyspark
print("PySpark version:", pyspark.__version__)
print("Spark master:", spark.sparkContext.master)
print("Python version (driver):", spark.sparkContext.pythonVer)
```
