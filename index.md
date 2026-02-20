# CYT180 — Lab 6: Getting Started with PySpark in Google Colab
**Weight:** 3% <br>
**Work Type:** Individual <br>
**Submission Format:** 2 minutes 30 seconds video, see submission instructions.

----

## Introduction
PySpark is the Python interface for Apache Spark, useful for distributed data processing and pipelines. Even on modest data, Spark’s DataFrame API and Spark SQL can make many data-wrangling tasks shorter, clearer, and easier to maintain than pandas.
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
PySpark is the Python interface for Apache Spark, allowing scalable distributed data processing. In this lab, we will run Spark inside Google Colab, which requires installing Java, installing PySpark, and creating a Spark session.
