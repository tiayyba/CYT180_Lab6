# CYT180 — Lab 6: Getting Started with PySpark in Google Colab
**Weight:** 3% <br>
**Work Type:** Individual <br>
**Submission Format:** screenshots.

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

----
  
## Section 1 — Getting Started
PySpark is the Python interface for Apache Spark, allowing scalable distributed data processing. In this lab, we will run Spark inside Google Colab. PySpark requires a Java runtime, the Spark framework, and a Python interface layer. Google Colab does not include Spark by default, so in this section we prepare the environment so Spark can run inside the notebook.

### 1. Install Java 11
Spark requires **Java 8+, but Spark 3.5.x recommends Java 11**, so we install it manually.
```python
# Install Java 11 (required for Apache Spark)
!apt-get update -qq
!apt-get install -qq openjdk-11-jdk-headless
```

### 2. Download Spark 3.5.2 (Hadoop 3)
We download the official precompiled binary distribution of Apache Spark version 3.5.2 (built for Hadoop 3).
This Spark distribution includes:
- The Spark engine (JVM-based execution framework)
- Built-in libraries and JAR files
- The bin/ scripts (spark-submit, spark-shell, etc.)
- The PySpark module inside the /python/ directory

This means the Spark binary already contains PySpark — we are not installing a separate Spark engine through pip.
  
```python
# Download and extract Spark 3.5.2 (Hadoop 3)
!wget -q https://archive.apache.org/dist/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz
!tar xf spark-3.5.2-bin-hadoop3.tgz
```
### 3. Install findspark

Although the Spark binary already contains `PySpark`, Python needs to know where Spark is installed.
We install `findspark` so Python can locate the Spark installation defined by `SPARK_HOME` and properly load the built-in PySpark module from the Spark distribution.

```python
# Install findspark so Python can locate SPARK_HOME
!pip -q install findspark
```

### 4. Set Environment Variables
```python
import os

# Set JAVA_HOME to Java 11 path
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"

# Set SPARK_HOME to the folder you just extracted
os.environ["SPARK_HOME"] = "/content/spark-3.5.2-bin-hadoop3"

# Prepend Spark bin to PATH
os.environ["PATH"] = os.environ["SPARK_HOME"] + "/bin:" + os.environ["PATH"]

# Prepend Java bin to PATH
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# Optional: check versions
!java -version
!echo $SPARK_HOME
```


### 5. Initialize Spark
Now we create the **SparkSession**, which is the entry point to all Spark functionality. Every Spark program starts with a **SparkSession**. It connects the notebook to the Spark engine and allows creation of DataFrames, SQL queries, and distributed operations.

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CYT180 - Colab Spark 3.5.2 Lab") \
    .getOrCreate()

spark
```

### 6. Verify Your Environment
Before continuing, confirm that Spark is installed and configured correctly.
If anything failed during installation (Java mismatch, corrupted download, missing environment variables), this check will reveal it immediately.

```python
import pyspark
print("PySpark version:", pyspark.__version__)
print("Spark master:", spark.sparkContext.master)
print("Default parallelism:", spark.sparkContext.defaultParallelism)
print("Python version (driver):", spark.sparkContext.pythonVer)
```
Notice that Spark runs in local[*] mode in Colab. This means Spark uses all available CPU cores in a single machine, not a real cluster.

----

## Section 2 — Creating a Simple DataFrame in PySpark
In this section, you will create your first PySpark DataFrame directly in memory.
This demonstrates how Spark structures data, infers schema, and provides basic descriptive statistics.
Unlike pandas, which loads data directly into local memory, Spark stores data in a distributed format that allows scaling to much larger datasets.

### 1. Creating an In-Memory DataFrame
We can manually define rows and column names, then pass them to spark.createDataFrame().

```python
from pyspark.sql import functions as F

data = [
    ('John',  '', 'Smith', '2000-01-01', 'M', 100000),
    ('Jane',  '', 'Smith', '1990-01-01', 'F', 150000),
    ('Jonas', '', 'Smith', '1995-01-01', 'M', 120000),
]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df_small = spark.createDataFrame(data=data, schema=columns)

df_small.show()
df_small.printSchema()
```
The above example:
- Creates a Spark DataFrame with six columns.
- Prints the data in a table format.
- Shows the inferred schema (Spark assigns types like string, bigint, etc.).

### 2. Getting Summary Statistics
Spark can quickly compute summary statistics across numeric columns:
```python
df_small.describe().show()
```
This provides the count, mean, standard deviation, min, and max for numeric columns (like salary).
Spark performs these operations in parallel, which becomes extremely powerful on large datasets.

### 3. Review Questions
Record your answers for the following questions:
- What data types did Spark infer for each column? Check using printSchema().
- Is describe() a transformation or an action? Why?
- Add a new column called age_rough using: `age_rough = 2025 - year(to_date(dob))` and show the updated DataFrame.

----

## Section 3 — Load a Real Dataset (OWID COVID‑19) and Explore
In this section, you will load a real CSV dataset (Our World in Data COVID‑19) into a Spark DataFrame, inspect its schema, convert data types, and perform basic exploratory operations. This mirrors a typical workflow used in data analytics and data engineering: <br>
**load → inspect → clean/convert → explore**

### 1. Download and Load the Dataset
We’ll download the CSV locally in Colab and let Spark load it from `/content`.
`inferSchema=True` helps Spark detect numeric types automatically.

```python
import requests, pathlib

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
local_path = pathlib.Path("/content/owid-covid-data.csv")

if not local_path.exists():
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    local_path.write_bytes(r.content)

df = spark.read.csv(str(local_path), header=True, inferSchema=True)

print("Row count (may take a few seconds):", df.count())
df.printSchema()
```

### 2. Convert the date Column Properly
Even with `inferSchema=True`, `date` columns is often load as strings in CSVs. We will convert `date` column to an actual date type so time‑based operations (like ordering, windows, and date math) work correctly.

``` python
from pyspark.sql import functions as F

df2 = df.withColumn("date", F.to_date("date"))
df2.select("location", "date", "new_cases", "new_deaths").show(5)
df2.printSchema()
```
- `withColumn()` creates a new date column cast to Spark’s date type.
- Verifies the new type via printSchema() and shows a sample.

### 3. Create an Analytical Subset
In real data processing, analysts often create focused subsets of large datasets. Your task is to create a smaller analytical DataFrame that includes: 
- Only these 5 countries:
  - United States
  - Canada
  - India
  - Brazil
  - Italy
- Only the most recent 120 days of data.
- Then you will:
  - Show the row count of your subset
  - Display the first 10 rows
 ```python
# Step 1: Define the selected countries
countries = ["United States", "Canada", "India", "Brazil", "Italy"]

# Step 2: Filter for those locations
df_subset = df2.filter(F.col("location").isin(countries))

# Step 3: Determine the most recent date in this filtered data
max_date = df_subset.agg(F.max("date")).first()[0]

# Step 4: Keep only the last 120 days
N = 120
df_subset = df_subset.filter(F.col("date") >= F.date_sub(F.lit(max_date), N))

# Step 5: Preview the subset
print("Number of rows in subset:", df_subset.count())
df_subset.select("location", "date", "new_cases", "new_deaths").show(10)
```
This task should help you learn:
- How to apply multi-condition filtering using .isin()
- How to compute an aggregated value (max(date))
- How to use date arithmetic to define a time window
- How transformations combine into a pipeline
- How actions (count, show) trigger computation

### 4. Filter and Order the Data
Let’s answer a simple question: What are the latest records for the United States?
```python
(df2
 .filter(F.col("location") == "United States")
 .orderBy(F.desc("date"))
 .select("location", "date", "new_cases", "new_deaths")
 .show(10))
```
Here:
- **filter** and **orderBy** are transformations (they build a plan).
- **show()** is an action (it executes the plan).

### 5. Aggregate: Total New Cases by Location
Compute total new cases grouped by location.

```python
agg_cases = (df2
    .groupBy("location")
    .agg(F.sum("new_cases").alias("total_new_cases"))
    .orderBy(F.desc("total_new_cases"))
)

agg_cases.show(10, truncate=False)
```
This step should help you learn:
- Combining groupBy and agg for summary statistics.
- show() triggers execution.
- These patterns are essential for Spark SQL and analytics.

### 6. Review Questions
- What data types were inferred for date, location, and new_cases before and after conversion in setp 2?
- Identify one transformation and one action from setp 4 and step 5.
- Show the latest 10 records for Canada (modify step 4).
- Modify step 5 to compute total new deaths by location.
- In step 3, what is the row count of your subset and what do the first 10 rows look like?
- Why is creating analytical subsets useful when working with large datasets?

----

## Section 4 — Transformations, Actions, and a 7‑Day Moving Average
In this section, you will deepen your understanding of how Spark processes data. Spark uses a lazy execution model, where transformations build up a computation plan and actions trigger execution. You will also compute a 7‑day moving average using Spark window functions — a common analytic technique when working with time‑series data.

### 1. Transformations vs. Actions
Let’s begin by reviewing the difference:
- **Transformations** These define operations on a DataFrame but do not execute immediately. Examples: select, filter, orderBy, groupBy, withColumn.
- **Actions**: These trigger execution and return a result. Examples: show, count, take, collect.
Knowing the difference is essential for understanding performance, execution plans, and caching.

### 2. Compute a 7‑Day Moving Average of New Cases
Time‑series analysis often uses moving averages to smooth daily fluctuations.
Here, you will calculate a 7‑day rolling average of `new_cases` for each country.

- **Define a window specification**
```python
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("date").rowsBetween(-6, 0)
```
- **Add the 7‑day average column**

```python
df3 = df2.withColumn("new_cases_7d_avg", F.avg("new_cases").over(w))

df3.select("location", "date", "new_cases", "new_cases_7d_avg") \
   .filter(F.col("location").isin("United States", "Canada")) \
   .orderBy("location", "date") \
   .show(12)
```
**What you should observe:**

- Rows now include both the raw new_cases and a smoothed new_cases_7d_avg.
- The first few dates for each location may show null if fewer than 7 days exist.

### 3. Why Moving Averages Matter
Daily case counts often fluctuate due to reporting delays, weekend effects, and batch updates.
A 7‑day average:

- reduces noise
- reveals general trends
- is widely used in epidemiology, finance, and operations analytics

### 4. Review Questions
- Modify the code to show the top 5 dates for Canada with the highest new_cases_7d_avg.
- In one sentence, explain why moving averages are useful in time‑series analysis.


----

## Section 5 — Performance: Lazy Evaluation
Spark is designed for distributed computing, and to achieve high performance it uses several key concepts one of which is lazy evaluation.
In this section, you will observe these behaviors directly.
Spark does not execute transformations immediately.
Instead, it builds a logical plan describing what should be done. Execution only begins when an action is called. The following code prints Spark’s logical and physical execution plans. It does not run the computations.

**View the Execution Plan (Does NOT run the job)**
```python
agg_cases.explain(mode="formatted")
```
**Trigger Execution With an Action**
```python
# count() is an action, so Spark executes the entire plan.
print("Number of rows in agg_cases:", agg_cases.count())
```

----


## Submission Instructions

Submit ** screenshots** that clearly show the work you completed in this lab. 
Your screenshots **must include** the following:

1. **Section 1:** Screenshot of Spark successfully initialized.
2. **Section 2**
  - Screenshot of df_small.describe().show()
  - Screenshot showing your added column age_rough
3. **Section 3**
    - Screenshot of df2.printSchema() after converting the date.
    - Screenshot of your subset row count
    - Screenshot of your latest 10 records for Canada
    - Screenshot of total new deaths by location

4. **Section 4 :** Screenshot showing the Hadoop streaming command and job completion
   - Screenshot of 7‑day moving average for the United States (first 12 rows)
   - Screenshot of top 5 days for Canada by new_cases_7d_avg

5. **Section 5 :**
   - Screenshot of your execution plan (agg_cases.explain())
   - Screenshot of the action execution (agg_cases.count() output)
6. **Written Answers to Reflection Questions from each section**
   - Include your answers directly beneath the relevant screenshot.
   - Answers should be short (1–2 sentences each)
7. Work should be complete, well organized and professionally presented. Too small, blurry or containing irrelevant output in screenshots will lead a deduction in marks up to 100%
