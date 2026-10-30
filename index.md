# CYT180 — Lab 6: Joins, Data Cleaning, and Writing Data with PySpark

**Weight:** 3%
**Work Type:** Individual
**Submission Format:** Your Colab notebook printed to PDF (see Submission Guidelines)

---

## Introduction

In Lab 5, you installed Spark in Google Colab, created DataFrames, performed common transformations, ran basic Spark SQL, and computed a 7‑day moving average. That workflow focused on working with one dataset at a time.

In real Big Data engineering, a typical workflow involves combining multiple datasets, cleaning and standardizing them, computing meaningful metrics, and writing the results to efficient formats. These steps form the foundation of a simple ETL (Extract–Transform–Load) workflow. In this lab, you will practice three essential skills:

1. Joining multiple DataFrames using the DataFrame API and Spark SQL
2. Cleaning: handling missing values and duplicate rows
3. Writing results to disk using CSV and Parquet formats

You will mostly run provided code to observe Spark's behavior and complete one small task in most sections.

**Every student works with their own set of countries.** Your set is chosen by the last digit of your student ID, so your numbers will be different from your classmates' numbers. Submissions with another student's numbers will receive 0.

---

## Learning Objectives

By the end of this lab, you will be able to:

- Perform inner, left, and anti joins in PySpark.
- Detect and handle missing values using `dropna()` and `fillna()`.
- Remove duplicate rows using `dropDuplicates()`.
- Write DataFrames to disk using CSV and Parquet formats.
- Compare file sizes and explain why Parquet is preferred in big‑data systems.
- Explain how cleaning and joining datasets fit into a typical ETL (Extract‑Transform‑Load) workflow.
- Work with a real cyber security dataset (daily attack statistics by country).

---

## Prerequisites

- You will work in Google Colab.
- You should be able to start a SparkSession (from Lab 5).

### Setup — Install Spark

Run this cell once each time you open a new Colab session. It installs PySpark from pip, which includes Spark itself, so there is no large download from the Apache website (that server is often very slow). It also makes sure Java is available, because Spark runs on Java. It usually takes less than a minute.

```python
# Install PySpark (this includes Spark itself)
!pip -q install pyspark==3.5.3

# Spark needs Java. Colab usually has it already; install Java 11 only if it is missing.
import shutil, subprocess
if shutil.which("java") is None:
    subprocess.run("apt-get update -qq && apt-get install -qq openjdk-11-jdk-headless > /dev/null", shell=True)

!java -version
```

---

## Grading (10 marks)

| Item | Marks |
|---|---|
| Task 1 – Anti join | 1 |
| Task 2 – Data types | 1 |
| Task 3 – Latest 10 records for your first country | 1 |
| Task 4 – `web_threat_pct` column | 1 |
| Task 5 – Row counts before and after cleaning | 1 |
| Task 6 – Read Parquet back | 1 |
| Reflection questions R1–R4 (1 mark each) | 4 |
| **Total** | **10** |

The provided code cells are not graded on their own, but your notebook must show that you ran them. A task whose output is missing receives 0. A submission without the Section 0 cell, or with numbers that do not match your student ID, receives 0.

---

## Section 0 — Your Name, Student ID, and Country Set

Run this cell **first**, at the top of your notebook. Type your own full name and student ID between the quotes before running it.

```python
import datetime

STUDENT_NAME = "Your Full Name"    # <-- type your full name
STUDENT_ID   = "000000000"         # <-- type your 9-digit student ID

# Each set has 5 countries. The last digit of your student ID picks your set.
LAB_SETS = {
    0: ["Canada",         "Germany",        "Nigeria",     "Thailand",      "Peru"],
    1: ["India",          "France",         "Kenya",       "South Africa",  "Ukraine"],
    2: ["United States",  "Japan",          "Egypt",       "Spain",         "Vietnam"],
    3: ["Brazil",         "United Kingdom", "Pakistan",    "Australia",     "Iran"],
    4: ["Italy",          "Mexico",         "Ghana",       "Philippines",   "Poland"],
    5: ["Germany",        "Argentina",      "Bangladesh",  "Turkey",        "Morocco"],
    6: ["Japan",          "Canada",         "Ethiopia",    "Saudi Arabia",  "Colombia"],
    7: ["France",         "India",          "Indonesia",   "South Korea",   "Sri Lanka"],
    8: ["Argentina",      "Italy",          "Nigeria",     "Malaysia",      "Nepal"],
    9: ["Malaysia",       "Brazil",         "South Africa","United Kingdom","Algeria"],
}

if STUDENT_ID == "000000000" or STUDENT_NAME == "Your Full Name":
    raise ValueError("Type your own name and student ID in this cell first.")

SET_NUMBER   = int(STUDENT_ID.strip()[-1])
MY_COUNTRIES = LAB_SETS[SET_NUMBER]

print("Name:      ", STUDENT_NAME)
print("Student ID:", STUDENT_ID)
print("Set number:", SET_NUMBER)
print("Countries: ", MY_COUNTRIES)
print("Timestamp: ", datetime.datetime.now().isoformat())
```

Use the variables `STUDENT_ID` and `MY_COUNTRIES` in later cells exactly as shown. Do not change your set.

---

## Section 1 — Simple DataFrames & Basic Joins

In this section, you will create two small security tables and learn how Spark performs different types of joins. Joins are one of the most important operations in data engineering and security analytics, because real-world data is almost never stored in a single table. A security analyst often has to combine **log data** (what happened) with **reference data** (who or what it belongs to).

### What is a Join?

A join combines two DataFrames based on a shared column called the **join key**. Spark matches rows where the key has the same value in both tables.

For example:

- `df_employees` (the company's user directory) has a `user_id` column
- `df_logins` (failed login counts taken from the authentication logs) also has a `user_id` column

If both tables contain a row with `user_id = 2`, Spark can join those rows so you get a single combined row containing all columns from both tables.

Joins allow you to:

- Detect missing or unmatched records (for example, an account that is not in the directory)
- Build analytics tables from multiple sources

In PySpark, the join key is written using the `on` parameter:

```python
df_employees.join(df_logins, on="user_id", how="left")
```

The `how` parameter controls the join type (for example `"inner"`, `"left"`, `"right"`, `"left_anti"`).

### Create Sample Data

We will use two tables:

- `df_employees` – the user directory: one row per employee account
- `df_logins` – the number of failed logins per account in the last 24 hours, keyed by `user_id`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

employees = [
    (1, "jsmith", "IT",      "Canada"),
    (2, "jdoe",   "Finance", "United States"),
    (3, "rkumar", "HR",      "India"),
    (4, "msilva", "Sales",   "Brazil")
]

failed_logins = [
    (1, 3),
    (2, 12),
    (5, 48)   # user_id 5 is NOT in the employee directory
]

df_employees = spark.createDataFrame(employees, ["user_id","username","department","country"])
df_logins    = spark.createDataFrame(failed_logins, ["user_id","failed_logins"])

df_employees.show()
df_logins.show()
```

### Inner Join

An **inner join** returns only the rows where the join key exists in **both** DataFrames.

- If a `user_id` appears in both tables → it is included
- If it appears in only one → it is excluded

Think of it as: *"Show me only matching rows."*

The following query shows only the employees who have failed logins in the log data.

```python
df_inner = df_employees.join(df_logins, on="user_id", how="inner")
df_inner.show()
```

You can also give the join condition as a boolean expression:

```python
df_inner2 = df_employees.join(df_logins, df_employees.user_id == df_logins.user_id, "inner")
df_inner2.show()
```

Notice that the second version shows `user_id` twice (one from each table). Using `on="user_id"` keeps only one copy.

### Left Join

A **left join** keeps **all rows from the left DataFrame** (`df_employees`), and adds matching data from the right DataFrame (`df_logins`) when available.

- If the left row has no match → Spark fills the missing values with `null`
- Nothing is removed from the left table

Think of it as: *"Keep every employee; add their failed login count if there is one."*

```python
df_left = df_employees.join(df_logins, on="user_id", how="left")
df_left.show()
```

### Anti Join

An **anti join** returns the rows from the **left** DataFrame that have **no match** in the right DataFrame. In PySpark it is written as `how="left_anti"`. Only the columns of the left DataFrame are returned.

To find the rows of a table that have no match, put **that table on the left**.

This is extremely useful in security for:

- finding accounts in the logs that are not in the user directory (possible rogue or leftover accounts)
- finding devices on the network that are not in the asset inventory
- finding data quality issues and orphan records

Think of it as: *"Show me the records that failed to match."*

### Task 1 — Anti Join (1 mark)

Find the rows in `df_logins` that do **not** have a matching employee in `df_employees`. Put `df_logins` on the left and use `how="left_anti"`. The expected output is one row with `user_id = 5`.

```python
# TODO: Task 1 - login rows with no matching employee
# Your code here

print("Task 1 |", STUDENT_ID)
```

**R1 (1 mark):** Look at your Task 1 output and your left join output. In 2–3 sentences, explain why `user_id = 5` appears in the anti join and why a security analyst should investigate it, and why `rkumar` and `msilva` have `null` for `failed_logins` in the left join.

---

## Section 2 — Create the Country Lookup Table

Real-world data pipelines almost always combine:

1. **Fact data** – large tables with many records over time (e.g., daily cyber attack statistics per country)
2. **Dimension data** – small lookup tables with descriptive attributes (e.g., a country's continent)

Spark joins allow us to merge these two types of data into a single enriched dataset.

In this section, you will create a small lookup table with two columns:

- `country` – the country name, written exactly as in the attack dataset (this is the **join key**)
- `continent` – used later to compare continents

**Note:** one country in your set has been left out of this table on purpose. You will find it in Section 5.

```python
data_countries = [
    ("Argentina",      "South America"),
    ("Australia",      "Oceania"),
    ("Bangladesh",     "Asia"),
    ("Brazil",         "South America"),
    ("Canada",         "North America"),
    ("Egypt",          "Africa"),
    ("Ethiopia",       "Africa"),
    ("France",         "Europe"),
    ("Germany",        "Europe"),
    ("Ghana",          "Africa"),
    ("India",          "Asia"),
    ("Indonesia",      "Asia"),
    ("Italy",          "Europe"),
    ("Japan",          "Asia"),
    ("Kenya",          "Africa"),
    ("Malaysia",       "Asia"),
    ("Mexico",         "North America"),
    ("Nigeria",        "Africa"),
    ("Pakistan",       "Asia"),
    ("Philippines",    "Asia"),
    ("Saudi Arabia",   "Asia"),
    ("South Africa",   "Africa"),
    ("South Korea",    "Asia"),
    ("Spain",          "Europe"),
    ("Thailand",       "Asia"),
    ("Turkey",         "Asia"),
    ("United Kingdom", "Europe"),
    ("United States",  "North America"),
]

df_countries = spark.createDataFrame(data_countries, ["country", "continent"])
df_countries.show(30, truncate=False)
```

---

## Section 3 — Load the Cyber Threat Dataset

In this section, you will load a real cyber security dataset into Spark: **daily cyber threat statistics by country**.

### About the Data

**Where it comes from.** Kaspersky, a security software company, runs a public *Cyberthreat Real-Time Map*. Its antivirus products report every attack they detect on their users' computers, and Kaspersky publishes daily statistics for each country. A researcher, Sufi (2024), collected these daily statistics into one dataset and published it for research (https://github.com/DrSufi/CyberData, MIT licence). Your instructor has prepared a smaller copy, `cyber_threats.csv` (about 3 MB), with 5 of the original 18 columns and everyday country names.

**What one row means.** Each row is **one country on one day**. The data covers October 2022 to December 2023 for 225 countries (77,623 rows in total).

**What the numbers mean.** Each attack column is the **fraction of Kaspersky users in that country who had that type of attack detected on that day**. For example, `web_threat = 0.0129` on a day in Canada means 1.29% of Kaspersky users in Canada had a web threat blocked that day.

| Column | Meaning |
|---|---|
| `date` | the day (year-month-day) |
| `country` | country name |
| `ransomware` | fraction of users where **ransomware** was detected: malware that encrypts files and demands payment. These values are very small. |
| `network_attack` | fraction of users where Kaspersky's **intrusion detection** blocked an attack coming over the network, for example an attempt to exploit a vulnerable service or a brute-force connection |
| `web_threat` | fraction of users where Kaspersky's **web anti-virus** blocked something malicious while browsing or downloading: a malicious web page, script or file |

**Keep in mind:**

- The numbers only describe **Kaspersky users**, not every computer in the country. Countries where few people use Kaspersky can show unusual values.
- These are **detections** (attacks that were found and blocked), not successful attacks.
- Some values are **missing** (empty in the file). You will deal with them in Section 5.

### Upload & Load the CSV

1. Download `cyber_threats.csv` from Blackboard.
2. In Colab, click the **folder icon** on the left, then the **upload** button, and choose the file. It will be saved as `/content/cyber_threats.csv`.

**Note:** files uploaded to Colab are deleted when the runtime restarts. If you get a "file not found" error, upload the file again.

```python
from pyspark.sql import functions as F

df = spark.read.csv("/content/cyber_threats.csv", header=True, inferSchema=True)

# Make sure 'date' is a proper Spark DateType
df = df.withColumn("date", F.to_date("date"))

print("Rows:", df.count())
df.printSchema()
df.show(5, truncate=False)
```

**Reading small numbers:** Spark shows very small numbers in scientific notation. For example, `1.3E-4` means 0.00013 (move the decimal point 4 places to the left).

### Filter to Your Countries

```python
df_attacks = df.filter(F.col("country").isin(MY_COUNTRIES))   # keep only your 5 countries

df_attacks.orderBy("country", "date").show(10, truncate=False)

print("Rows for my countries:", df_attacks.count())
```

### Task 2 — Verify Data Types (1 mark)

Print **only** the data types of the columns `country`, `date`, and `web_threat` from `df_attacks`.

Hint: `df_attacks.select(...).dtypes` or `df_attacks.select(...).printSchema()`. You should see a string type for `country`, a date type for `date`, and `double` for `web_threat`.

```python
# TODO: Task 2 - data types of country, date, web_threat
# Your code here

print("Task 2 |", STUDENT_ID)
```

### Task 3 — Latest 10 Records for Your First Country (1 mark)

Your first country is `MY_COUNTRIES[0]`. Show its **latest 10 records** (all 5 columns), ordered by `date` **descending** (most recent first). Use `.show(10, truncate=False)`.

```python
# TODO: Task 3 - latest 10 records for MY_COUNTRIES[0], newest first
# Your code here

print("Task 3 |", STUDENT_ID)
```

---

## Section 4 — Join the Attack Data with the Country Lookup Table

In this section, you will join the fact data (`df_attacks`, daily attack statistics) with the dimension table (`df_countries`). Both tables have a column called `country`, so we join on it with `on="country"`, exactly like the `user_id` joins in Section 1.

### Left Join on `country`

We use a **left join** because we want to keep **all** attack rows, even if a country is missing from the lookup table.

```python
df_joined = df_attacks.join(df_countries, on="country", how="left")

df_joined.orderBy("country", "date").show(10, truncate=False)
print("Rows after join:", df_joined.count())
```

What this join does:

- Keeps every row from `df_attacks`
- Adds `continent` from `df_countries`
- If a country is missing in `df_countries`, Spark fills `continent` with `null`
- Keeps the same number of rows as `df_attacks`

### Task 4 — Add a Percentage Column (1 mark)

The attack values are fractions (0.0129), which are hard to read. In `df_joined`, create a new column `web_threat_pct` that shows the web threat value as a **percentage**:

```
web_threat_pct = web_threat * 100
```

Use `withColumn`. Then show the **10 rows with the highest `web_threat_pct`**: order by `web_threat_pct` descending and use `.show(10, truncate=False)`. Show the columns `country`, `continent`, `date`, `web_threat_pct`.

If some rows show `null` for `continent`, that is expected. You will find out why in Section 5.

```python
# TODO: Task 4 - add web_threat_pct to df_joined, then show the top 10 rows
# Your code here

print("Task 4 |", STUDENT_ID)
```

---

## Section 5 — Data Cleaning (Missing Values, Duplicates, Sanity Checks)

In this section, you will inspect, clean, and check the joined dataset from Section 4. You will:

- Find missing values (`continent`, `ransomware`, `network_attack`, `web_threat`)
- Drop rows for countries that are not in the lookup table (missing `continent`)
- Fill missing attack values with 0
- Remove duplicate rows using the key (`country`, `date`)
- Compare row counts before and after cleaning

**How missing values look in this file:** a missing value is an empty field in the CSV (`,,`), which Spark reads as `null`. A blank can mean the data was not collected that day, or that the value was too small to report. Filling with 0 is reasonable for the second case but not really for the first. We fill with 0 here to keep the lab simple. In real work, always choose a strategy that matches what the data means.

### 1. Inspect Missing Values

Each `isNull()` gives true/false. Casting it to `int` turns true into 1, so the sum is the number of nulls.

```python
df_joined.select(
    F.sum(F.col("continent").isNull().cast("int")).alias("missing_continent"),
    F.sum(F.col("ransomware").isNull().cast("int")).alias("missing_ransomware"),
    F.sum(F.col("network_attack").isNull().cast("int")).alias("missing_network_attack"),
    F.sum(F.col("web_threat").isNull().cast("int")).alias("missing_web_threat")
).show()

# Which country is not in the lookup table?
df_joined.filter(F.col("continent").isNull()) \
         .groupBy("country").count().show(truncate=False)
```

### 2. Keep the Row Count Before Cleaning

```python
count_before_clean = df_joined.count()
print("Rows BEFORE cleaning:", count_before_clean)
```

### 3. Perform the Cleaning Steps

```python
# 3.1 Drop rows whose country is not in the lookup table (no continent)
df_clean = df_joined.dropna(subset=["continent"])

# 3.2 Fill missing attack values with 0
df_clean = df_clean.fillna({"ransomware": 0, "network_attack": 0, "web_threat": 0})

# 3.3 Remove duplicate records by (country, date)
df_clean = df_clean.dropDuplicates(["country", "date"])

df_clean.orderBy("country", "date").show(10, truncate=False)
```

### Task 5 — Compare Row Counts (1 mark)

Print the row count **before** cleaning, the row count **after** cleaning, and the **number of rows dropped**.

```python
# TODO: Task 5 - print before, after, and dropped
# Your code here

print("Task 5 |", STUDENT_ID)
```

### 4. Sanity Check: Are There Any Nulls Left?

```python
df_clean.select(
    F.sum(F.col("continent").isNull().cast("int")).alias("remaining_null_continent"),
    F.sum(F.col("ransomware").isNull().cast("int")).alias("remaining_null_ransomware"),
    F.sum(F.col("network_attack").isNull().cast("int")).alias("remaining_null_network_attack"),
    F.sum(F.col("web_threat").isNull().cast("int")).alias("remaining_null_web_threat")
).show()
```

All four numbers should be 0.

### 5. Recompute the Percentage Column on the Clean Data

`web_threat_pct` was calculated before we filled the nulls, so we calculate it again.

```python
df_clean = df_clean.withColumn("web_threat_pct", F.col("web_threat") * 100)

df_clean.orderBy("country", "date").show(10, truncate=False)
```

**R2 (1 mark):** Using your own output: which of your countries was dropped during cleaning, how many rows were dropped, and why could those rows not be used for the continent comparison in Section 6? (2–3 sentences)

---

## Section 6 — Spark SQL Joins and Aggregations

In this section, you will do the same kind of work as before (joins and summaries), but this time using **Spark SQL** instead of the DataFrame API.

**Why do this again in SQL?** Spark gives you two ways to ask the same question:

- the **DataFrame API** (Python code such as `df.join(...)`, `df.groupBy(...)`), which you used in Sections 1–5, and
- **SQL** (queries such as `SELECT ... FROM ... JOIN ... GROUP BY ...`).

Both run on the same Spark engine and give the same results. SQL is worth knowing because many analysts and security tools use it: SIEM search languages, Hive, Databricks and most databases are SQL or SQL-like. In your job you will often write SQL queries over large log tables.

What you will do, step by step:

1. **Create temp views** – give your DataFrames table names so SQL can use them.
2. **LEFT JOIN in SQL** – repeat the employees/failed-logins left join from Section 1, so you can compare the SQL version with the DataFrame version.
3. **Aggregate in SQL** – summarize your cleaned attack data per country (average and maximum).
4. **Join + aggregate in one query** – join the raw attack data to the country lookup table and summarize per continent, all in a single SQL statement.

### 1. Create Temp Views

A temporary view gives a DataFrame a table name that you can use inside `spark.sql("...")`.

```python
df_employees.createOrReplaceTempView("employees")
df_logins.createOrReplaceTempView("logins")
df_clean.createOrReplaceTempView("attacks_clean")
df_attacks.createOrReplaceTempView("attacks_raw")
df_countries.createOrReplaceTempView("countries")
```

### 2. LEFT JOIN in SQL

List every employee, with their failed login count if there is one.

```python
spark.sql("""
SELECT
  e.user_id,
  e.username,
  e.department,
  l.failed_logins
FROM employees e
LEFT JOIN logins l
  ON e.user_id = l.user_id
ORDER BY l.failed_logins DESC
""").show()
```

You should see all four employees. Employees with no failed logins show `NULL`, and they appear last. This is the same result as the DataFrame left join in Section 1.

### 3. SQL Aggregation over `attacks_clean`

For each of your countries, show the continent, the **average** daily web threat percentage, and the **highest** daily web threat percentage.

```python
spark.sql("""
SELECT
  country,
  continent,
  ROUND(AVG(web_threat_pct), 2) AS avg_web_threat_pct,
  ROUND(MAX(web_threat_pct), 2) AS max_web_threat_pct
FROM attacks_clean
GROUP BY country, continent
ORDER BY avg_web_threat_pct DESC
""").show(truncate=False)
```

- `AVG` and `MAX` are computed per country.
- `continent` is in the `GROUP BY` because every column in `SELECT` must either be grouped or be inside an aggregate function.

### 4. Join + Aggregate in One SQL Query

Starting from the raw attack data (`attacks_raw`), join to the `countries` lookup table and compute the average ransomware, network attack and web threat percentages **per continent**.

```python
spark.sql("""
SELECT
  c.continent,
  COUNT(*)                                     AS days,
  ROUND(AVG(a.ransomware)     * 100, 4)        AS avg_ransomware_pct,
  ROUND(AVG(a.network_attack) * 100, 2)        AS avg_network_attack_pct,
  ROUND(AVG(a.web_threat)     * 100, 2)        AS avg_web_threat_pct
FROM attacks_raw a
LEFT JOIN countries c
  ON a.country = c.country
WHERE c.continent IS NOT NULL
GROUP BY c.continent
ORDER BY avg_web_threat_pct DESC
""").show(truncate=False)
```

Note: `AVG` in SQL simply skips `NULL` values, so this query works on the raw data without filling them first.

**R3 (1 mark):** Using your two SQL results: which of your countries has the highest `avg_web_threat_pct`, and which continent is highest in the per-continent query? Look at the `days` column: why can a continent average be misleading when it is based on only one or two countries? (2–3 sentences)

---

## Section 7 — Writing Data: CSV vs Parquet (and Comparing Sizes)

In this section, you will save your cleaned dataset to disk in two common formats and compare their sizes. This mirrors a standard step in data pipelines where results are written to a data lake for later use.

- **CSV** is human-readable but large and has no schema (no column types).
- **Parquet** is a columnar, compressed format that stores the schema. Spark is optimized for it.

### 1. Write Cleaned Data to CSV and Parquet

```python
df_clean.write.mode("overwrite").option("header", True).csv("/content/lab6_output_csv")
df_clean.write.mode("overwrite").parquet("/content/lab6_output_parquet")
```

- Spark writes **folders**, not single files, because the data may be split across tasks.
- `mode("overwrite")` replaces older output if you run the cell again.

### 2. Inspect the Folders

```python
print("CSV folder:")
!ls -la /content/lab6_output_csv

print("\nParquet folder:")
!ls -la /content/lab6_output_parquet
```

The CSV folder contains `part-...csv` text files. The Parquet folder contains `part-...parquet` binary files. Both also contain a `_SUCCESS` marker file.

### 3. Compare Sizes with `du -sh`

```python
print("CSV size:")
!du -sh /content/lab6_output_csv

print("\nParquet size:")
!du -sh /content/lab6_output_parquet
```

### Task 6 — Read Back the Parquet and Confirm the Schema (1 mark)

Read `/content/lab6_output_parquet` into a new DataFrame. Print its schema, show 5 rows, and print its row count. The row count should equal your "after cleaning" count from Task 5.

```python
# TODO: Task 6 - read the Parquet folder, printSchema(), show(5), count()
# Your code here

print("Task 6 |", STUDENT_ID)
```

**R4 (1 mark):** Write your CSV size and your Parquet size from the `du -sh` output. Which is smaller, and give two reasons why Parquet is preferred for analytics. (2–3 sentences)

---

## Conclusion

In this lab, you worked through a small data‑engineering workflow on real cyber security data. You enriched a fact dataset (daily attack statistics per country) with a dimension table (country lookup), cleaned the result, created a derived percentage column, compared countries and continents, and wrote the final dataset in two formats. You also wrote the same joins in both the DataFrame API and Spark SQL.

These steps are the core of an ETL pipeline:

1. **Extract** data from source systems
2. **Transform** it through cleaning, validation, and enrichment
3. **Load** it into optimized storage formats for later analytics

---

## Submission Guidelines

1. **Run the whole notebook from top to bottom** (Runtime → Run all) so every output is visible. The Section 0 cell must be the first cell.
2. **Write the reflection answers (R1–R4) in a text cell** directly under the task they belong to. Answer in your own words, 2–3 sentences each, and refer to **your own numbers**. Delete any template or AI-generated text.
3. **Print the notebook to PDF:** File → Print → Save as PDF. Check that the outputs are readable and not cut off.
4. **Submit the PDF** on Blackboard, named `Lab6_FirstName_LastName.pdf`.

Deductions:

- A missing task output or missing reflection answer receives 0 for that item.
- Missing Section 0 cell, or numbers that do not match your student ID's set: 0 for the lab.
