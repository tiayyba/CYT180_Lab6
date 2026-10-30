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

---

## Prerequisites

- You will work in Google Colab.
- You should be able to start a SparkSession (from Lab 5).

**Tip:** If Spark is not yet installed in your Colab, run the same install steps from Lab 5 (Java 11, Spark 3.5.x, findspark) and then create the `SparkSession`.

---

## Grading (10 marks)

| Item | Marks |
|---|---|
| Task 1 – Anti join | 1 |
| Task 2 – Data types | 1 |
| Task 3 – Latest 10 records for your first country | 1 |
| Task 4 – `cases_per_million` | 1 |
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
    0: ["Canada",         "Germany",        "Nigeria",     "Thailand",     "Peru"],
    1: ["India",          "France",         "Kenya",       "South Africa", "Ukraine"],
    2: ["United States",  "Japan",          "Egypt",       "Spain",        "Vietnam"],
    3: ["Brazil",         "United Kingdom", "Pakistan",    "Australia",    "Iran"],
    4: ["Italy",          "Mexico",         "Ghana",       "Philippines",  "Poland"],
    5: ["Germany",        "Argentina",      "Bangladesh",  "Turkey",       "Morocco"],
    6: ["Japan",          "Canada",         "Ethiopia",    "Saudi Arabia", "Colombia"],
    7: ["France",         "India",          "Indonesia",   "South Korea",  "Sri Lanka"],
    8: ["Argentina",      "Italy",          "Nigeria",     "Malaysia",     "Nepal"],
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

In this section, you will create two small DataFrames and learn how Spark performs different types of joins. Joins are one of the most important operations in data engineering because real-world datasets are almost never stored in a single table.

### What is a Join?

A join combines two DataFrames based on a shared column called the **join key**. Spark matches rows where the key has the same value in both tables.

For example:

- `df_people` has a `person_id` column
- `df_salary` also has a `person_id` column

If both tables contain a row with `person_id = 2`, Spark can join those rows so you get a single combined row containing all columns from both tables.

Joins allow you to:

- Detect missing or unmatched records
- Build analytics tables from multiple sources

In PySpark, the join key is written using the `on` parameter:

```python
df_people.join(df_salary, on="person_id", how="left")
```

The `how` parameter controls the join type (for example `"inner"`, `"left"`, `"right"`, `"left_anti"`).

### Create Sample Data

We will use two tables:

- `df_people` – information about individuals
- `df_salary` – salary records keyed by `person_id`

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

people = [
    (1, "John", "Smith", "Canada"),
    (2, "Jane", "Doe", "United States"),
    (3, "Ravi", "Kumar", "India"),
    (4, "Maria", "Silva", "Brazil")
]

salaries = [
    (1, 100000),
    (2, 150000),
    (5, 90000)   # no matching person_id in people
]

df_people = spark.createDataFrame(people, ["person_id","first","last","country"])
df_salary = spark.createDataFrame(salaries, ["person_id","salary"])

df_people.show()
df_salary.show()
```

### Inner Join

An **inner join** returns only the rows where the join key exists in **both** DataFrames.

- If a `person_id` appears in both tables → it is included
- If it appears in only one → it is excluded

Think of it as: *"Show me only matching rows."*

The following query shows only the people who have a matching salary record.

```python
df_inner = df_people.join(df_salary, on="person_id", how="inner")
df_inner.show()
```

You can also give the join condition as a boolean expression:

```python
df_inner2 = df_people.join(df_salary, df_people.person_id == df_salary.person_id, "inner")
df_inner2.show()
```

Notice that the second version shows `person_id` twice (one from each table). Using `on="person_id"` keeps only one copy.

### Left Join

A **left join** keeps **all rows from the left DataFrame** (`df_people`), and adds matching data from the right DataFrame (`df_salary`) when available.

- If the left row has no match → Spark fills the missing values with `null`
- Nothing is removed from the left table

Think of it as: *"Keep everyone in the people table; add salary data if available."*

```python
df_left = df_people.join(df_salary, on="person_id", how="left")
df_left.show()
```

### Anti Join

An **anti join** returns the rows from the **left** DataFrame that have **no match** in the right DataFrame. In PySpark it is written as `how="left_anti"`. Only the columns of the left DataFrame are returned.

To find the rows of a table that have no match, put **that table on the left**.

This is extremely useful for:

- finding data quality issues
- detecting orphan records
- identifying missing relationships

Think of it as: *"Show me the records that failed to match."*

### Task 1 — Anti Join (1 mark)

Find the rows in `df_salary` that do **not** have a matching person in `df_people`. Put `df_salary` on the left and use `how="left_anti"`. The expected output is one row with `person_id = 5`.

```python
# TODO: Task 1 - salary rows with no matching person
# Your code here

print("Task 1 |", STUDENT_ID)
```

**R1 (1 mark):** Look at your Task 1 output and your left join output. In 2–3 sentences, explain why `person_id = 5` appears in the anti join, and why Ravi and Maria have `null` salary in the left join.

---

## Section 2 — Create the Country Population Table

Real-world data pipelines almost always combine:

1. **Fact data** – large tables with daily records (e.g., COVID cases per day)
2. **Dimension data** – small tables with descriptive attributes (e.g., population, continent)

Spark joins allow us to merge these two types of data into a single enriched dataset.

In this section, you will create a small lookup table of population and continent for many countries. Later you will join it with COVID‑19 data to compute `cases_per_million`.

**Note:** one country in your set has been left out of this table on purpose. You will find it in Section 5.

```python
data_pop = [
    ("Argentina",         46_000_000,  "South America"),
    ("Australia",         26_000_000,  "Oceania"),
    ("Bangladesh",       171_000_000,  "Asia"),
    ("Brazil",           215_000_000,  "South America"),
    ("Canada",            38_000_000,  "North America"),
    ("Egypt",            111_000_000,  "Africa"),
    ("Ethiopia",         123_000_000,  "Africa"),
    ("France",            68_000_000,  "Europe"),
    ("Germany",           83_000_000,  "Europe"),
    ("Ghana",             33_000_000,  "Africa"),
    ("India",          1_417_000_000,  "Asia"),
    ("Indonesia",        276_000_000,  "Asia"),
    ("Italy",             59_000_000,  "Europe"),
    ("Japan",            124_000_000,  "Asia"),
    ("Kenya",             54_000_000,  "Africa"),
    ("Malaysia",          34_000_000,  "Asia"),
    ("Mexico",           128_000_000,  "North America"),
    ("Nigeria",          219_000_000,  "Africa"),
    ("Pakistan",         236_000_000,  "Asia"),
    ("Philippines",      116_000_000,  "Asia"),
    ("Saudi Arabia",      36_000_000,  "Asia"),
    ("South Africa",      60_000_000,  "Africa"),
    ("South Korea",       52_000_000,  "Asia"),
    ("Spain",             48_000_000,  "Europe"),
    ("Thailand",          72_000_000,  "Asia"),
    ("Turkey",            85_000_000,  "Asia"),
    ("United Kingdom",    68_000_000,  "Europe"),
    ("United States",    338_000_000,  "North America"),
]

df_pop = spark.createDataFrame(data_pop, ["location","population","continent"])
df_pop.show(30, truncate=False)
```

---

## Section 3 — Load the COVID Dataset (Same Source as Lab 5)

In this section, you will download and load the Our World in Data (OWID) COVID‑19 dataset into a Spark DataFrame and make sure the `date` column has the proper Spark `date` type. We will then filter to **your five countries** so the next steps are fast and easy to inspect in Colab.

This is important because:

- Large "fact" datasets like daily COVID records are common in analytics.
- Proper data types (e.g., `date`) are important for ordering and time‑based calculations.
- Filtering to a small, focused subset is a common step to speed up development.

### Download & Load the CSV

The cell below downloads the CSV into `/content` (Colab's working directory) if it is not already there, and loads it with `inferSchema=True` so Spark can detect numeric types. We then cast the `date` string into a proper Spark `date` type.

```python
import requests, pathlib
from pyspark.sql import functions as F

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
local_path = pathlib.Path("/content/owid-covid-data.csv")

# Download the file if it doesn't exist yet
if not local_path.exists():
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    local_path.write_bytes(r.content)

# Load with header + schema inference
df = spark.read.csv(str(local_path), header=True, inferSchema=True)

# Convert 'date' to a proper Spark DateType
df = df.withColumn("date", F.to_date("date"))

print("Row count (may take a few seconds):", df.count())
df.printSchema()
```

### Filter to Your Countries

```python
df_covid = df.filter(F.col("location").isin(MY_COUNTRIES))   # keep only your 5 countries

# Preview, ordered by location and date so the output is easy to read
df_covid.select("location", "date", "new_cases", "new_deaths") \
        .orderBy("location", "date") \
        .show(10, truncate=False)

print("Rows for my countries:", df_covid.count())
```

### Task 2 — Verify Data Types (1 mark)

Print **only** the data types of the columns `location`, `date`, and `new_cases` from `df_covid`.

Hint: `df_covid.select(...).dtypes` or `df_covid.select(...).printSchema()`. You should see a string type for `location`, a date type for `date`, and a numeric type (`int` or `double`) for `new_cases`.

```python
# TODO: Task 2 - data types of location, date, new_cases
# Your code here

print("Task 2 |", STUDENT_ID)
```

### Task 3 — Latest 10 Records for Your First Country (1 mark)

Your first country is `MY_COUNTRIES[0]`. Show its **latest 10 records** with the columns `location`, `date`, `new_cases`, `new_deaths`, ordered by `date` **descending** (most recent first). Use `.show(10, truncate=False)`.

Some of the most recent rows may show `NULL` (no value reported that day). That is expected. You will handle missing values in Section 5.

```python
# TODO: Task 3 - latest 10 records for MY_COUNTRIES[0], newest first
# Your code here

print("Task 3 |", STUDENT_ID)
```

---

## Section 4 — Join COVID Data with Population Data

In this section, you will join a large fact dataset (`df_covid`, daily COVID case counts) with a small dimension table (`df_pop`, population and continent). This is one of the most common operations in data engineering. We join on the column `location`, which both tables have. The purpose is to add population to every COVID row so we can compute per-capita metrics such as `cases_per_million`.

### Left Join on `location`

We use a **left join** because we want to keep all rows from the COVID dataset, even if a country is missing from the population table. Both datasets have `location`, `population` and `continent` columns, so we give each DataFrame a short alias (`c` and `p`) and say exactly which table each column comes from.

```python
c = df_covid.alias("c")   # COVID data
p = df_pop.alias("p")     # population table

df_joined = c.join(p, on=F.col("c.location") == F.col("p.location"), how="left")

df_joined = df_joined.select(
    F.col("c.location").alias("location"),
    F.col("c.date").alias("date"),
    F.col("c.new_cases").alias("new_cases"),
    F.col("c.new_deaths").alias("new_deaths"),
    F.col("p.population").alias("population"),
    F.col("p.continent").alias("continent")
)

df_joined.orderBy("location", "date").show(10, truncate=False)
print("Rows after join:", df_joined.count())
```

What this join does:

- Keeps every row from `df_covid`
- Adds `population` and `continent` from `df_pop`
- If a country is missing in `df_pop`, Spark fills `population` and `continent` with `null`
- Keeps the same number of rows as `df_covid`

### Task 4 — Compute `cases_per_million` (1 mark)

In `df_joined`, create a new column called `cases_per_million` using this formula:

```
cases_per_million = new_cases * 1,000,000 / population
```

This metric lets you compare countries fairly despite different population sizes. Use `withColumn` to create it.

**Divide first, then multiply** (`new_cases / population * 1_000_000`). `new_cases` is a whole-number (`int`) column, and an `int` can only hold numbers up to about 2.1 billion. If you multiply first, large days such as 238,269 × 1,000,000 are too big for an `int` and Spark silently gives a wrong answer. Dividing first turns the value into a decimal (`double`), which avoids the problem.

Then show the **10 rows with the largest `cases_per_million`**: order by `cases_per_million` descending and use `.show(10, truncate=False)`. Show the columns `location`, `date`, `new_cases`, `population`, `cases_per_million`.

```python
# TODO: Task 4 - add cases_per_million to df_joined, then show the top 10 rows
# Hint: F.col("new_cases") / F.col("population") * 1_000_000
# Your code here

print("Task 4 |", STUDENT_ID)
```

---

## Section 5 — Data Cleaning (Missing Values, Duplicates, Sanity Checks)

In this section, you will inspect, clean, and check the joined dataset from Section 4. You will:

- Find missing values (`population`, `new_cases`, `new_deaths`)
- Drop rows that cannot support per‑capita metrics (missing `population`)
- Fill `new_cases` and `new_deaths` with 0
- Remove duplicate rows using the key (`location`, `date`)
- Compare row counts before and after cleaning

**Context:** In production, "filling with zero" is not always correct. We do it here to keep the lab simple. Always choose a strategy that matches what the data means.

### 1. Inspect Missing Values

Each `isNull()` gives true/false. Casting it to `int` turns true into 1, so the sum is the number of nulls.

```python
df_joined.select(
    F.sum(F.col("population").isNull().cast("int")).alias("missing_population"),
    F.sum(F.col("new_cases").isNull().cast("int")).alias("missing_new_cases"),
    F.sum(F.col("new_deaths").isNull().cast("int")).alias("missing_new_deaths")
).show()

# Which country has no population?
df_joined.filter(F.col("population").isNull()) \
         .groupBy("location").count().show()
```

### 2. Keep the Row Count Before Cleaning

```python
count_before_clean = df_joined.count()
print("Rows BEFORE cleaning:", count_before_clean)
```

### 3. Perform the Cleaning Steps

```python
# 3.1 Drop rows with missing population (per-capita metrics need a denominator)
df_clean = df_joined.dropna(subset=["population"])

# 3.2 Fill missing new_cases and new_deaths with 0
df_clean = df_clean.fillna({"new_cases": 0, "new_deaths": 0})

# 3.3 Remove duplicate records by (location, date)
df_clean = df_clean.dropDuplicates(["location", "date"])

df_clean.orderBy("location", "date").show(10, truncate=False)
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
    F.sum(F.col("population").isNull().cast("int")).alias("remaining_null_population"),
    F.sum(F.col("new_cases").isNull().cast("int")).alias("remaining_null_new_cases"),
    F.sum(F.col("new_deaths").isNull().cast("int")).alias("remaining_null_new_deaths")
).show()
```

All three numbers should be 0.

### 5. Recompute the Per‑Capita Metric on the Clean Data

```python
df_clean = df_clean.withColumn(
    "cases_per_million",
    F.col("new_cases") / F.col("population") * 1_000_000
)

df_clean.orderBy("location", "date").show(10, truncate=False)
```

**R2 (1 mark):** Using your own output: which of your countries was dropped during cleaning, how many rows were dropped, and why could those rows not be used to compute `cases_per_million`? (2–3 sentences)

---

## Section 6 — Spark SQL Joins and Aggregations

In this section, you will practice joining and aggregating with Spark SQL. SQL gives you a declarative way to write the same logic you wrote with the DataFrame API.

### 1. Create Temp Views

A temporary view gives a DataFrame a table name that you can use inside `spark.sql("...")`.

```python
df_people.createOrReplaceTempView("people")
df_salary.createOrReplaceTempView("salary")
df_clean.createOrReplaceTempView("covid_clean")
df_covid.createOrReplaceTempView("covid_raw")
df_pop.createOrReplaceTempView("pop")
```

### 2. LEFT JOIN in SQL

List everyone from the `people` table, with their salary if it exists.

```python
spark.sql("""
SELECT
  p.person_id,
  p.first,
  p.last,
  p.country,
  s.salary
FROM people p
LEFT JOIN salary s
  ON p.person_id = s.person_id
ORDER BY s.salary DESC
""").show()
```

You should see all rows from `people`. People without a salary show `NULL`, and they appear last.

### 3. SQL Aggregation over `covid_clean`

For each `location`, show the population, continent, average daily `new_cases` and maximum daily `new_cases`.

```python
spark.sql("""
SELECT
  location,
  population,
  continent,
  ROUND(AVG(new_cases), 2) AS avg_new_cases,
  MAX(new_cases) AS max_new_cases
FROM covid_clean
GROUP BY location, population, continent
ORDER BY avg_new_cases DESC
""").show(truncate=False)
```

- `AVG(new_cases)` and `MAX(new_cases)` are computed per `location`.
- `population` and `continent` are in the `GROUP BY` because every column in `SELECT` must either be grouped or be inside an aggregate function.

### 4. Join + Aggregate in One SQL Query

Starting from the raw COVID data (`covid_raw`), join to `pop` and compute the average cases per million for each country.

```python
spark.sql("""
SELECT
  c.location,
  p.continent,
  p.population,
  ROUND(AVG(c.new_cases / p.population * 1000000), 2) AS avg_cases_per_million
FROM covid_raw c
LEFT JOIN pop p
  ON c.location = p.location
WHERE p.population IS NOT NULL
GROUP BY c.location, p.continent, p.population
ORDER BY avg_cases_per_million DESC
""").show(truncate=False)
```

**R3 (1 mark):** Compare your two SQL results. Which of your countries has the highest `avg_new_cases`, and which has the highest `avg_cases_per_million`? If they are different countries, explain why. If they are the same, explain what `avg_cases_per_million` adds. (2–3 sentences)

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

In this lab, you worked through a small data‑engineering workflow. You enriched a fact dataset (daily COVID‑19 records) with a dimension table (country population), cleaned the result, computed a per‑capita metric, and wrote the final dataset in two formats. You also wrote the same joins in both the DataFrame API and Spark SQL.

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
