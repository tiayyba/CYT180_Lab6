# CYT180 — Lab 6: Joins, Data Cleaning, and Writing Data with PySpark

**Weight:** 3%  
**Work Type:** Individual  
**Submission Format:** Screenshots + short written answers  

---

##  Introduction

In Lab 5, you installed Spark in Google Colab, created DataFrames, performed common transformations, ran basic Spark SQL, and computed a 7‑day moving average. That workflow focused on working with **one dataset at a time**.

In real Big Data engineering, a typical workflow involves **combining multiple datasets**, cleaning and standardizing them, computing meaningful metrics, and writing the results to efficient formats. These steps form the foundation of a simple ETL (Extract–Transform–Load) workflow.
In this lab, you will practice three essential skills:

1. Joining multiple DataFrames using the **DataFrame API** and **Spark SQL**
2. Cleaning: handling missing values and duplicate rows
3. **Writing results** to disk using CSV and Parquet formats

You will mostly **run provided code** to observe Spark’s behavior and complete a few **simple tasks** in each section.

---

##  Learning Objectives

By the end of this lab, you will be able to:

- Perform inner, left, and anti joins in PySpark.
- Detect and handle missing values using `dropna()`, `fillna()`, and conditional logic.
- Remove duplicate rows using `dropDuplicates()`.
- Write DataFrames to disk using CSV and Parquet formats.
- Compare file sizes and explain why Parquet is preferred in big‑data systems.
- Explain how cleaning and joining datasets fit into a typical ETL (Extract‑Transform‑Load) workflow.

---

##  Prerequisites

- You will work in **Google Colab**.
- You should be able to initialize a **SparkSession** (from Lab 6).

**Tip**: If Spark is not yet initialized in your Colab, run the same install steps from Lab 6 (Java 11, Spark 3.5.x, findspark) and then create the `SparkSession`.

----

## Section 1 — Simple DataFrames & Basic Joins

In this section, you will create two small DataFrames and learn how Spark performs different types of joins.
Joins are one of the most important operations in data engineering because real-world datasets are almost never stored in a single table.

### What is a Join?

A **join** combines two DataFrames based on a shared column called the **join key**. Spark matches rows where the key has the same value in both tables.

For example:

- `df_people` has a `person_id` column  
- `df_salary` also has a `person_id` column  

If both tables contain a row with `person_id = 2`, Spark can **join** those rows so you get a single combined row containing all columns from both tables.

Joins allow you to:

- Detect missing or unmatched records  
- Build analytics tables from multiple sources  

In PySpark, the join condition is written using the `on` parameter:

```python
df_people.join(df_salary, on="person_id", how="left")
```

The `how` parameter controls the join type (e.g., `"inner"`, `"left"`, `"right"`, `"anti"`).

Understanding this basic concept will help you interpret the different join types introduced shortly.

### Create Sample Data
We will use two tables:

- `df_people` – information about individuals  
- `df_salary` – salary records keyed by `person_id`

Let's create these tables.
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
An **inner join** returns only the rows where the join key exists *in both* DataFrames.

- If a `person_id` appears in both tables → it is included  
- If it appears in only one → it is excluded

Think of it as: **"Show me only matching rows."**

In PySpark, you specify the join condition using the `on=` parameter in `DataFrame.join()`.

The following query will show only the people who have a matching salary record. It excludes anyone without a salary and excludes any salary entries that don’t have a matching person.

```python
df_inner = df_people.join(df_salary, on="person_id", how="inner")
df_inner.show()
```

You can also provide a boolean expression:  
```python
df_inner = df_people.join(df_salary, df_people.person_id == df_salary.person_id, "inner")
```
In both cases, the **join key** is defined **with `on`** (either as a column name or a boolean expression). The `how` argument controls the join type: `"inner"` etc.


###  Left Join

A **left join** keeps *all* rows from the left DataFrame (`df_people`), and adds matching data from the right DataFrame (`df_salary`) when available.

- If the left row has no match → Spark fills missing values with `null`
- Nothing is removed from the left table

Think of it as:  **"Keep everyone in the people table; add salary data if available."**

Let's write a query to: "List everyone from the people table, and include salary if available. If a person has no salary record, show null for salary."

```python
df_left = df_people.join(df_salary, on="person_id", how="left")
df_left.show()
```


###  Anti Join

An **anti join** returns rows from one DataFrame where **no match exists** in the other.

Specifically: a **right anti join** shows rows in the right table (`df_salary`)  that *do not* have a matching `person_id` in the left table (`df_people`)
This is extremely useful for:

- finding data quality issues  
- detecting orphan records  
- identifying missing relationships  

Think of it as:  **"Show me the records that failed to match."**


### Task 1 — Anti Join
Perform a right anti join on df_salary to find rows in the salary table that do not have a matching person in `df_people`.
Expected output includes `person_id = 5`.
Add your code below and include a screenshot of the result.

```python
# TODO: Right anti join on df_salary to find salaries with no matching people
# Your code here
```
### Reflection Questions
- In 2-4 sentences, explain why `person_id = 5` appears in the anti join result.

### Screenshots to capture for Section 1
- Inner join output
- Left join output
- **Task 1:** Right anti join code + output (must show `person_id = 5`)
- Answer to the reflection question

----

## Section 2 — Load a Country Metadata Dataset

Real-world data pipelines almost always combine:

1. **Fact data** – large tables with daily records (e.g., COVID cases per day)  
2. **Dimension data** – small tables with descriptive attributes (e.g., population, region, categories)

Spark joins allow us to merge these two types of data into a single enriched dataset.

In this section, you will create a small lookup table containing population and continent information for the five countries used earlier.  
You will later join this with COVID‑19 data to compute per‑capita metrics such as `cases_per_million`.

### Create Population/Continent Data

```python
data_pop = [
    ("Canada", 38000000, "North America"),
    ("United States", 331000000, "North America"),
    ("India", 1380000000, "Asia"),
    ("Brazil", 212000000, "South America"),
    ("Italy", 60000000, "Europe")
]

df_pop = spark.createDataFrame(data_pop, ["location","population","continent"])
df_pop.show()
```
----

## Section 3 — Load COVID Dataset (Same Source as Lab 6)

In this section, you will **download and load** the Our World in Data (OWID) COVID‑19 dataset into a Spark DataFrame and prepare it for joining by ensuring the `date` column has the proper Spark `date` type.  
We will then **filter to five focus countries** so downstream tasks are fast and easy to inspect in Colab.

This is important becasue:
- Large “fact” datasets like daily COVID records are common in analytics.
- Proper data types (e.g., `date`) are important for ordering, window functions, and time‑based joins/aggregations.
- Filtering to a small, focused subset is a common step to accelerate iteration in development.

### Download & Load the CSV

The cell below downloads the latest CSV into `/content` (Colab’s working directory) if it’s not already present, and loads it with `inferSchema=True` so Spark can detect numeric types.  
We then cast the `date` string into a proper Spark `date` type so that time operations will work as expected.

```python
import requests, pathlib
from pyspark.sql import functions as F

url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
local_path = pathlib.Path("/content/owid-covid-data.csv")

# Download the file if it doesn't exist yet
if not local_path.exists():
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    local_path.write_bytes(r.content)

# Load with header + schema inference
df = spark.read.csv(str(local_path), header=True, inferSchema=True)

# Convert 'date' to a proper Spark DateType
df = df.withColumn("date", F.to_date("date"))

print("Row count (may take a few seconds):", df.count())
df.printSchema()
```

### Filter to a Focus Subset

We will keep the same five countries used elsewhere in the lab to keep outputs readable and consistent.

```python
countries = ["Canada", "United States", "India", "Brazil", "Italy"]
df_covid = df.filter(F.col("location").isin(countries)) # Uses `.isin(...)` to filter rows for our five countries.

# Filtered preview
# Orders by `location, date` to make the output human-friendly.
df_covid.select("location", "date", "new_cases", "new_deaths") \
        .orderBy("date") \
        .show(10, truncate=False)
```

### Task 2 — Verify Data Types 

Print just the data types of the columns `location`, `date`, and `new_cases` from `df_covid` so you can confirm Spark’s understanding of your data.

**Hint:** You can either `df_covid.dtypes` or `df_covid.printSchema()`, but your output should clearly indicate the types for those three columns only.
The expected data type for  `location` should be a string type, `date` should be a date type (after casting), `new_cases` should be a numeric type (often `DoubleType`).
```python
# TODO: Show the data types for 'location', 'date', and 'new_cases' from df_covid
# Your code here

```
### Task 3 — Show the Latest Records for Canada

Display the **latest 10 records** for **Canada**, showing the columns:
- `location`
- `date`
- `new_cases`
- `new_deaths`

Order by `date` **descending** so the most recent records appear first.

```python
# TODO: Filter to Canada and show latest 10 records ordered by date descending
# Your code here
```
### Screenshots to capture for Section 3
  - Filtered preview (`df_covid` sample, e.g., `.show(10, truncate=False)`)
  - Task 2: Types for `location`, `date`, `new_cases`
  - Task 3: Latest 10 records for **Canada** (ordered by date descending)

----

## Section 4 — Join COVID Data with Population Data

In this section, you will learn how to join a large fact dataset (`df_covid`, containing daily COVID case counts) with a small dimension table (`df_pop`, containing population and continent metadata). This is one of the most common operations in data engineering and analytics. We will join the two DataFrames using the column `location`. Both tables contain this field, so Spark can match rows correctly.
The purpose of this join is to enrich the COVID dataset with population information so we can compute **per-capita metrics** such as `cases_per_million`.

### Left Join on `location`

We perform a **left join** because we want to keep all rows from the COVID dataset, even if a country is missing from the population table (rare, but safe).
We use DataFrame aliases and qualified columns to avoid ambiguous references (both datasets have a `location` and `population` column).

```
from pyspark.sql import functions as F

c = df_covid.alias("c") # df_covid (has location, date, new_cases, population, ...)
p = df_pop.alias("p") # df_pop (has location, population, continent)

df_joined = (
    c.join(p, on=F.col("c.location") == F.col("p.location"), how="left")
)

df_joined = df_joined.select(
    F.col("c.location").alias("location"),
    F.col("c.date").alias("date"),
    F.col("c.new_cases").alias("new_cases"),
    F.col("c.new_deaths").alias("new_deaths"),
    F.col("p.population").alias("population"),    
    F.col("p.continent").alias("continent")
)

df_joined.orderBy("date").show(10, truncate=False)
```

**What this join does:**
- Keeps every row from `df_covid`  
- Adds `population` and `continent` from `df_pop`  
- If a country is missing in `df_pop`, Spark fills `population` and `continent` with `null`  
- Maintains the same number of rows as the COVID subset  
- Still sorted by `date` for readability

### Task 4 — Compute `cases_per_million`

In the datafrmae `df_joined`, create a new numeric column called `cases_per_million` using this formula:
```
cases_per_million = new_cases * 1,000,000 / population
```
This metric allows you to compare countries fairly despite differences in population size. You can use the function`withColumn` to create `cases_per_million`.

After creating this column, **preview the results**:
- Filter to `cases_per_million > 0` and `cases_per_million IS NOT NULL`. **Important:** If you simply sort by `date` and show the first 10 rows, you may see many zeros (early dates often have `new_cases = 0`). 
- You may **either**:
    - order by `date` (to see earliest non‑zero values), **or**
    - order by `cases_per_million` descending (to see the largest values first).

```python
# TODO: Add cases_per_million column and preview non-zero values
# Hint: Use F.lit(1_000_000) and a filter like (F.col("cases_per_million") > 0)

# Your code here
```

### Screenshots to capture for Section 4
  - Joined table preview with `population` and `continent` 
  - Task 4: `cases_per_million` code + preview including `cases_per_million` 

----

## Section 5 — Data Cleaning (Missing Values, Deduplication, Sanity Checks)

In this section, you will **inspect**, **clean**, and **verify** the joined dataset from Section 4.  
You will:
- Identify missing values (e.g., `population`, `new_cases`)
- Drop rows that cannot support per‑capita metrics (missing `population`)
- Fill select numeric columns with sensible defaults for this lab (`new_cases`, `new_deaths` → `0`)
- Remove **duplicate** rows using a composite key (`location`, `date`)
- Verify the effect of cleaning by comparing **row counts** before and after

 **Context:** In production, “filling with zero” is not universally correct. We do it here for instructional simplicity and to stabilize downstream calculations. Always choose imputation strategies that match your data semantics.

### 1. Inspect Missingness

Use boolean expressions cast to integers to **count** nulls in key columns. This pattern is scalable and fast in Spark.

```python
from pyspark.sql import functions as F

df_joined.select(
    F.sum(F.col("population").isNull().cast("int")).alias("missing_population"),
    F.sum(F.col("new_cases").isNull().cast("int")).alias("missing_new_cases"),
    F.sum(F.col("new_deaths").isNull().cast("int")).alias("missing_new_deaths")
).show()
```

**Notes:**
- `population` should generally be present for the five countries we added; any nulls indicate a join mismatch.
- `new_cases` / `new_deaths` may have nulls due to reporting gaps.


### 2. Keep a Copy of the Pre‑Clean State

We will use this to compare row counts **before vs after** cleaning.

```python
df_before_clean = df_joined
count_before_clean = df_joined.count()
print("Rows BEFORE cleaning:", count_before_clean)
```


### 3. Perform Cleaning Steps 

- **Drop** rows missing `population` (we cannot compute per‑capita metrics without it).
- **Fill** selected numeric columns (`new_cases`, `new_deaths`) with `0` for this exercise.
- **Deduplicate** by the natural time‑series key: (`location`, `date`).

```python
# 3.1 Drop rows with missing population (per-capita metrics require a denominator)
df_clean = df_joined.dropna(subset=["population"])

# 3.2 Fill selected numeric nulls with zeros for stability in simple metrics
df_clean = df_clean.fillna({"new_cases": 0, "new_deaths": 0})

# 3.3 Remove duplicate records by (location, date)
df_clean = df_clean.dropDuplicates(["location", "date"])

# Optional preview
df_clean.orderBy("date").show(10, truncate=False)
```

### Task 5 — Compare Row Counts (Before vs After)

Print the row counts **before** and **after** cleaning. Verify that:
- The **after** count is **less than or equal to** the **before** count
- Differences are explainable by dropped `population` nulls and duplicate rows

```python
# TODO: Show counts before and after cleaning
# Your code here

# Optional: show how many were dropped
```

### 4. Sanity Check: Are There Any Remaining Nulls? (Run)

This helps confirm the dataset is in a usable state for downstream metrics.

```python
df_clean.select(
    F.sum(F.col("population").isNull().cast("int")).alias("remaining_null_population"),
    F.sum(F.col("new_cases").isNull().cast("int")).alias("remaining_null_new_cases"),
    F.sum(F.col("new_deaths").isNull().cast("int")).alias("remaining_null_new_deaths")
).show()
```

**What you want to see:**
- `remaining_null_population` should be `0` (we dropped them).
- `remaining_null_new_cases` / `remaining_null_new_deaths` should be `0` if your fill worked.


### 5. Guard the Per‑Capita Metric

If you created `cases_per_million` in Section 4 **before** cleaning, recompute it now to ensure correctness after the cleaning steps.

```python
df_clean = df_clean.withColumn(
    "cases_per_million",
    (F.col("new_cases") * F.lit(1_000_000)) / F.col("population")
)

df_clean.select.orderBy("date").show(10, truncate=False)
```

### Reflection Questions (Write short answers beneath your screenshots)
1. Why is **dropping duplicates** by (`location`, `date`) important in time‑series pipelines?  
2. When is it **not** appropriate to fill missing values with zero? Give one example.  
3. Why must rows with missing `population` be removed (or fixed) before computing per‑capita metrics?

### Screenshots to capture for Section 5
  - Row counts **before vs after** cleaning (and difference)
  - Remaining nulls check after cleaning

---

## Section 6 — Spark SQL Joins and Aggregations

In this section, you will practice joining and aggregating using **Spark SQL**.  
You already completed joins with the **DataFrame API**; SQL gives you a declarative way to write the same logic, which many analysts and data engineers prefer for multi-table queries.

In this section, we will:
- Create temporary views for small demo tables (`people`, `salary`) and the cleaned COVID table.
- Run a **LEFT JOIN** in SQL (people ↔ salary).
- Write an **aggregation** in SQL over the cleaned COVID data.
- (Provided) Solution queries you can run directly.

### 1. Create Temp Views

We expose existing DataFrames to Spark SQL by creating **temporary views**.  
You can then query them with `spark.sql("...")`.

```python
# Make sure these DataFrames exist from prior sections:
# df_people, df_salary, df_clean

df_people.createOrReplaceTempView("people")
df_salary.createOrReplaceTempView("salary")
df_clean.createOrReplaceTempView("covid_clean")
```

### 2. Example: LEFT JOIN in SQL

**Problem statement:** List everyone from the `people` table, and include their `salary` if it exists.  
If no salary record exists, the `salary` column should be `NULL`.

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

**You should observe that:**
- All rows from `people` appear.
- If a person has no salary, the `salary` column is `NULL`.
- The order puts the highest salaries first; `NULL` salaries are shown last.

### 3. Example: SQL Aggregation over `covid_clean`

**Problem statement:** Write a **single SQL query** over `covid_clean` that returns, for each `location`:

- `location`
- `population`
- `continent`
- **average** daily `new_cases` as `avg_new_cases`
- **maximum** daily `new_cases` as `max_new_cases`

```python
spark.sql("""
SELECT
  location,
  population,
  continent,
  AVG(new_cases) AS avg_new_cases,
  MAX(new_cases) AS max_new_cases
FROM covid_clean
GROUP BY location, population, continent
ORDER BY avg_new_cases DESC
LIMIT 10
""").show(truncate=False)
```

**Explanation:**
- `AVG(new_cases)` and `MAX(new_cases)` are computed per `location`.
- We include `population` and `continent` in the `GROUP BY` to keep the result uniquely defined and to prevent aggregate errors.
- The `ORDER BY` shows countries with higher average new cases first.
- `LIMIT 10` keeps the output concise for screenshots.

### 4. Join + Aggregate in one SQL query

**Problem statement:**  From the **raw** COVID subset (pre-clean, available as `df_covid` → temp view `covid_raw`), join to `df_pop` (temp view `pop`) and compute average cases per million **in SQL**.  This mirrors the DataFrame logic you did earlier.

```python
# df_covid.createOrReplaceTempView("covid_raw")
# df_pop.createOrReplaceTempView("pop")
```
After creating the views, we will write the SQL query to join and aggregate.

```python
spark.sql("""
SELECT
  c.location,
  p.continent,
  p.population,
  AVG(c.new_cases * 1000000.0 / p.population) AS avg_cases_per_million
FROM covid_raw c
LEFT JOIN pop p
  ON c.location = p.location
WHERE p.population IS NOT NULL
GROUP BY c.location, p.continent, p.population
ORDER BY avg_cases_per_million DESC
LIMIT 10
""").show(truncate=False)
```

### Reflection Questions

1. When might **SQL** be more readable or maintainable than the DataFrame API for joins and aggregations?  
2. Why should you include **dimension columns** like `population` and `continent` in the `GROUP BY` along with `location`?  

### Screenshot Requirements for Section 6

Include clear screenshots of:

1. The **LEFT JOIN** SQL query and output (`people` ↔ `salary`).  
2. Your **aggregation** SQL query over `covid_clean` and its output (with `avg_new_cases`, `max_new_cases`).  
3. **Join + Aggregate** in one SQL query
4. Answers to the reflection questions 

----

## Section 7 — Writing Data: CSV vs Parquet (and Comparing Sizes)

In this section, you will **persist** your cleaned dataset to disk in two common formats and compare their storage sizes.  
This mirrors a standard step in data engineering pipelines where results are written to **data lakes** or **lakehouses** for downstream use.

**Why this matters:**
- **CSV** is human-readable but large and schema-less.
- **Parquet** is a **columnar**, **compressed**, and **schema-aware** format optimized for analytics and Spark.
- Understanding when and why to prefer Parquet is table stakes for Big Data work.

### 1. Write Cleaned Data to CSV and Parquet

We will write the `df_clean` DataFrame (created in Section 5) to two separate directories in Colab’s `/content` workspace.

```python
# Write CSV (directory with multiple part files)
df_clean.write.mode("overwrite").csv("/content/lab7_output_csv")

# Write Parquet (directory with multiple part files and metadata)
df_clean.write.mode("overwrite").parquet("/content/lab7_output_parquet")
```

**Notes:**
- Spark writes **directories**, not single files, because data may be partitioned across tasks.
- `mode("overwrite")` ensures reruns replace older outputs.


### 2. Inspect Directory Structure

The `ls -la` shell command helps you see what Spark wrote in each folder.

```python
print("CSV directory listing:")
!ls -la /content/lab7_output_csv | head -n 20

print("\nParquet directory listing:")
!ls -la /content/lab7_output_parquet | head -n 20
```

**Notes:**
- CSV output folder will contain multiple `part-...` files (text files with CSV rows).
- Parquet output folder will contain `part-...` **binary** files plus metadata.

### 3. Compare Sizes with `du -sh`

We’ll compare the total sizes of the CSV and Parquet directories.  
`du -sh` prints **human-readable** sizes (e.g., KB, MB).

```python
print("CSV size:")
!du -sh /content/lab7_output_csv

print("\nParquet size:")
!du -sh /content/lab7_output_parquet
```

**Notes:**
- Parquet is typically **smaller** due to compression and columnar encoding.
- Your exact numbers will vary by runtime and filtered subset.

### Task 6 — Read Back the Parquet and Confirm Schema

Read the Parquet output back into a new DataFrame and print its **schema** and a small **preview**.  
This verifies that writing and reading Parquet preserves column types and data (including the `cases_per_million` if present in `df_clean`).

**Starter:**
```python
# TODO: Read back Parquet, print schema, and show sample rows
# Your code here
```

### Reflection Questions (write your answers below your screenshots)

1. Which output format had the **smaller** directory size, and **why**?  
2. Name two reasons Parquet is preferred in analytic workloads compared to CSV.  


### Screenshot Requirements for Section 7

Include clear screenshots of:
- The **write** operations to CSV and Parquet (the code cells).
- The **directory listings** for both outputs.
- The **`du -sh`** size comparison for CSV vs Parquet.
- Task 6 that reads Parquet back and shows **schema + sample rows**.  

---
## Conclusion

In this lab, you extended your Spark skills beyond basic DataFrame operations by working through a realistic mini–data‑engineering workflow. You learned how to enrich a large fact dataset (daily COVID‑19 records) with a smaller dimension table (country population metadata), clean and standardize the resulting data, compute meaningful per‑capita metrics, and write the final dataset in multiple formats. You also practiced joining data using both the DataFrame API and Spark SQL, reinforcing how Spark can operate in both programmatic and declarative styles.

You now have hands‑on experience with:
- Joining DataFrames using `on` keys
- Handling missing values strategically
- Removing duplicates in time‑series data
- Creating per‑capita and derived metrics
- Executing SQL joins and aggregations
- Writing data to CSV and Parquet, and comparing storage behavior
- Reading Parquet back into Spark and validating schema integrity

These steps reflect the core of a modern **ETL pipeline**:
1. **Extract** data from source systems  
2. **Transform** it through cleaning, validation, and enrichment  
3. **Load** it into optimized storage formats for downstream analytics  

By the end of this lab, you should feel comfortable navigating Spark across its most commonly used components—DataFrames, SQL, joins, cleaning operations, and data serialization. These skills will support you in more advanced data engineering, big‑data analytics, and cloud‑based processing workflows in future labs and real‑world projects.

---


## Submission Guidelines

This final section defines **exactly what you must submit**, how to **prove ownership** of your work in Colab, and what **quality standards** will be used for grading. Follow it carefully to avoid deductions.

- **Required screenshots:**
    - Every section lists what screenshots to include. Please add them in correct order under proper headings.
    - Make sure your final PDF report is well organized

- **Ownership Proof (Show Date/Time + Username in Colab)**
    - Add a small cell at the **top of your notebook** that prints your **username** and the **current timestamp** from the runtime.

    ```python
    import getpass, datetime
    print("CYT180 Lab 7 Owner:", getpass.getuser())
    print("Timestamp:", datetime.datetime.now().isoformat())
    ```

Take a screenshot **showing this cell’s output** and include it with your submission.  
If your environment returns a generic user (e.g., `root` in Colab), also type your full name/username in a Markdown cell and include that in the same screenshot.

- **Presentation & Professionalism Requirements**
    - **Readability:** Use `.show(10, truncate=False)` for samples so columns are not cut off.
    - **Zoom:** Ensure text is **large enough** to read in the screenshot (no tiny text).
    - **Relevance:** Keep only relevant output; avoid long walls of text/logs.
    - **Organization:** Submit in the requested order. Clearly label each section in your document.
    - **Completeness:** Missing any required screenshot or answer may incur deduction up to 100%.
