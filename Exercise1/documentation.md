# Assignment 1

Student: Simone Kern

Course: SBD

Semester: 3

### 3.1 Create a large dataset

this NOT in the postgres cmd but normal bash

```bash
cd data
python3 expand.py
```

## 4. Verification

```  count  
---------
 1000000
(1 row)

Time: 178.239 ms
 id | first_name | last_name | gender |     department     | salary |   country
----+------------+-----------+--------+--------------------+--------+--------------
  1 | Andreas    | Scott     | Male   | Audit              |  69144 | Bosnia
  2 | Tim        | Lopez     | Male   | Energy Management  |  62082 | Taiwan
  3 | David      | Ramirez   | Male   | Quality Assurance  |  99453 | South Africa
  4 | Victor     | Sanchez   | Male   | Level Design       |  95713 | Cuba
  5 | Lea        | Edwards   | Female | Energy Management  |  60425 | Iceland
  6 | Oliver     | Baker     | Male   | Payroll            |  74110 | Poland
  7 | Emily      | Lopez     | Female | SOC                |  83526 | Netherlands
  8 | Tania      | King      | Female | IT                 |  95142 | Thailand
  9 | Max        | Hernandez | Male   | Workforce Planning | 101198 | Latvia
 10 | Juliana    | Harris    | Female | Compliance         | 103336 | Chile
(10 rows)

Time: 0.532 ms
```

## 5. Analytical queries

### (a) Simple aggregation

```sql
SELECT department, AVG(salary)
FROM people_big
GROUP BY department
LIMIT 10;
```

```
      department       |         avg
-----------------------+---------------------
 Accounting            |  85150.560834888851
 Alliances             |  84864.832756437315
 Analytics             | 122363.321232406454
 API                   |  84799.041690986409
 Audit                 |  84982.559610499577
 Backend               |  84982.349086542585
 Billing               |  84928.436430727944
 Bioinformatics        |  85138.080510264425
 Brand                 |  85086.881434454358
 Business Intelligence |  85127.097446808511
(10 rows)

Time: 322.852 ms
```

### (b) Nested aggregation

```sql
SELECT country, AVG(avg_salary)
FROM (
  SELECT country, department, AVG(salary) AS avg_salary
  FROM people_big
  GROUP BY country, department
) sub
GROUP BY country
LIMIT 10;
```

```
  country   |        avg
------------+--------------------
 Algeria    | 87230.382040504578
 Argentina  | 86969.866763623360
 Armenia    | 87245.059590528218
 Australia  | 87056.715662987876
 Austria    | 87127.824046597584
 Bangladesh | 87063.832793583033
 Belgium    | 86940.103641985310
 Bolivia    | 86960.615658334041
 Bosnia     | 87102.274664951815
 Brazil     | 86977.731228862018
(10 rows)

Time: 513.575 ms
```

### (c) Top-N sort

```sql
SELECT *
FROM people_big
ORDER BY salary DESC
LIMIT 10;
```

```
   id   | first_name | last_name | gender |    department    | salary |   country    
--------+------------+-----------+--------+------------------+--------+--------------
 764650 | Tim        | Jensen    | Male   | Analytics        | 160000 | Bulgaria
  10016 | Anastasia  | Edwards   | Female | Analytics        | 159998 | Kuwait
 754528 | Adrian     | Young     | Male   | Game Analytics   | 159997 | UK
 893472 | Mariana    | Cook      | Female | People Analytics | 159995 | South Africa
 240511 | Diego      | Lopez     | Male   | Game Analytics   | 159995 | Malaysia
 359891 | Mariana    | Novak     | Female | Game Analytics   | 159992 | Mexico
  53102 | Felix      | Taylor    | Male   | Data Science     | 159989 | Bosnia
 768143 | Teresa     | Campbell  | Female | Game Analytics   | 159988 | Spain
 729165 | Antonio    | Weber     | Male   | Analytics        | 159987 | Moldova
 952549 | Adrian     | Harris    | Male   | Analytics        | 159986 | Georgia
(10 rows)

Time: 62.843 ms
```

## Exercise 1 - PostgreSQL Analytical Queries (E-commerce)

In the `ecommerce` folder:

1. Generate a new dataset by running the provided Python script.
2. Load the generated data into PostgreSQL in a **new table**.
---
1. 
in bash 
```
cd ecommerce
python dataset_generator.py
```

Output:
```bash
$ python dataset_generator.py
Generated 1,000,000 rows -> orders_1M.csv
```
Moved the csv file into the data folder.

2. Load the generated data into PostgreSQL in a **new table**

start postgres
```
docker exec -it pg-bigdata psql -U postgres
```

then use

```
-- 1. Drop table if it exists
DROP TABLE IF EXISTS orders;

-- 2. Create table
CREATE TABLE orders (
    customer_name TEXT,
    product_category TEXT,
    quantity INTEGER,
    price_per_unit NUMERIC(10,2),
    order_date DATE,
    country TEXT
);

-- 3. Load CSV into PostgreSQL
-- Make sure the CSV is accessible inside the container at /data/orders_1M.csv
\COPY orders(
    customer_name,
    product_category,
    quantity,
    price_per_unit,
    order_date,
    country
)
FROM '/data/orders_1M.csv'
DELIMITER ','
CSV HEADER;
```

## Exercise 1 - PostgreSQL Analytical Queries (E-commerce)

**A.** What is the single item with the highest `price_per_unit`?

```
postgres=# 
SELECT
    product_category,
    price_per_unit
FROM orders
ORDER BY price_per_unit DESC
LIMIT 1;

 product_category | price_per_unit 
------------------+----------------
 Automotive       |        2000.00
(1 row)
```

**B.** What are the top 3 products with the highest total quantity sold across all orders?

```
SELECT
    product_category,
    SUM(quantity) AS total_quantity_sold
FROM orders
GROUP BY product_category
ORDER BY total_quantity_sold DESC
LIMIT 3;

 product_category | total_quantity_sold 
------------------+---------------------
 Health & Beauty  |              300842
 Electronics      |              300804
 Toys             |              300598
(3 rows)
```

**C.** What is the total revenue per product category?  
(Revenue = `price_per_unit × quantity`)

```
SELECT
    product_category,
    SUM(quantity * price_per_unit) AS total_revenue
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;


 product_category | total_revenue 
------------------+---------------
 Automotive       |  306589798.86
 Electronics      |  241525009.45
 Home & Garden    |   78023780.09
 Sports           |   61848990.83
 Health & Beauty  |   46599817.89
 Office Supplies  |   38276061.64
 Fashion          |   31566368.22
 Toys             |   23271039.02
 Grocery          |   15268355.66
 Books            |   12731976.04
(10 rows)
```

**D.** Which customers have the highest total spending?

```
SELECT
    customer_name,
    SUM(quantity * price_per_unit) AS total_spent
FROM orders
GROUP BY customer_name
ORDER BY total_spent DESC
LIMIT 10;


 customer_name  | total_spent 
----------------+-------------
 Carol Taylor   |   991179.18
 Nina Lopez     |   975444.95
 Daniel Jackson |   959344.48
 Carol Lewis    |   947708.57
 Daniel Young   |   946030.14
 Alice Martinez |   935100.02
 Ethan Perez    |   934841.24
 Leo Lee        |   934796.48
 Eve Young      |   933176.86
 Ivy Rodriguez  |   925742.64
(10 rows)
```

## Exercise 2
Assuming there are naive joins executed by users, such as:
```sql
SELECT COUNT(*)
FROM people_big p1
JOIN people_big p2
  ON p1.country = p2.country;
```
## Problem Statement

This query takes more than **10 minutes** to complete, significantly slowing down the entire system. Additionally, the **OLTP database** currently in use has inherent limitations in terms of **scalability and efficiency**, especially when operating in **large-scale cloud environments**.

## Discussion Question

Considering the requirements for **scalability** and **efficiency**, what **approaches and/or optimizations** can be applied to improve the system’s:

- Scalability  
- Performance  
- Overall efficiency  

Please **elaborate with a technical discussion**.

### Answer:

The observed performance issue arises because an **OLTP database is being misused for large-scale analytical workloads**, such as self-joins over millions of rows, which are inherently expensive and poorly aligned with OLTP design principles. OLTP systems are optimized for low-latency transactional access on small subsets of data, whereas analytical queries require scanning, aggregating, and joining large portions of a dataset, leading to excessive execution times and resource contention.

To improve **performance**, immediate database-level optimizations can be applied, such as **query rewriting** (e.g., replacing naive self-joins with aggregation-based equivalents), **proper indexing**, and **partitioning** on frequently queried columns like `country`. However, while these optimizations may reduce execution time, they do not fundamentally solve the scalability limitations caused by the tight coupling of storage and compute in traditional relational databases.

From a **scalability** perspective, the slides strongly motivate **architectural separation of OLTP and OLAP workloads**. Analytical queries should be offloaded to systems specifically designed for large-scale data processing, such as **distributed processing engines (e.g., Apache Spark)** operating on column-oriented storage formats. These systems scale horizontally, distribute computation across multiple nodes, and efficiently process aggregations and joins in parallel.

For **overall efficiency**, modern cloud-native data architectures such as **data warehouses, data lakes, or lakehouse architectures** provide a balanced solution. They decouple storage and compute, support columnar data layouts, and enable cost-efficient scaling while maintaining analytical performance. In this setup, the OLTP database remains responsible for transactional workloads, while analytical processing is handled by scalable OLAP systems, aligning with the architectural best practices discussed in the lecture and significantly improving system robustness and efficiency. 

## Exercise 3
## Run with Spark (inside Jupyter)

Open your **Jupyter Notebook** environment:

- **URL:** http://localhost:8888/?token=lab  
- **Action:** Create a new notebook

Then run the following **updated Spark example**, which uses the same data stored in **PostgreSQL**.

Notebook is stored in the folder for notebooks. Output:

```
=== Loading people_big from PostgreSQL ===
Rows loaded: 1000000
Load time: 4.59 seconds

=== Query (a): AVG salary per department ===
+------------------+----------+
|department        |avg_salary|
+------------------+----------+
|Workforce Planning|85090.82  |
|Web Development   |84814.36  |
|UX Design         |84821.2   |
|UI Design         |85164.64  |
|Treasury          |84783.27  |
|Training          |85148.1   |
|Tax               |85018.57  |
|Sustainability    |85178.99  |
|Supply Chain      |84952.89  |
|Subscriptions     |84899.19  |
+------------------+----------+

Query (a) time: 3.31 seconds

=== Query (b): Nested aggregation ===
+------------+-----------------+
|country     |avg_salary       |
+------------+-----------------+
|Egypt       |87382.229633112  |
|Kuwait      |87349.3517377211 |
|Saudi Arabia|87348.80512175433|
|Panama      |87345.00623707911|
|Denmark     |87328.03514120901|
|Jamaica     |87305.437352083  |
|Lebanon     |87292.76891750695|
|Turkey      |87290.69043798617|
|Malaysia    |87253.78746341489|
|Kazakhstan  |87251.74274968785|
+------------+-----------------+

Query (b) time: 2.73 seconds

=== Query (c): Top 10 salaries ===
+------+----------+---------+------+----------------+------+------------+
|id    |first_name|last_name|gender|department      |salary|country     |
+------+----------+---------+------+----------------+------+------------+
|764650|Tim       |Jensen   |Male  |Analytics       |160000|Bulgaria    |
|10016 |Anastasia |Edwards  |Female|Analytics       |159998|Kuwait      |
|754528|Adrian    |Young    |Male  |Game Analytics  |159997|UK          |
|240511|Diego     |Lopez    |Male  |Game Analytics  |159995|Malaysia    |
|893472|Mariana   |Cook     |Female|People Analytics|159995|South Africa|
|359891|Mariana   |Novak    |Female|Game Analytics  |159992|Mexico      |
|53102 |Felix     |Taylor   |Male  |Data Science    |159989|Bosnia      |
|768143|Teresa    |Campbell |Female|Game Analytics  |159988|Spain       |
|729165|Antonio   |Weber    |Male  |Analytics       |159987|Moldova     |
|952549|Adrian    |Harris   |Male  |Analytics       |159986|Georgia     |
+------+----------+---------+------+----------------+------+------------+

Query (c) time: 3.31 seconds

=== Query (d): Heavy self-join COUNT (DANGEROUS) ===
Join count: 10983941260
Query (d) time: 15.86 seconds

=== Query (d-safe): Join-equivalent rewrite ===
+-----------+
|total_pairs|
+-----------+
|10983941260|
+-----------+

Query (d-safe) time: 1.66 seconds
```

## Exercise 4
Port the SQL queries from exercise 1 to spark.

Code: 

```python
# ============================================
# 0. Imports & Spark session
# ============================================

import time
import builtins  # <-- IMPORTANT
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    round as spark_round,   # Spark round ONLY for Columns
    count,
    col,
    sum as _sum
)

spark = (
    SparkSession.builder
    .appName("PostgresVsSparkBenchmark")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.history.fs.logDirectory", "/tmp/spark-events")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================
# 1. JDBC connection config
# ============================================

jdbc_url = "jdbc:postgresql://postgres:5432/postgres"
jdbc_props = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# ============================================
# 2.1 Load data from PostgreSQL
# ============================================

print("\n=== Loading people_big from PostgreSQL ===")

start = time.time()

df_big = spark.read.jdbc(
    url=jdbc_url,
    table="people_big",
    properties=jdbc_props
)

# Force materialization
row_count = df_big.count()

print(f"Rows loaded: {row_count}")
print("Load time:", builtins.round(time.time() - start, 2), "seconds")

# Register temp view
df_big.createOrReplaceTempView("people_big")

# ============================================
# 2.2 Load orders table from PostgreSQL
# ============================================

print("\n=== Loading orders from PostgreSQL ===")

start = time.time()

df_orders = spark.read.jdbc(
    url=jdbc_url,
    table="orders",
    properties=jdbc_props
)

# Force materialization
orders_count = df_orders.count()

print(f"Orders rows loaded: {orders_count}")
print("Load time:", builtins.round(time.time() - start, 2), "seconds")

# Register temp view
df_orders.createOrReplaceTempView("orders")

# ============================================
# 3. Query (a): single item with the highest `price_per_unit`
# ============================================

print("\n=== Query (a): single item with the highest `price_per_unit` ===")

start = time.time()

q_a = spark.sql("""
SELECT
    product_category,
    price_per_unit
FROM orders
ORDER BY price_per_unit DESC
LIMIT 1;
"""
)

q_a.collect()
q_a.show(truncate=False)
print("Query (a) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# 4. Query (b): top 3 products with the highest total quantity sold across all orders
# ============================================

print("\n=== Query (b): top 3 products with the highest total quantity sold across all orders ===")

start = time.time()

q_b = spark.sql("""
SELECT
    product_category,
    SUM(quantity) AS total_quantity_sold
FROM orders
GROUP BY product_category
ORDER BY total_quantity_sold DESC
LIMIT 3;
""")

q_b.collect()
q_b.show(truncate=False)
print("Query (b) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# 5. Query (c): total revenue per product category
# ============================================

print("\n=== Query (c): total revenue per product category ===")

start = time.time()

q_c = spark.sql("""
SELECT
    product_category,
    SUM(quantity * price_per_unit) AS total_revenue
FROM orders
GROUP BY product_category
ORDER BY total_revenue DESC;
"""
)

q_c.collect()
q_c.show(truncate=False)
print("Query (c) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# 6. Query (d): highest total spending
# ============================================

print("\n=== Query (d): highest total spending ===")

start = time.time()

q_d = spark.sql("""
SELECT
    customer_name,
    SUM(quantity * price_per_unit) AS total_spent
FROM orders
GROUP BY customer_name
ORDER BY total_spent DESC
LIMIT 10;
"""
)

q_d.collect()
q_d.show(truncate=False)
print("Query (d) time:", builtins.round(time.time() - start, 2), "seconds")

# ============================================
# 7. Cleanup
# ============================================

spark.stop()
```

Output:
```
=== Loading people_big from PostgreSQL ===
Rows loaded: 1000000
Load time: 0.24 seconds

=== Loading orders from PostgreSQL ===
Orders rows loaded: 1000000
Load time: 0.27 seconds

=== Query (a): single item with the highest `price_per_unit` ===
+----------------+--------------+
|product_category|price_per_unit|
+----------------+--------------+
|Automotive      |2000.00       |
+----------------+--------------+

Query (a) time: 1.11 seconds

=== Query (b): top 3 products with the highest total quantity sold across all orders ===
+----------------+-------------------+
|product_category|total_quantity_sold|
+----------------+-------------------+
|Health & Beauty |300842             |
|Electronics     |300804             |
|Toys            |300598             |
+----------------+-------------------+

Query (b) time: 0.96 seconds

=== Query (c): total revenue per product category ===
+----------------+-------------+
|product_category|total_revenue|
+----------------+-------------+
|Automotive      |306589798.86 |
|Electronics     |241525009.45 |
|Home & Garden   |78023780.09  |
|Sports          |61848990.83  |
|Health & Beauty |46599817.89  |
|Office Supplies |38276061.64  |
|Fashion         |31566368.22  |
|Toys            |23271039.02  |
|Grocery         |15268355.66  |
|Books           |12731976.04  |
+----------------+-------------+

Query (c) time: 1.7 seconds

=== Query (d): highest total spending ===
+--------------+-----------+
|customer_name |total_spent|
+--------------+-----------+
|Carol Taylor  |991179.18  |
|Nina Lopez    |975444.95  |
|Daniel Jackson|959344.48  |
|Carol Lewis   |947708.57  |
|Daniel Young  |946030.14  |
|Alice Martinez|935100.02  |
|Ethan Perez   |934841.24  |
|Leo Lee       |934796.48  |
|Eve Young     |933176.86  |
|Ivy Rodriguez |925742.64  |
+--------------+-----------+

Query (d) time: 1.55 seconds
```