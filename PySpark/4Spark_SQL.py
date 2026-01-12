from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("DataFrame-SQL").getOrCreate()

# Create a dataframe from a csv file
df = spark.read.csv("./data/persons.csv", header=True, inferSchema=True)
# desplaying data type of each column
df.printSchema()
# Display 5 rows
df.show(5)

"""Creating and managing temporary views"""
# Check if a temp view already exists:
spark.catalog.tableExists("my_table")

# Create one:
df.createOrReplaceTempView("my_table")

# Query it using SQL-like queries:
"""Perform SQL-like queries"""
# Query 1
spark.sql("SELECT * FROM my_table WHERE age > 25").show()
# or like in sql
spark.sql("""
    SELECT *
    FROM my_table
    WHERE age > 25
""").show()

# Query 2
spark.sql("""
    SELECT
        gender,
        AVG(salary) avg_salary
    FROM my_table
    GROUP BY gender
""").show()

# Droping a temp view once done:
spark.catalog.dropTempView("my_table")

# Check again:
spark.catalog.tableExists("my_table")

"""Subqueries"""
# Create DataFrames
employee_data = [
    (1, "John"),
    (2, "Alice"),
    (3, "Bob"),
    (4, "Emily"),
    (5, "David"),
    (6, "Sarah"),
    (7, "Michael"),
    (8, "Lisa"),
    (9, "William"),
]
employees = spark.createDataFrame(employee_data, ["id", "name"])

salary_data = [
    ("HR", 1, 60000),
    ("HR", 2, 55000),
    ("HR", 3, 58000),
    ("IT", 4, 70000),
    ("IT", 5, 72000),
    ("IT", 6, 68000),
    ("Sales", 7, 75000),
    ("Sales", 8, 78000),
    ("Sales", 9, 77000),
]
salaries = spark.createDataFrame(salary_data, ["department", "id", "salary"])

employees.show()

salaries.show()

# Register as temporary views
employees.createOrReplaceTempView("employees")
salaries.createOrReplaceTempView("salaries")

# Subquery to find employees with salaries above average
result = spark.sql("""
    SELECT name
    FROM employees
    WHERE id IN (
        SELECT id
        FROM salaries
        WHERE salary > (SELECT AVG(salary) FROM salaries)
    )
""")

result.show()

"""Window function"""
employee_salary = spark.sql("""
    select  salaries.*, employees.name
    from salaries 
    left join employees on salaries.id = employees.id
""")

employee_salary.show()

# Create a window specification
window_spec = Window.partitionBy("department").orderBy(F.desc("salary"))

# Calculate the rank of employees within each department based on salary
employee_salary.withColumn("rank", F.rank().over(window_spec)).show()

# Droping a temp view once done:
spark.catalog.dropTempView("employees")
spark.catalog.dropTempView("salaries")

spark.stop()
