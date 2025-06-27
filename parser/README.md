# SQL to Spark Converter

A robust parser that converts SQL/PostgreSQL scripts into Spark code (both SparkSQL and PySpark DataFrame API).

## Features

- Parses complex SQL queries including:
  - SELECT, JOIN, WHERE, GROUP BY, ORDER BY, HAVING clauses
  - Subqueries
  - Common Table Expressions (CTEs)
  - UNION operations
  - DISTINCT and LIMIT clauses
  - Window functions
  
- PostgreSQL-specific features:
  - Array access expressions (e.g., `array[1]`)
  - JSON operators (e.g., `json->>'key'`)
  - Cast expressions (both standard `CAST(expr AS type)` and PostgreSQL-style `expr::type`)

- Generates equivalent Spark code in two formats:
  - SparkSQL strings that can be executed with `spark.sql()`
  - PySpark DataFrame API code with equivalent operations

## Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Command-line interface

```bash
# Convert SQL to SparkSQL
python -m parser.main --format sparksql --sql "SELECT id, name FROM users WHERE age > 18"

# Convert SQL file to PySpark DataFrame API code
python -m parser.main --format pyspark --file input.sql

# Output to a file
python -m parser.main --format sparksql --sql "SELECT * FROM users" --output output.py
```

### Using as a library

```python
from parser.sql_parser.parser import SQLParser
from parser.converters.spark_sql_converter import SparkSQLConverter
from parser.converters.pyspark_converter import PySparkConverter

# Parse SQL to AST
sql_query = "SELECT id, name FROM users WHERE age > 18"
parser = SQLParser()
ast = parser.parse(sql_query)

# Convert to SparkSQL
spark_sql_converter = SparkSQLConverter()
spark_sql_code = spark_sql_converter.convert(ast)
print(spark_sql_code)

# Convert to PySpark DataFrame API
pyspark_converter = PySparkConverter()
pyspark_code = pyspark_converter.convert(ast)
print(pyspark_code)
```

## Project Structure

- `parser/`: Main package
  - `sql_parser/`: SQL parsing modules
    - `parser.py`: Main SQL parser
    - `window_parser.py`: Window functions parser
    - `advanced_clauses.py`: Advanced SQL features parser
    - `postgres_extensions.py`: PostgreSQL-specific features parser
  - `converters/`: Code generation modules
    - `base_converter.py`: Base converter class
    - `spark_sql_converter.py`: SparkSQL code generator
    - `pyspark_converter.py`: PySpark DataFrame API code generator
  - `ui/`: User interface modules
    - `cli.py`: Command-line interface
  - `tests/`: Test modules
  - `main.py`: Main entry point

## Examples

### Input SQL

```sql
SELECT 
    u.id, 
    u.name, 
    COUNT(o.order_id) AS order_count
FROM 
    users u
LEFT JOIN 
    orders o ON u.id = o.user_id
WHERE 
    u.active = true
GROUP BY 
    u.id, u.name
HAVING 
    COUNT(o.order_id) > 0
ORDER BY 
    order_count DESC
LIMIT 
    10
```

### Output SparkSQL

```python
spark.sql("""
SELECT u.id, u.name, COUNT(o.order_id) AS order_count
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.active = true
GROUP BY u.id, u.name
HAVING COUNT(o.order_id) > 0
ORDER BY order_count DESC
LIMIT 10
""")
```

### Output PySpark DataFrame API

```python
from pyspark.sql.functions import count, desc

df_users = spark.table("users").alias("u")
df_orders = spark.table("orders").alias("o")

result = df_users \
    .join(df_orders, df_users["id"] == df_orders["user_id"], "left") \
    .filter("u.active = true") \
    .groupBy("u.id", "u.name") \
    .agg(count("o.order_id").alias("order_count")) \
    .filter("order_count > 0") \
    .orderBy(desc("order_count")) \
    .limit(10)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
