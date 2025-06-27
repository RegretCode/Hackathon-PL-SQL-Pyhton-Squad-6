"""
Demo script for the SQL to Spark converter.
This script demonstrates how to use the converter with a complex SQL query.
"""

from converters.pyspark_converter import PySparkConverter
from converters.spark_sql_converter import SparkSQLConverter
from sql_parser.parser import SQLParser
import sys
import os

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    """Main function for the demo script."""

    # Read the sample query
    sample_query_path = os.path.join(
        os.path.dirname(__file__), "tests", "sample_query.sql")
    try:
        with open(sample_query_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
    except IOError as e:
        print(f"Error reading sample query file: {e}", file=sys.stderr)
        # Use a simple query as fallback
        sql_query = "SELECT f.nome, d.nome as departamento FROM funcionarios f JOIN departamentos d ON f.departamento_id = d.id WHERE f.ativo = TRUE"

    print("=" * 80)
    print("SQL to Spark Converter Demo")
    print("=" * 80)
    print("\nInput SQL Query:")
    print("-" * 40)
    print(sql_query)
    print("-" * 40)

    try:
        # Parse the SQL query
        print("\nParsing SQL query...")
        sql_parser = SQLParser()
        ast = sql_parser.parse(sql_query)
        print("SQL query parsed successfully!")

        # Convert to SparkSQL
        print("\nConverting to SparkSQL...")
        spark_sql_converter = SparkSQLConverter()
        spark_sql_code = spark_sql_converter.convert(ast)

        print("\nSparkSQL Output:")
        print("-" * 40)
        print(spark_sql_code)
        print("-" * 40)

        # Convert to PySpark DataFrame API
        print("\nConverting to PySpark DataFrame API...")
        pyspark_converter = PySparkConverter()
        pyspark_code = pyspark_converter.convert(ast)

        print("\nPySpark DataFrame API Output:")
        print("-" * 40)
        print(pyspark_code)
        print("-" * 40)

        print("\nDemo completed successfully!")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
