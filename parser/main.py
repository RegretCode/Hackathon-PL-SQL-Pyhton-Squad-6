"""
Main module for the SQL to Spark converter.
This module provides a command-line interface to convert SQL queries to Spark code.
"""

import argparse
import sys
import logging
import os

# Adiciona o diretório atual ao path para permitir imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from converters.pyspark_converter import PySparkConverter
from converters.spark_sql_converter import SparkSQLConverter
from sql_parser.parser import SQLParser


def main():
    """Entry point for the SQL to Spark converter."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Convert SQL to Spark code',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py --format sparksql --sql "SELECT id, name FROM users WHERE age > 18"
    python main.py --format pyspark --file input.sql
    """
    )

    parser.add_argument('--format', choices=['sparksql', 'pyspark'], required=True,
                        help='Output format: sparksql or pyspark')

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--sql', help='SQL query string')
    input_group.add_argument('--file', help='File containing SQL query')

    parser.add_argument('--output', help='Output file (default: stdout)')

    args = parser.parse_args()

    # Get the input SQL
    if args.sql:
        sql_query = args.sql
    else:
        try:
            with open(args.file, 'r') as f:
                sql_query = f.read()
        except IOError as e:
            print(f"Error reading input file: {e}", file=sys.stderr)
            return 1

    try:
        # Parse the SQL
        print("Parsing SQL query...")
        sql_parser = SQLParser()
        ast = sql_parser.parse(sql_query)

        # Convert to the requested format
        print(f"Converting to {args.format}...")
        if args.format == 'sparksql':
            converter = SparkSQLConverter()
        else:  # pyspark
            converter = PySparkConverter()

        spark_code = converter.convert(ast)

        # Output the result
        if args.output:
            try:
                with open(args.output, 'w') as f:
                    f.write(spark_code)
                print(f"Output written to {args.output}")
            except IOError as e:
                print(f"Error writing output file: {e}", file=sys.stderr)
                return 1
        else:
            print("\nGenerated Spark code:")
            print("=" * 40)
            print(spark_code)
            print("=" * 40)

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
