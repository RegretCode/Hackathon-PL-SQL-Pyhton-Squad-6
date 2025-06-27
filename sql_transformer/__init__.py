"""
SQL Transformer - Conversor de SQL para PySpark e SparkSQL
"""

from .parser import parse_sql
from .converter_pyspark import convert_to_pyspark
from .converter_sparksql import convert_to_sparksql
from .dialect_converter import DialectConverter
from .oracle_to_postgresql import translate_oracle_dql_to_postgresql, normalize_sql_query

__version__ = "1.0.0"
__all__ = [
    'parse_sql',
    'convert_to_pyspark', 
    'convert_to_sparksql',
    'DialectConverter',
    'translate_oracle_dql_to_postgresql',
    'normalize_sql_query'
]