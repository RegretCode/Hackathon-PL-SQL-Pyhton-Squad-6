"""
Test SQL to Spark converter with empresa database queries.
This module contains examples of SQL queries for the empresa database and their conversion to Spark code.
"""

import unittest
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sql_parser.parser import SQLParser
from converters.spark_sql_converter import SparkSQLConverter
from converters.pyspark_converter import PySparkConverter


class TestEmpresaQueries(unittest.TestCase):
    """Test cases for empresa database SQL queries."""

    def setUp(self):
        """Set up the test environment."""
        self.parser = SQLParser()
        self.spark_sql_converter = SparkSQLConverter()
        self.pyspark_converter = PySparkConverter()

    def test_simple_select(self):
        """Test a simple SELECT query."""
        sql = "SELECT id, nome, cargo, salario FROM funcionarios WHERE ativo = TRUE"

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nSimple SELECT to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nSimple SELECT to PySpark DataFrame API:")
        print(pyspark_code)

    def test_join_query(self):
        """Test a query with JOIN."""
        sql = """
        SELECT f.nome, d.nome as departamento, f.salario
        FROM funcionarios f
        JOIN departamentos d ON f.departamento_id = d.id
        WHERE f.salario > 5000
        ORDER BY f.salario DESC
        """

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nJOIN query to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nJOIN query to PySpark DataFrame API:")
        print(pyspark_code)

    def test_aggregate_query(self):
        """Test an aggregate query."""
        sql = """
        SELECT d.nome as departamento, COUNT(f.id) as num_funcionarios, AVG(f.salario) as media_salario
        FROM departamentos d
        LEFT JOIN funcionarios f ON d.id = f.departamento_id
        GROUP BY d.nome
        ORDER BY num_funcionarios DESC
        """

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nAggregate query to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nAggregate query to PySpark DataFrame API:")
        print(pyspark_code)

    def test_complex_join_query(self):
        """Test a complex query with multiple joins."""
        sql = """
        SELECT 
            f.nome as funcionario, 
            p.nome as projeto,
            a.horas_semanais,
            av.nota as avaliacao
        FROM 
            funcionarios f
        JOIN 
            alocacoes a ON f.id = a.funcionario_id
        JOIN 
            projetos p ON a.projeto_id = p.id
        LEFT JOIN 
            avaliacoes av ON f.id = av.funcionario_id AND p.id = av.projeto_id
        WHERE 
            f.ativo = TRUE
        ORDER BY 
            av.nota DESC
        """

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nComplex join query to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nComplex join query to PySpark DataFrame API:")
        print(pyspark_code)

    def test_subquery(self):
        """Test a query with a subquery."""
        sql = """
        SELECT 
            f.nome, 
            f.salario,
            (SELECT AVG(salario) FROM funcionarios WHERE departamento_id = f.departamento_id) as media_depto
        FROM 
            funcionarios f
        WHERE 
            f.salario > (SELECT AVG(salario) FROM funcionarios)
        ORDER BY 
            f.salario DESC
        """

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nSubquery to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nSubquery to PySpark DataFrame API:")
        print(pyspark_code)

    def test_window_function(self):
        """Test a query with window functions."""
        sql = """
        SELECT 
            f.nome,
            f.salario,
            d.nome as departamento,
            RANK() OVER (PARTITION BY f.departamento_id ORDER BY f.salario DESC) as rank_salario
        FROM 
            funcionarios f
        JOIN 
            departamentos d ON f.departamento_id = d.id
        """

        # Parse the SQL to AST
        ast = self.parser.parse(sql)

        # Convert to SparkSQL
        spark_sql_code = self.spark_sql_converter.convert(ast)
        print("\nWindow function to SparkSQL:")
        print(spark_sql_code)

        # Convert to PySpark DataFrame API
        pyspark_code = self.pyspark_converter.convert(ast)
        print("\nWindow function to PySpark DataFrame API:")
        print(pyspark_code)


if __name__ == "__main__":
    unittest.main()
