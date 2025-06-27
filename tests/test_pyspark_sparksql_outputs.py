"""
Testes para verificar se as saídas de PySpark e SparkSQL estão corretas
"""
import unittest
import sys
import os

# Adiciona o diretório pai ao path para importar os módulos
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sql_transformer.parser import parse_sql
from sql_transformer.converter_pyspark import convert_to_pyspark
from sql_transformer.converter_sparksql import convert_to_sparksql
from sql_transformer.dialect_converter import DialectConverter


class TestPySparkSparkSQLOutputs(unittest.TestCase):
    
    def test_basic_query_outputs(self):
        """Testa saídas básicas de PySpark e SparkSQL"""
        sql = "SELECT nome, idade FROM clientes WHERE idade > 30"
        parsed = parse_sql(sql)
        
        # Teste PySpark - agora usa method chaining
        pyspark_output = convert_to_pyspark(parsed)
        self.assertIn('spark.table("clientes")', pyspark_output)
        self.assertIn('.filter(expr("idade > 30"))', pyspark_output)
        self.assertIn('.select(col("nome"), col("idade"))', pyspark_output)
        self.assertIn('.show()', pyspark_output)
        # Não deve ter reassignments
        self.assertNotIn('df = df.', pyspark_output)
        
        # Teste SparkSQL
        sparksql_output = convert_to_sparksql(parsed)
        expected_sparksql = "SELECT nome, idade\nFROM clientes\nWHERE idade > 30"
        self.assertEqual(sparksql_output, expected_sparksql)
    
    def test_aggregate_query_outputs(self):
        """Testa saídas com funções agregadas"""
        sql = "SELECT COUNT(*), SUM(valor) FROM vendas GROUP BY categoria"
        parsed = parse_sql(sql)
        
        # Teste PySpark - method chaining
        pyspark_output = convert_to_pyspark(parsed)
        self.assertIn('spark.table("vendas")', pyspark_output)
        self.assertIn('groupBy(col("categoria"))', pyspark_output)
        self.assertIn("count('*')", pyspark_output)
        self.assertIn('sum(col("valor"))', pyspark_output)
        self.assertIn('.show()', pyspark_output)
        
        # Teste SparkSQL
        sparksql_output = convert_to_sparksql(parsed)
        self.assertIn("SELECT COUNT(*), SUM(valor)", sparksql_output)
        self.assertIn("GROUP BY categoria", sparksql_output)
    
    def test_join_query_outputs(self):
        """Testa saídas com JOINs"""
        sql = "SELECT c.nome, p.valor FROM clientes c LEFT JOIN pedidos p ON c.id = p.cliente_id"
        parsed = parse_sql(sql)
        
        # Teste PySpark - method chaining com JOINs
        pyspark_output = convert_to_pyspark(parsed)
        self.assertIn('spark.table("clientes c")', pyspark_output)
        self.assertIn('df_p = spark.table("p")', pyspark_output)
        self.assertIn('.join(', pyspark_output)
        self.assertIn('"left pedidos"', pyspark_output)
        self.assertIn('.show()', pyspark_output)
        
        # Teste SparkSQL
        sparksql_output = convert_to_sparksql(parsed)
        self.assertIn("LEFT JOIN pedidos p ON c.id = p.cliente_id", sparksql_output)
    
    def test_order_by_limit_outputs(self):
        """Testa saídas com ORDER BY e LIMIT"""
        sql = "SELECT nome FROM usuarios ORDER BY nome DESC LIMIT 10"
        parsed = parse_sql(sql)
        
        # Teste PySpark - method chaining
        pyspark_output = convert_to_pyspark(parsed)
        self.assertIn('.orderBy(col("nome").desc())', pyspark_output)
        self.assertIn('.limit(10)', pyspark_output)
        self.assertIn('.show()', pyspark_output)
        self.assertNotIn('df = df.', pyspark_output)  # Sem reassignments
        
        # Teste SparkSQL
        sparksql_output = convert_to_sparksql(parsed)
        self.assertIn("ORDER BY nome DESC", sparksql_output)
        self.assertIn("LIMIT 10", sparksql_output)
    
    def test_oracle_conversion_outputs(self):
        """Testa conversão de Oracle para Spark"""
        oracle_sql = "SELECT nome FROM funcionarios WHERE NVL(ativo, 'N') = 'S' AND ROWNUM <= 5"
        
        # Detectar dialeto
        dialect = DialectConverter.detect_dialect(oracle_sql)
        self.assertEqual(dialect, "oracle")
        
        # Converter para Spark
        converted_sql = DialectConverter.convert_to_spark_compatible(oracle_sql)
        
        # Verificar conversões Oracle
        self.assertIn("COALESCE", converted_sql)  # NVL -> COALESCE
        self.assertNotIn("NVL", converted_sql)
        self.assertIn("ROW_NUMBER()", converted_sql)  # ROWNUM -> ROW_NUMBER()
        self.assertNotIn("ROWNUM", converted_sql)
    
    def test_postgresql_conversion_outputs(self):
        """Testa conversão de PostgreSQL para Spark"""
        postgres_sql = "SELECT nome FROM usuarios WHERE nome ILIKE '%silva%' AND created_at >= NOW() - INTERVAL '30 days'"
        
        # Detectar dialeto
        dialect = DialectConverter.detect_dialect(postgres_sql)
        self.assertEqual(dialect, "postgresql")
        
        # Converter para Spark
        converted_sql = DialectConverter.convert_to_spark_compatible(postgres_sql)
        
        # Verificar conversões PostgreSQL
        self.assertIn("LIKE", converted_sql)  # ILIKE -> LIKE
        self.assertNotIn("ILIKE", converted_sql)
        self.assertIn("CURRENT_TIMESTAMP", converted_sql)  # NOW() -> CURRENT_TIMESTAMP
        self.assertNotIn("NOW()", converted_sql)
    
    def test_pyspark_syntax_correctness(self):
        """Testa se a sintaxe PySpark gerada está correta"""
        sql = "SELECT nome, COUNT(*) as total FROM clientes GROUP BY nome HAVING COUNT(*) > 1"
        parsed = parse_sql(sql)
        pyspark_output = convert_to_pyspark(parsed)
        
        # Verificar elementos essenciais da sintaxe PySpark
        self.assertIn('spark.table(', pyspark_output)
        self.assertIn('col(', pyspark_output)
        self.assertIn('.groupBy(', pyspark_output)
        self.assertIn('.agg(', pyspark_output)
        self.assertIn('.filter(', pyspark_output)  # Para HAVING
        self.assertIn('.show()', pyspark_output)
        # Verificar que usa method chaining
        self.assertNotIn('df = df.', pyspark_output)
    
    def test_sparksql_syntax_correctness(self):
        """Testa se a sintaxe SparkSQL gerada está correta"""
        sql = "SELECT c.nome, p.valor FROM clientes c INNER JOIN pedidos p ON c.id = p.cliente_id WHERE c.ativo = 1 ORDER BY p.valor DESC"
        parsed = parse_sql(sql)
        sparksql_output = convert_to_sparksql(parsed)
        
        # Verificar estrutura SQL correta
        lines = sparksql_output.split('\n')
        self.assertTrue(lines[0].startswith('SELECT'))
        self.assertTrue(any(line.startswith('FROM') for line in lines))
        self.assertTrue(any('INNER JOIN' in line for line in lines))
        self.assertTrue(any(line.startswith('WHERE') for line in lines))
        self.assertTrue(any(line.startswith('ORDER BY') for line in lines))
    
    def test_error_handling(self):
        """Testa tratamento de erros"""
        # Query incompleta (sem FROM)
        incomplete_sql = "SELECT nome"
        parsed = parse_sql(incomplete_sql)
        
        pyspark_output = convert_to_pyspark(parsed)
        sparksql_output = convert_to_sparksql(parsed)
        
        # Deve retornar mensagens de erro apropriadas
        self.assertIn("Erro", pyspark_output)
        self.assertIn("Erro", sparksql_output)


if __name__ == '__main__':
    unittest.main()