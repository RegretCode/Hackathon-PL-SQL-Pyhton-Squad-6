"""
Testes unitários para o tradutor SQL para PySpark.
"""

import unittest
from src.translator.sql_translator import translate_sql, convert_where_clause, SQLTranslator


class TestSQLTranslator(unittest.TestCase):
    
    def setUp(self):
        """Configurar fixtures de teste."""
        self.translator = SQLTranslator()
    
    def test_basic_select(self):
        """Testar tradução de declaração SELECT básica."""
        sql = "SELECT nome, idade FROM usuarios"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIn('spark.sql("""SELECT nome, idade FROM usuarios""")', spark_sql)
        self.assertIsNotNone(pyspark_code)
        self.assertIn("usuarios_df.select(F.col('nome'), F.col('idade'))", pyspark_code)
    
    def test_select_with_where(self):
        """Testar SELECT com cláusula WHERE."""
        sql = "SELECT nome FROM usuarios WHERE idade > 18"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertIn(".filter((F.col('idade') > 18))", pyspark_code)
    
    def test_select_with_order_by(self):
        """Testar SELECT com cláusula ORDER BY."""
        sql = "SELECT nome FROM usuarios ORDER BY idade DESC"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertIn(".orderBy(F.col('idade').desc())", pyspark_code)
    
    def test_select_with_limit(self):
        """Testar SELECT com cláusula LIMIT."""
        sql = "SELECT nome FROM usuarios LIMIT 10"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertIn(".limit(10)", pyspark_code)
    
    def test_select_with_alias(self):
        """Testar SELECT com aliases de colunas."""
        sql = "SELECT nome, idade as user_age FROM usuarios"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertIn("F.col('idade').alias('user_age')", pyspark_code)
    
    def test_select_all_columns(self):
        """Testar declaração SELECT *."""
        sql = "SELECT * FROM usuarios"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertEqual(pyspark_code.strip().split('\n')[-1], "usuarios_df")
    
    def test_complex_query(self):
        """Testar consulta complexa com múltiplas cláusulas."""
        sql = "SELECT nome, idade FROM usuarios WHERE idade > 18 ORDER BY idade DESC LIMIT 5"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertIn(".select(F.col('nome'), F.col('idade'))", pyspark_code)
        self.assertIn(".filter((F.col('idade') > 18))", pyspark_code)
        self.assertIn(".orderBy(F.col('idade').desc())", pyspark_code)
        self.assertIn(".limit(5)", pyspark_code)
    
    def test_where_clause_conversion(self):
        """Testar conversão de cláusula WHERE."""
        where_clause = "idade > 18 AND nome = 'João'"
        result = convert_where_clause(where_clause)
        
        self.assertIn("F.col('idade') > 18", result)
        self.assertIn("F.col('nome') == 'João'", result)
        self.assertIn(" & ", result)
    
    def test_translator_class(self):
        """Testar funcionalidade da classe SQLTranslator."""
        sql = "SELECT nome FROM usuarios"
        result = self.translator.translate(sql)
        
        self.assertIsInstance(result, dict)
        self.assertIn('original_sql', result)
        self.assertIn('spark_sql', result)
        self.assertIn('pyspark_code', result)
        self.assertIn('translation_available', result)
        self.assertIn('translation_id', result)
        
        self.assertEqual(result['original_sql'], sql)
        self.assertTrue(result['translation_available'])
        self.assertEqual(result['translation_id'], 1)
    
    def test_translation_counter(self):
        """Testar contador de tradução no SQLTranslator."""
        initial_count = self.translator.get_translation_count()
        
        self.translator.translate("SELECT * FROM test1")
        self.translator.translate("SELECT * FROM test2")
        
        final_count = self.translator.get_translation_count()
        self.assertEqual(final_count, initial_count + 2)
    
    def test_invalid_sql(self):
        """Testar tratamento de SQL inválido."""
        sql = "INVALID SQL QUERY"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(spark_sql)
        self.assertIsNone(pyspark_code)
    
    def test_sql_with_semicolon(self):
        """Testar consulta SQL com ponto e vírgula final."""
        sql = "SELECT nome FROM usuarios;"
        spark_sql, pyspark_code = translate_sql(sql)
        
        self.assertIsNotNone(pyspark_code)
        self.assertNotIn(";", pyspark_code)


class TestWhereClauseConversion(unittest.TestCase):
    
    def test_equality_string(self):
        """Testar conversão de igualdade de string."""
        where_clause = "nome = 'João'"
        result = convert_where_clause(where_clause)
        self.assertIn("F.col('nome') == 'João'", result)
    
    def test_equality_number(self):
        """Testar conversão de igualdade de número."""
        where_clause = "idade = 25"
        result = convert_where_clause(where_clause)
        self.assertIn("F.col('idade') == 25", result)
    
    def test_greater_than(self):
        """Testar conversão de maior que."""
        where_clause = "idade > 18"
        result = convert_where_clause(where_clause)
        self.assertIn("F.col('idade') > 18", result)
    
    def test_and_condition(self):
        """Testar conversão de condição AND."""
        where_clause = "idade > 18 AND nome = 'João'"
        result = convert_where_clause(where_clause)
        self.assertIn(" & ", result)
    
    def test_or_condition(self):
        """Testar conversão de condição OR."""
        where_clause = "idade > 18 OR idade < 65"
        result = convert_where_clause(where_clause)
        self.assertIn(" | ", result)


if __name__ == '__main__':
    unittest.main()
