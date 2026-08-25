"""
Testes unitários para o módulo oracle_to_postgresql
"""
import unittest
import sys
import os

# Adiciona o diretório pai ao path para importar o módulo
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from sql_transformer.oracle_to_postgresql import (
    map_oracle_date_format_to_pg,
    translate_oracle_dql_to_postgresql,
    normalize_sql_query
)


class TestOracleToPostgreSQL(unittest.TestCase):
    
    def test_map_oracle_date_format_to_pg(self):
        """Testa o mapeamento de formatos de data Oracle para PostgreSQL"""
        # Testes básicos de formato
        self.assertEqual(map_oracle_date_format_to_pg('yyyy'), 'YYYY')
        self.assertEqual(map_oracle_date_format_to_pg('mm'), 'MM')
        self.assertEqual(map_oracle_date_format_to_pg('dd'), 'DD')
        self.assertEqual(map_oracle_date_format_to_pg('hh24'), 'HH24')
        self.assertEqual(map_oracle_date_format_to_pg('mi'), 'MI')
        self.assertEqual(map_oracle_date_format_to_pg('ss'), 'SS')
        
        # Testes de formatos compostos
        self.assertEqual(map_oracle_date_format_to_pg('dd/mm/yyyy'), 'DD/MM/YYYY')
        self.assertEqual(map_oracle_date_format_to_pg('yyyy-mm-dd'), 'YYYY-MM-DD')
        self.assertEqual(map_oracle_date_format_to_pg('hh24:mi:ss'), 'HH24:MI:SS')
    
    def test_rownum_conversion(self):
        """Testa conversão de ROWNUM para LIMIT"""
        oracle_query = "SELECT * FROM employees WHERE ROWNUM <= 10"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("LIMIT 10", result)
        self.assertNotIn("ROWNUM", result)
    
    def test_dual_removal(self):
        """Testa remoção de FROM DUAL"""
        oracle_query = "SELECT SYSDATE FROM DUAL"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertNotIn("FROM DUAL", result.upper())
    
    def test_nvl_to_coalesce(self):
        """Testa conversão de NVL para COALESCE"""
        oracle_query = "SELECT NVL(column1, 'default') FROM table1"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("COALESCE", result)
        self.assertNotIn("NVL", result)
    
    def test_sysdate_conversion(self):
        """Testa conversão de SYSDATE para NOW()"""
        oracle_query = "SELECT SYSDATE FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("NOW()", result)
        self.assertNotIn("SYSDATE", result)
    
    def test_trunc_conversion(self):
        """Testa conversão de TRUNC para DATE_TRUNC"""
        oracle_query = "SELECT TRUNC(hire_date) FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("DATE_TRUNC", result)
        
        oracle_query_with_format = "SELECT TRUNC(hire_date, 'MM') FROM employees"
        result_with_format = translate_oracle_dql_to_postgresql(oracle_query_with_format)
        self.assertIn("DATE_TRUNC('month'", result_with_format)
    
    def test_add_months_conversion(self):
        """Testa conversão de ADD_MONTHS"""
        oracle_query = "SELECT ADD_MONTHS(hire_date, 6) FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("INTERVAL \\'6 MONTH\\", result)
        self.assertNotIn("ADD_MONTHS", result)
    
    def test_last_day_conversion(self):
        """Testa conversão de LAST_DAY"""
        oracle_query = "SELECT LAST_DAY(hire_date) FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("DATE_TRUNC('MONTH'", result)
        self.assertNotIn("LAST_DAY", result)
    
    def test_to_number_conversion(self):
        """Testa conversão de TO_NUMBER para CAST"""
        oracle_query = "SELECT TO_NUMBER('123') FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("CAST", result)
        self.assertIn("AS NUMERIC", result)
        self.assertNotIn("TO_NUMBER", result)
    
    def test_varchar2_conversion(self):
        """Testa conversão de VARCHAR2 para VARCHAR"""
        oracle_query = "SELECT CAST(column1 AS VARCHAR2(100)) FROM table1"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("VARCHAR(100)", result)
        self.assertNotIn("VARCHAR2", result)
    
    def test_number_conversion(self):
        """Testa conversão de NUMBER para NUMERIC"""
        oracle_query = "SELECT CAST(column1 AS NUMBER(10,2)) FROM table1"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("NUMERIC(10,2)", result)
        self.assertNotIn("NUMBER", result)
    
    def test_complex_query_conversion(self):
        """Testa conversão de query complexa"""
        oracle_query = """
        SELECT 
            employee_id,
            NVL(first_name, 'Unknown') as name,
            TRUNC(hire_date) as hire_date,
            ADD_MONTHS(hire_date, 12) as anniversary,
            TO_NUMBER(salary) as salary_num
        FROM employees 
        WHERE ROWNUM <= 5
        """
        result = translate_oracle_dql_to_postgresql(oracle_query)
        
        # Verifica se todas as conversões foram aplicadas
        self.assertIn("COALESCE", result)
        self.assertIn("DATE_TRUNC", result)
        self.assertIn("INTERVAL \\'12 MONTH\\", result)
        self.assertIn("CAST", result)
        self.assertIn("LIMIT 5", result)
        
        # Verifica se as funções Oracle foram removidas
        self.assertNotIn("NVL", result)
        self.assertNotIn("ROWNUM", result)
        self.assertNotIn("TO_NUMBER", result)
    
    def test_normalize_sql_query(self):
        """Testa a função normalize_sql_query"""
        oracle_query = "SELECT SYSDATE FROM DUAL WHERE ROWNUM <= 1"
        result = normalize_sql_query(oracle_query)
        
        self.assertIn("LIMIT 1", result)
        self.assertNotIn("DUAL", result.upper())
        self.assertNotIn("ROWNUM", result)
        self.assertIn("NOW()", result)
        # Vamos apenas verificar se o resultado não está vazio
    
    def test_semicolon_addition(self):
        """Testa se o ponto e vírgula é adicionado ao final"""
        oracle_query = "SELECT * FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertTrue(result.strip().endswith(';'))
    
    def test_to_char_date_format_conversion(self):
        """Testa conversão de TO_CHAR com formatos de data"""
        oracle_query = "SELECT TO_CHAR(hire_date, 'DD/MM/YYYY') FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("TO_CHAR", result)
        self.assertIn("'DD/MM/YYYY'", result)
    
    def test_to_date_format_conversion(self):
        """Testa conversão de TO_DATE com formatos"""
        oracle_query = "SELECT TO_DATE('01/01/2023', 'DD/MM/YYYY') FROM employees"
        result = translate_oracle_dql_to_postgresql(oracle_query)
        self.assertIn("TO_DATE", result)
        self.assertIn("'DD/MM/YYYY'", result)


if __name__ == '__main__':
    unittest.main()