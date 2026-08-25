"""
Exemplo de uso do conversor Oracle para PostgreSQL
"""

from sql_transformer.oracle_to_postgresql import translate_oracle_dql_to_postgresql
from sql_transformer.dialect_converter import DialectConverter

# Exemplo 1: Query Oracle simples
oracle_query1 = """
SELECT employee_id, first_name, last_name, hire_date
FROM employees 
WHERE ROWNUM <= 10
AND hire_date >= SYSDATE - 365
"""

print("=== EXEMPLO 1: Query Oracle Simples ===")
postgresql_query1 = translate_oracle_dql_to_postgresql(oracle_query1)

# Exemplo 2: Query Oracle com funções de data
oracle_query2 = """
SELECT 
    TO_CHAR(hire_date, 'DD/MM/YYYY') as data_contratacao,
    NVL(commission_pct, 0) as comissao,
    ADD_MONTHS(hire_date, 12) as um_ano_depois,
    TRUNC(SYSDATE) as hoje
FROM employees
WHERE TO_DATE('01/01/2020', 'DD/MM/YYYY') <= hire_date
"""

print("\n=== EXEMPLO 2: Query Oracle com Funções de Data ===")
postgresql_query2 = translate_oracle_dql_to_postgresql(oracle_query2)

# Exemplo 3: Usando o DialectConverter
oracle_query3 = """
SELECT COUNT(*) FROM DUAL;
SELECT TO_NUMBER('123.45') as numero FROM DUAL;
"""

print("\n=== EXEMPLO 3: Usando DialectConverter ===")
dialect = DialectConverter.detect_dialect(oracle_query3)
print(f"Dialeto detectado: {dialect}")

postgresql_query3 = DialectConverter.convert_oracle_to_postgresql(oracle_query3)