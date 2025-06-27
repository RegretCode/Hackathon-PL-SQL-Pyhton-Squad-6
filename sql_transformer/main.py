from sql_transformer.parser import parse_sql
from sql_transformer.converter_pyspark import convert_to_pyspark
from sql_transformer.converter_sparksql import convert_to_sparksql
from sql_transformer.dialect_converter import DialectConverter

def demo_basic():
    """Demonstração básica"""
    print("=== DEMONSTRAÇÃO BÁSICA ===")
    sql = "SELECT nome, idade FROM clientes WHERE idade > 30"
    parsed = parse_sql(sql)

    print("\n--- PySpark ---")
    print(convert_to_pyspark(parsed))

    print("\n--- SparkSQL ---")
    print(convert_to_sparksql(parsed))

def demo_complex():
    """Demonstração com query complexa"""
    print("\n\n=== DEMONSTRAÇÃO COMPLEXA ===")
    sql = """
    SELECT c.nome, c.email, COUNT(p.id) as total_pedidos, SUM(p.valor) as valor_total
    FROM clientes c
    LEFT JOIN pedidos p ON c.id = p.cliente_id
    WHERE c.ativo = 1 AND c.data_cadastro >= '2023-01-01'
    GROUP BY c.nome, c.email
    HAVING COUNT(p.id) > 0
    ORDER BY valor_total DESC
    LIMIT 10
    """
    
    parsed = parse_sql(sql)
    
    print("\n--- PySpark ---")
    print(convert_to_pyspark(parsed))
    
    print("\n--- SparkSQL ---")
    print(convert_to_sparksql(parsed))

def demo_oracle():
    """Demonstração com dialeto Oracle"""
    print("\n\n=== DEMONSTRAÇÃO ORACLE ===")
    oracle_sql = """
    SELECT nome, salario, ROWNUM
    FROM (
        SELECT nome, salario
        FROM funcionarios
        WHERE NVL(ativo, 'N') = 'S'
        ORDER BY salario DESC
    )
    WHERE ROWNUM <= 5
    """
    
    print("SQL Original (Oracle):")
    print(oracle_sql)
    
    # Detectar e converter dialeto
    dialect = DialectConverter.detect_dialect(oracle_sql)
    print(f"\nDialeto detectado: {dialect}")
    
    converted_sql = DialectConverter.convert_to_spark_compatible(oracle_sql)
    print("\nSQL Convertido para Spark:")
    print(converted_sql)
    
    # Parse e conversão
    parsed = parse_sql(converted_sql)
    
    print("\n--- PySpark ---")
    print(convert_to_pyspark(parsed))

def demo_postgresql():
    """Demonstração com dialeto PostgreSQL"""
    print("\n\n=== DEMONSTRAÇÃO POSTGRESQL ===")
    postgres_sql = """
    SELECT nome, email, EXTRACT(YEAR FROM data_nascimento) as ano_nascimento
    FROM usuarios
    WHERE nome ILIKE '%silva%'
    AND created_at >= NOW() - INTERVAL '30 days'
    ORDER BY created_at DESC
    LIMIT 20
    """
    
    print("SQL Original (PostgreSQL):")
    print(postgres_sql)
    
    # Detectar e converter dialeto
    dialect = DialectConverter.detect_dialect(postgres_sql)
    print(f"\nDialeto detectado: {dialect}")
    
    converted_sql = DialectConverter.convert_to_spark_compatible(postgres_sql)
    print("\nSQL Convertido para Spark:")
    print(converted_sql)
    
    # Parse e conversão
    parsed = parse_sql(converted_sql)
    
    print("\n--- SparkSQL ---")
    print(convert_to_sparksql(parsed))

def main():
    """Função principal com demonstrações"""
    print("SQL TO SPARK TRANSFORMER - DEMONSTRACAO")
    print("=" * 50)
    
    demo_basic()
    demo_complex()
    demo_oracle()
    demo_postgresql()
    
    print("\n\n" + "=" * 50)
    print("Demonstracao concluida!")
    print("\nPara usar a interface web: streamlit run interface/app.py")
    print("Para usar o CLI: python -m sql_transformer.cli --help")

if __name__ == "__main__":
    main()