#!/usr/bin/env python3
"""
Script para testar queries complexas preservando a estrutura original
"""

def convert_complex_sql_to_spark(sql_file_path):
    """Converte SQL complexo para SparkSQL preservando a estrutura original"""
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            original_sql = f.read().strip()
        
        # Para queries complexas com CTEs e window functions, 
        # a melhor abordagem é preservar a SQL original
        spark_sql_code = f'''spark.sql("""
{original_sql}
""")'''
        
        print("Query complexa convertida para SparkSQL")
        print("\n" + "="*60)
        print("SPARKSQL RESULT:")
        print("="*60)
        print(spark_sql_code)
        print("="*60)
        
        return spark_sql_code
        
    except Exception as e:
        print(f"Erro: {e}")
        return None

if __name__ == "__main__":
    convert_complex_sql_to_spark('tests/sample_query.sql')