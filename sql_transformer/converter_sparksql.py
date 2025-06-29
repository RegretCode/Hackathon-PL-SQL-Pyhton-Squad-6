# Conversor SparkSQL integrado
from .dialect_converter import DialectConverter

def format_spark_sql(sql):
    """Format SQL query in spark.sql(''' ''') format."""
    # Replace any existing triple quotes to avoid conflicts
    sql = sql.replace('"""', "'")
    return f'spark.sql("""\n{sql}\n""")'

def convert_to_sparksql(parsed_sql):
    """Converter SQL parseado para SparkSQL compatível."""
    # Se receber um objeto ParsedSQL, usar o SQL original
    if hasattr(parsed_sql, 'original_query'):
        sql = parsed_sql.original_query
    else:
        # Se for um dicionário ou string
        sql = str(parsed_sql)
    
    # Detectar dialeto e converter para Spark compatível
    dialect = DialectConverter.detect_dialect(sql)
    
    if dialect in ['oracle', 'postgresql']:
        converted_sql = DialectConverter.convert_to_spark_compatible(sql)
    else:
        # Já é SQL padrão, usar como está
        converted_sql = sql
        
    # Format the SQL in spark.sql format
    return format_spark_sql(converted_sql)