# Conversor SparkSQL integrado
from .dialect_converter import DialectConverter

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
        return converted_sql
    else:
        # Já é SQL padrão, retornar como está
        return sql