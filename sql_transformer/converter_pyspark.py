# Conversor PySpark integrado
from .sql_translator import SimpleSQLTranslator

def convert_to_pyspark(parsed_sql):
    """Converter SQL parseado para PySpark usando o SimpleSQLTranslator."""
    translator = SimpleSQLTranslator()
    
    # Se receber um objeto ParsedSQL, reconstruir o SQL original
    if hasattr(parsed_sql, 'original_query'):
        sql = parsed_sql.original_query
    else:
        # Se for um dicionário ou string
        sql = str(parsed_sql)
    
    result = translator.translate(sql)
    
    if result['success']:
        return result['pyspark_code']
    else:
        return f"# Erro na conversão: {result['error']}"

def parse_sql(sql):
    """Função de compatibilidade para manter interface existente."""
    from .parser import SQLParser
    parser = SQLParser()
    return parser.parse(sql)