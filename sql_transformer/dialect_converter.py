"""
Conversor de dialetos SQL específicos (Oracle, PostgreSQL) para Spark SQL
"""
import re

class DialectConverter:
    """Converte construções específicas de dialetos SQL para Spark SQL"""
    
    @staticmethod
    def detect_dialect(sql_query):
        """Detecta o dialeto SQL baseado em palavras-chave específicas"""
        sql_upper = sql_query.upper()
        
        # Indicadores Oracle
        oracle_indicators = [
            'ROWNUM', 'SYSDATE', 'NVL', 'DECODE', 'CONNECT BY',
            'START WITH', 'DUAL', 'ROWID', 'NEXTVAL', 'CURRVAL'
        ]
        
        # Indicadores PostgreSQL
        postgres_indicators = [
            'LIMIT', 'OFFSET', 'ILIKE', 'ARRAY', 'JSONB',
            'GENERATE_SERIES', 'EXTRACT', 'AGE', 'NOW()'
        ]
        
        oracle_score = sum(1 for indicator in oracle_indicators if indicator in sql_upper)
        postgres_score = sum(1 for indicator in postgres_indicators if indicator in sql_upper)
        
        if oracle_score > postgres_score:
            return 'oracle'
        elif postgres_score > oracle_score:
            return 'postgresql'
        else:
            return 'standard'
    
    @staticmethod
    def convert_oracle_to_spark(sql_query):
        """Converte construções Oracle para Spark SQL"""
        converted = sql_query
        
        # ROWNUM -> ROW_NUMBER()
        converted = re.sub(
            r'\bROWNUM\b',
            'ROW_NUMBER() OVER (ORDER BY 1)',
            converted,
            flags=re.IGNORECASE
        )
        
        # SYSDATE -> CURRENT_TIMESTAMP
        converted = re.sub(
            r'\bSYSDATE\b',
            'CURRENT_TIMESTAMP',
            converted,
            flags=re.IGNORECASE
        )
        
        # NVL -> COALESCE
        converted = re.sub(
            r'\bNVL\s*\(',
            'COALESCE(',
            converted,
            flags=re.IGNORECASE
        )
        
        # DECODE -> CASE WHEN
        decode_pattern = r'\bDECODE\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*(?:,\s*([^)]+))?\s*\)'
        def decode_replacement(match):
            expr = match.group(1)
            value1 = match.group(2)
            result1 = match.group(3)
            default = match.group(4) if match.group(4) else 'NULL'
            return f'CASE WHEN {expr} = {value1} THEN {result1} ELSE {default} END'
        
        converted = re.sub(decode_pattern, decode_replacement, converted, flags=re.IGNORECASE)
        
        # Remover referências à tabela DUAL
        converted = re.sub(r'\bFROM\s+DUAL\b', '', converted, flags=re.IGNORECASE)
        
        return converted
    
    @staticmethod
    def convert_postgresql_to_spark(sql_query):
        """Converte construções PostgreSQL para Spark SQL"""
        converted = sql_query
        
        # ILIKE -> LIKE (Spark não suporta ILIKE nativamente)
        converted = re.sub(r'\bILIKE\b', 'LIKE', converted, flags=re.IGNORECASE)
        
        # NOW() -> CURRENT_TIMESTAMP
        converted = re.sub(r'\bNOW\s*\(\s*\)', 'CURRENT_TIMESTAMP', converted, flags=re.IGNORECASE)
        
        # EXTRACT -> DATE_PART (mais compatível com Spark)
        extract_pattern = r'\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\s*\)'
        def extract_replacement(match):
            part = match.group(1)
            expr = match.group(2)
            return f"DATE_PART('{part}', {expr})"
        
        converted = re.sub(extract_pattern, extract_replacement, converted, flags=re.IGNORECASE)
        
        # Converter arrays PostgreSQL para arrays Spark
        converted = re.sub(r'\bARRAY\[([^\]]+)\]', r'ARRAY(\1)', converted, flags=re.IGNORECASE)
        
        return converted
    
    @staticmethod
    def convert_to_spark_compatible(sql_query):
        """Converte SQL de qualquer dialeto para Spark SQL compatível"""
        dialect = DialectConverter.detect_dialect(sql_query)
        
        if dialect == 'oracle':
            return DialectConverter.convert_oracle_to_spark(sql_query)
        elif dialect == 'postgresql':
            return DialectConverter.convert_postgresql_to_spark(sql_query)
        else:
            return sql_query  # Já é padrão SQL