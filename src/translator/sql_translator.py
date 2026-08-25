"""
Módulo de Tradução SQL para PySpark

Este módulo fornece funcionalidade para traduzir consultas SQL para chamadas da API PySpark DataFrame.
Suporta declarações SELECT com cláusulas WHERE, ORDER BY e LIMIT.
"""

import re
from typing import Tuple, Optional


def convert_where_clause(where_clause: str) -> str:
    """
    Converte a cláusula SQL WHERE para o formato de filtro PySpark DataFrame.
    Lida com operadores e comparações básicas.
    
    Args:
        where_clause (str): A string da cláusula WHERE do SQL
        
    Returns:
        str: A expressão de filtro PySpark convertida
    """
    # Conversões simples para operadores comuns em SQL
    conversions = [
        (r"(\w+)\s*=\s*'([^']*)'", r"F.col('\1') == '\2'"),
        (r"(\w+)\s*=\s*(\d+)", r"F.col('\1') == \2"),
        (r"(\w+)\s*!=\s*'([^']*)'.", r"F.col('\1') != '\2'"),
        (r"(\w+)\s*!=\s*(\d+)", r"F.col('\1') != \2"),
        (r"(\w+)\s*<>\s*'([^']*)'.", r"F.col('\1') != '\2'"),
        (r"(\w+)\s*<>\s*(\d+)", r"F.col('\1') != \2"),
        (r"(\w+)\s*>\s*(\d+)", r"F.col('\1') > \2"),
        (r"(\w+)\s*<\s*(\d+)", r"F.col('\1') < \2"),
        (r"(\w+)\s*>=\s*(\d+)", r"F.col('\1') >= \2"),
        (r"(\w+)\s*<=\s*(\d+)", r"F.col('\1') <= \2"),
        (r"(\w+)\s+LIKE\s+'([^']*)'.", r"F.col('\1').like('\2')"),
        (r"(\w+)\s+IN\s*\(([^)]+)\)", r"F.col('\1').isin([\2])"),
        (r"\bAND\b", r" & "),
        (r"\bOR\b", r" | "),
        (r"\bNOT\s+(\w+)", r"~F.col('\1')"),
    ]

    result = where_clause
    for pattern, replacement in conversions:
        result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

    # Lidar com parênteses para expressões complexas
    result = f"({result})"

    return result


def translate_sql(sql_query: str) -> Tuple[str, Optional[str]]:
    """
    Dada uma string SQL, retorna duas strings:
    * Chamada SQL do Spark usando spark.sql(<sql_query>)
    * Cadeia de DataFrame do PySpark em `<table>_df`
    
    Args:
        sql_query (str): A string da consulta SQL para traduzir
        
    Returns:
        Tuple[str, Optional[str]]: Uma tupla contendo:
            - Versão Spark SQL da consulta
            - Versão da API PySpark DataFrame (None se não suportada)
    """
    # Limpeza de SQL: retire e remova o ponto e vírgula final
    sql_clean = sql_query.strip().rstrip(';')

    # Padrão de regex atualizado
    pattern = (
        r"SELECT\s+(?P<select>.*?)\s+FROM\s+(?P<from>\S+)"
        r"(?:\s+WHERE\s+(?P<where>.*?)(?=\s+ORDER\s+BY|\s+LIMIT|$))?"
        r"(?:\s+ORDER\s+BY\s+(?P<orderby>.*?)(?=\s+LIMIT|$))?"
        r"(?:\s+LIMIT\s+(?P<limit>\d+))?"
    )

    match = re.search(pattern, sql_clean, re.IGNORECASE | re.DOTALL)
    if not match:
        # Fallback: somente Spark SQL bruto
        spark_sql = f'spark.sql("""{sql_clean}""")'
        return spark_sql, None

    parts = match.groupdict()
    table = parts['from']
    var_name = f"{table}_df"

    # Construir Spark SQL com aspas triplas
    spark_sql = f'spark.sql("""{sql_clean}""")'

    # Construir cadeia de DataFrame PySpark
    chain = var_name

    # Manipulando a cláusula SELECT primeiro
    select_clause = parts['select'].strip()
    if select_clause != '*':
        cols = [c.strip() for c in select_clause.split(',')]
        # Lidar com aliases e funções de coluna
        formatted_cols = []
        for col in cols:
            if ' as ' in col.lower():
                # Lidar com aliases: "column_name como alias"
                col_parts = re.split(r'\s+as\s+', col, flags=re.IGNORECASE)
                if len(col_parts) == 2:
                    formatted_cols.append(f"F.col('{col_parts[0].strip()}').alias('{col_parts[1].strip()}')")
                else:
                    formatted_cols.append(f"F.col('{col.strip()}')")
            else:
                formatted_cols.append(f"F.col('{col.strip()}')")

        chain += f".select({', '.join(formatted_cols)})"

    # Manipular cláusula WHERE
    if parts.get('where'):
        where_clause = parts['where'].strip()
        # Converter operadores SQL para o formato PySpark
        where_clause = convert_where_clause(where_clause)
        chain += f".filter({where_clause})"

    # Lidar com a cláusula ORDER BY
    if parts.get('orderby'):
        order_items = []
        orderby_parts = [item.strip() for item in parts['orderby'].split(',')]
        for item in orderby_parts:
            parts_split = item.split()
            col = parts_split[0]
            direction = parts_split[1].upper() if len(parts_split) > 1 else 'ASC'
            if direction == 'DESC':
                order_items.append(f"F.col('{col}').desc()")
            else:
                order_items.append(f"F.col('{col}').asc()")
        chain += f".orderBy({', '.join(order_items)})"

    # Lidar com cláusula LIMIT
    if parts.get('limit'):
        chain += f".limit({parts['limit']})"

    # Adicionar declaração de importação para funções PySpark
    pyspark_code = "from pyspark.sql import functions as F\n\n" + chain

    return spark_sql, pyspark_code


class SQLTranslator:
    """
    Uma classe para traduzir consultas SQL para operações PySpark DataFrame.
    """
    
    def __init__(self):
        self.translation_count = 0
    
    def translate(self, sql_query: str) -> dict:
        """
        Traduz uma consulta SQL para ambas as APIs: Spark SQL e PySpark DataFrame.
        
        Args:
            sql_query (str): A consulta SQL para traduzir
            
        Returns:
            dict: Um dicionário contendo os resultados da tradução
        """
        self.translation_count += 1
        
        spark_sql, pyspark_code = translate_sql(sql_query)
        
        return {
            'original_sql': sql_query,
            'spark_sql': spark_sql,
            'pyspark_code': pyspark_code,
            'translation_available': pyspark_code is not None,
            'translation_id': self.translation_count
        }
    
    def get_translation_count(self) -> int:
        """Obtém o número de traduções realizadas."""
        return self.translation_count
