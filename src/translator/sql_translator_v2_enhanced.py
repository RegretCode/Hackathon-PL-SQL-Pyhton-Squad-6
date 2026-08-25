"""
Módulo Tradutor SQL para PySpark - Versão Atualizada

Este módulo fornece funcionalidade para traduzir consultas SQL para chamadas da API DataFrame do PySpark.
Agora usa o módulo SQLParser dedicado para capacidades de análise aprimoradas, incluindo suporte para:
- JOINs (INNER, LEFT, RIGHT)
- GROUP BY com agregações
- HAVING clauses
- Funções de agregação (COUNT, SUM, AVG, MIN, MAX)
- Condições WHERE complexas
"""

from typing import Tuple, Optional
from .sql_parser import SQLParser, ParsedSQL


def convert_parsed_where_to_pyspark(where_conditions) -> str:
    """
    Converte condições WHERE analisadas para expressões de filtro PySpark.
    
    Args:
        where_conditions: Lista de objetos WhereCondition do parser
        
    Returns:
        str: Expressão de filtro PySpark
    """
    if not where_conditions:
        return ""
    
    filter_parts = []
    
    for condition in where_conditions:
        # Converter operador
        if condition.operator == "=":
            if isinstance(condition.value, str):
                filter_expr = f"F.col('{condition.column}') == '{condition.value}'"
            else:
                filter_expr = f"F.col('{condition.column}') == {condition.value}"
        elif condition.operator in ["!=", "<>"]:
            if isinstance(condition.value, str):
                filter_expr = f"F.col('{condition.column}') != '{condition.value}'"
            else:
                filter_expr = f"F.col('{condition.column}') != {condition.value}"
        elif condition.operator in [">", "<", ">=", "<="]:
            filter_expr = f"F.col('{condition.column}') {condition.operator} {condition.value}"
        elif condition.operator == "LIKE":
            filter_expr = f"F.col('{condition.column}').like('{condition.value}')"
        elif condition.operator == "IN":
            values_str = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in condition.value])
            filter_expr = f"F.col('{condition.column}').isin([{values_str}])"
        elif condition.operator == "IS NULL":
            filter_expr = f"F.col('{condition.column}').isNull()"
        elif condition.operator == "IS NOT NULL":
            filter_expr = f"F.col('{condition.column}').isNotNull()"
        else:
            filter_expr = f"F.col('{condition.column}') {condition.operator} {condition.value}"
        
        filter_parts.append(filter_expr)
        
        # Adicionar operador lógico para próxima condição
        if condition.logical_operator:
            if condition.logical_operator == "AND":
                filter_parts.append(" & ")
            elif condition.logical_operator == "OR":
                filter_parts.append(" | ")
    
    return "(" + "".join(filter_parts) + ")"


def convert_join_to_pyspark(join_clauses, base_table) -> str:
    """
    Converte cláusulas JOIN analisadas para operações de join PySpark.
    
    Args:
        join_clauses: Lista de objetos JoinClause do parser
        base_table: Nome da tabela base
        
    Returns:
        str: Código PySpark para joins
    """
    if not join_clauses:
        return ""
    
    join_code = ""
    for join in join_clauses:
        # Determinar o tipo de join
        if join.join_type.value == "INNER":
            join_method = "join"
            join_how = ""
        elif join.join_type.value == "LEFT":
            join_method = "join"
            join_how = ", 'left'"
        elif join.join_type.value == "RIGHT":
            join_method = "join"
            join_how = ", 'right'"
        else:
            join_method = "join"
            join_how = ""
        
        # Construir condição de join
        join_condition = f"F.col('{join.left_column}') == {join.right_table}_df['{join.right_column}']"
        
        # Adicionar o join
        join_code += f".{join_method}({join.right_table}_df, {join_condition}{join_how})"
    
    return join_code


def convert_group_by_to_pyspark(group_by_columns, select_columns) -> str:
    """
    Converte cláusulas GROUP BY analisadas para operações PySpark.
    
    Args:
        group_by_columns: Lista de objetos GroupByColumn do parser
        select_columns: Lista de colunas SELECT para verificar agregações
        
    Returns:
        str: Código PySpark para group by
    """
    if not group_by_columns:
        return ""
    
    # Construir lista de colunas de agrupamento
    group_cols = [f"F.col('{col.column}')" for col in group_by_columns]
    
    # Construir lista de agregações
    agg_exprs = []
    for col in select_columns:
        if col.is_aggregate:
            if col.aggregate_function == "COUNT":
                if col.name == "*":
                    agg_exprs.append("F.count(F.lit(1)).alias('count')")
                else:
                    agg_exprs.append(f"F.count('{col.name}').alias('{col.alias or col.name}')")
            elif col.aggregate_function == "SUM":
                agg_exprs.append(f"F.sum('{col.name}').alias('{col.alias or col.name}')")
            elif col.aggregate_function == "AVG":
                agg_exprs.append(f"F.avg('{col.name}').alias('{col.alias or col.name}')")
            elif col.aggregate_function == "MIN":
                agg_exprs.append(f"F.min('{col.name}').alias('{col.alias or col.name}')")
            elif col.aggregate_function == "MAX":
                agg_exprs.append(f"F.max('{col.name}').alias('{col.alias or col.name}')")
        else:
            # Coluna não agregada deve estar em GROUP BY
            if not col.is_wildcard:
                agg_exprs.append(f"F.first('{col.name}').alias('{col.alias or col.name}')")
    
    group_by_code = f".groupBy({', '.join(group_cols)})"
    if agg_exprs:
        group_by_code += f".agg({', '.join(agg_exprs)})"
    
    return group_by_code


def translate_sql_with_parser(sql_query: str) -> Tuple[str, Optional[str]]:
    """
    Traduz consulta SQL para PySpark usando o parser dedicado.
    Suporta funcionalidades avançadas como JOINs, GROUP BY, HAVING e agregações.
    
    Args:
        sql_query (str): A string da consulta SQL para traduzir
        
    Returns:
        Tuple[str, Optional[str]]: Uma tupla contendo:
            - Versão Spark SQL da consulta
            - Versão da API PySpark DataFrame (None se não suportada)
    """
    parser = SQLParser()
    parsed = parser.parse(sql_query)
    
    # Sempre criar versão Spark SQL
    spark_sql = f'spark.sql("""{sql_query.strip().rstrip(";")}""")'
    
    # Verificar se a análise foi bem-sucedida
    if not parsed.is_valid or not parsed.from_table:
        return spark_sql, None
    
    # Construir cadeia de DataFrame PySpark
    table_name = parsed.from_table
    var_name = f"{table_name}_df"
    chain = var_name
    
    # Manipular JOINs primeiro
    if parsed.join_clauses:
        join_code = convert_join_to_pyspark(parsed.join_clauses, table_name)
        chain += join_code
    
    # Manipular cláusula WHERE
    if parsed.where_conditions:
        filter_expr = convert_parsed_where_to_pyspark(parsed.where_conditions)
        chain += f".filter({filter_expr})"
    
    # Manipular GROUP BY e agregações
    if parsed.group_by_columns or parsed.has_aggregates:
        if parsed.group_by_columns:
            group_by_code = convert_group_by_to_pyspark(parsed.group_by_columns, parsed.select_columns)
            chain += group_by_code
        else:
            # Agregações sem GROUP BY
            agg_exprs = []
            for col in parsed.select_columns:
                if col.is_aggregate:
                    if col.aggregate_function == "COUNT":
                        if col.name == "*":
                            agg_exprs.append("F.count(F.lit(1)).alias('count')")
                        else:
                            agg_exprs.append(f"F.count('{col.name}').alias('{col.alias or col.name}')")
                    elif col.aggregate_function == "SUM":
                        agg_exprs.append(f"F.sum('{col.name}').alias('{col.alias or col.name}')")
                    elif col.aggregate_function == "AVG":
                        agg_exprs.append(f"F.avg('{col.name}').alias('{col.alias or col.name}')")
                    elif col.aggregate_function == "MIN":
                        agg_exprs.append(f"F.min('{col.name}').alias('{col.alias or col.name}')")
                    elif col.aggregate_function == "MAX":
                        agg_exprs.append(f"F.max('{col.name}').alias('{col.alias or col.name}')")
            if agg_exprs:
                chain += f".agg({', '.join(agg_exprs)})"
    
    # Manipular cláusula HAVING
    if parsed.having_conditions:
        having_expr = convert_parsed_where_to_pyspark(parsed.having_conditions)
        chain += f".filter({having_expr})"
    
    # Manipular cláusula SELECT (após agregações)
    if parsed.select_columns and not parsed.has_aggregates and not parsed.group_by_columns:
        # Verificar se é SELECT *
        if len(parsed.select_columns) == 1 and parsed.select_columns[0].is_wildcard:
            # SELECT * - não precisa de .select()
            pass
        else:
            # Construir lista de colunas
            formatted_cols = []
            for col in parsed.select_columns:
                if col.alias:
                    formatted_cols.append(f"F.col('{col.name}').alias('{col.alias}')")
                else:
                    formatted_cols.append(f"F.col('{col.name}')")
            
            chain += f".select({', '.join(formatted_cols)})"
    
    # Manipular cláusula ORDER BY
    if parsed.order_by_columns:
        order_items = []
        for order_col in parsed.order_by_columns:
            if order_col.direction == "DESC":
                order_items.append(f"F.col('{order_col.column}').desc()")
            else:
                order_items.append(f"F.col('{order_col.column}').asc()")
        chain += f".orderBy({', '.join(order_items)})"
    
    # Manipular cláusula LIMIT
    if parsed.limit_count:
        chain += f".limit({parsed.limit_count})"
    
    # Adicionar declaração de importação
    pyspark_code = "from pyspark.sql import functions as F\n\n" + chain
    
    return spark_sql, pyspark_code


# Função legada para compatibilidade com versões anteriores
def translate_sql(sql_query: str) -> Tuple[str, Optional[str]]:
    """
    Função legada que chama o novo tradutor baseado em parser.
    Mantida para compatibilidade com versões anteriores.
    """
    return translate_sql_with_parser(sql_query)


class SQLTranslator:
    """
    Classe de Tradutor SQL aprimorada usando o parser dedicado.
    Suporta funcionalidades avançadas como JOINs, GROUP BY, agregações e HAVING.
    """
    
    def __init__(self):
        self.translation_count = 0
        self.parser = SQLParser()
    
    def translate(self, sql_query: str) -> dict:
        """
        Traduz uma consulta SQL para ambas as APIs: Spark SQL e PySpark DataFrame.
        Inclui suporte para funcionalidades avançadas.
        
        Args:
            sql_query (str): A consulta SQL para traduzir
            
        Returns:
            dict: Um dicionário contendo os resultados da tradução
        """
        self.translation_count += 1
        
        # Analisar o SQL primeiro
        parsed = self.parser.parse(sql_query)
        
        # Traduzir usando o parser
        spark_sql, pyspark_code = translate_sql_with_parser(sql_query)
        
        return {
            'original_sql': sql_query,
            'spark_sql': spark_sql,
            'pyspark_code': pyspark_code,
            'translation_available': pyspark_code is not None,
            'translation_id': self.translation_count,
            'parsed_sql': parsed,  # Incluir estrutura analisada
            'parsing_errors': parsed.errors if not parsed.is_valid else [],
            'has_joins': bool(parsed.join_clauses),
            'has_aggregates': parsed.has_aggregates,
            'has_group_by': bool(parsed.group_by_columns),
            'has_having': bool(parsed.having_conditions)
        }
    
    def get_translation_count(self) -> int:
        """Obtém o número de traduções realizadas."""
        return self.translation_count
    
    def get_parser_info(self) -> dict:
        """Obtém informações sobre as capacidades do parser."""
        return self.parser.get_parser_info()
    
    def parse_only(self, sql_query: str) -> dict:
        """
        Analisa consulta SQL sem tradução - útil para análise.
        
        Args:
            sql_query (str): A consulta SQL para analisar
            
        Returns:
            dict: Estrutura SQL analisada
        """
        parsed = self.parser.parse(sql_query)
        
        return {
            'original_sql': sql_query,
            'is_valid': parsed.is_valid,
            'table': parsed.from_table,
            'columns': [str(col) for col in parsed.select_columns],
            'where_conditions': [str(cond) for cond in parsed.where_conditions],
            'joins': [str(join) for join in parsed.join_clauses],
            'group_by': [str(col) for col in parsed.group_by_columns],
            'having_conditions': [str(cond) for cond in parsed.having_conditions],
            'order_by': [str(col) for col in parsed.order_by_columns],
            'limit': parsed.limit_count,
            'has_aggregates': parsed.has_aggregates,
            'errors': parsed.errors
        }
