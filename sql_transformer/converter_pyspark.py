def convert_to_pyspark(parsed_sql):
    """Converte SQL parseado para código PySpark"""
    lines = []
    
    # Inicializar DataFrame principal
    if parsed_sql.get("from"):
        lines.append(f'df = spark.table("{parsed_sql["from"]}")')
    else:
        return "# Erro: Tabela FROM não especificada"
    
    # Processar JOINs
    for join in parsed_sql.get("joins", []):
        join_type = join["type"].lower().replace(" join", "")
        if join_type == "inner":
            join_type = "inner"
        elif join_type == "left":
            join_type = "left"
        elif join_type == "right":
            join_type = "right"
        elif join_type == "full":
            join_type = "outer"
        
        lines.append(f'df_{join["table"]} = spark.table("{join["table"]}")')
        lines.append(f'df = df.join(df_{join["table"]}, expr("{join["condition"]}"), "{join_type}")')
    
    # Processar WHERE
    if parsed_sql.get("where"):
        where_condition = _convert_sql_condition_to_pyspark(parsed_sql["where"])
        lines.append(f'df = df.filter({where_condition})')
    
    # Processar GROUP BY
    if parsed_sql.get("group_by"):
        group_cols = ', '.join(f'col("{col}")' for col in parsed_sql["group_by"])
        
        # Identificar funções agregadas no SELECT
        agg_functions = []
        regular_selects = []
        
        for select_item in parsed_sql.get("select", []):
            expr_text = select_item["expression"]
            if any(func in expr_text.upper() for func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]):
                agg_func = _convert_aggregate_function(expr_text, select_item.get("alias"))
                agg_functions.append(agg_func)
            else:
                regular_selects.append(select_item)
        
        if agg_functions:
            lines.append(f'df = df.groupBy({group_cols}).agg({", ".join(agg_functions)})')
        else:
            lines.append(f'df = df.groupBy({group_cols})')
    
    # Processar HAVING
    if parsed_sql.get("having"):
        having_condition = _convert_sql_condition_to_pyspark(parsed_sql["having"])
        lines.append(f'df = df.filter({having_condition})')
    
    # Processar SELECT (se não houver GROUP BY)
    if parsed_sql.get("select") and not parsed_sql.get("group_by"):
        select_expressions = []
        for select_item in parsed_sql["select"]:
            expr_text = select_item["expression"]
            alias = select_item.get("alias")
            
            if alias:
                select_expressions.append(f'col("{expr_text}").alias("{alias}")')
            else:
                select_expressions.append(f'col("{expr_text}")')
        
        selects = ', '.join(select_expressions)
        lines.append(f'df = df.select({selects})')
    
    # Processar ORDER BY
    if parsed_sql.get("order_by"):
        order_expressions = []
        for order_col in parsed_sql["order_by"]:
            if "DESC" in order_col.upper():
                col_name = order_col.replace("DESC", "").replace("desc", "").strip()
                order_expressions.append(f'col("{col_name}").desc()')
            else:
                col_name = order_col.replace("ASC", "").replace("asc", "").strip()
                order_expressions.append(f'col("{col_name}").asc()')
        
        orders = ', '.join(order_expressions)
        lines.append(f'df = df.orderBy({orders})')
    
    # Processar LIMIT
    if parsed_sql.get("limit"):
        lines.append(f'df = df.limit({parsed_sql["limit"]})')
    
    # Adicionar show() para visualizar resultado
    lines.append('df.show()')
    
    return "\n".join(lines)

def _convert_sql_condition_to_pyspark(condition):
    """Converte condições SQL para expressões PySpark"""
    # Substituições básicas para compatibilidade PySpark
    condition = condition.replace("=", "==")
    return f'expr("{condition}")'  # Usar expr() para expressões complexas

def _convert_aggregate_function(expr_text, alias=None):
    """Converte funções agregadas SQL para PySpark"""
    expr_upper = expr_text.upper()
    
    if "COUNT(" in expr_upper:
        if "COUNT(*)" in expr_upper:
            result = "count('*')"
        else:
            col_name = expr_text[expr_text.find("(")+1:expr_text.find(")")]
            result = f'count(col("{col_name}"))'
    elif "SUM(" in expr_upper:
        col_name = expr_text[expr_text.find("(")+1:expr_text.find(")")]
        result = f'sum(col("{col_name}"))'
    elif "AVG(" in expr_upper:
        col_name = expr_text[expr_text.find("(")+1:expr_text.find(")")]
        result = f'avg(col("{col_name}"))'
    elif "MAX(" in expr_upper:
        col_name = expr_text[expr_text.find("(")+1:expr_text.find(")")]
        result = f'max(col("{col_name}"))'
    elif "MIN(" in expr_upper:
        col_name = expr_text[expr_text.find("(")+1:expr_text.find(")")]
        result = f'min(col("{col_name}"))'
    else:
        result = f'expr("{expr_text}")'  # Fallback para expressões complexas
    
    if alias:
        result += f'.alias("{alias}")'
    
    return result