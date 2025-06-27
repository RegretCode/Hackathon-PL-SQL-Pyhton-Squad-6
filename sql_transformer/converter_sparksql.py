def convert_to_sparksql(parsed_sql):
    """Converte SQL parseado para SparkSQL formatado"""
    if not parsed_sql.get("select") or not parsed_sql.get("from"):
        return "-- Erro: Query incompleta (SELECT e FROM são obrigatórios)"
    
    lines = []
    
    # SELECT clause
    select_items = []
    for select_item in parsed_sql["select"]:
        expr_text = select_item["expression"]
        alias = select_item.get("alias")
        
        if alias:
            select_items.append(f"{expr_text} AS {alias}")
        else:
            select_items.append(expr_text)
    
    lines.append(f"SELECT {', '.join(select_items)}")
    
    # FROM clause
    lines.append(f"FROM {parsed_sql['from']}")
    
    # JOINs
    for join in parsed_sql.get("joins", []):
        lines.append(f"{join['type']} {join['table']} ON {join['condition']}")
    
    # WHERE clause
    if parsed_sql.get("where"):
        lines.append(f"WHERE {parsed_sql['where']}")
    
    # GROUP BY clause
    if parsed_sql.get("group_by"):
        lines.append(f"GROUP BY {', '.join(parsed_sql['group_by'])}")
    
    # HAVING clause
    if parsed_sql.get("having"):
        lines.append(f"HAVING {parsed_sql['having']}")
    
    # ORDER BY clause
    if parsed_sql.get("order_by"):
        lines.append(f"ORDER BY {', '.join(parsed_sql['order_by'])}")
    
    # LIMIT clause
    if parsed_sql.get("limit"):
        lines.append(f"LIMIT {parsed_sql['limit']}")
    
    return "\n".join(lines)