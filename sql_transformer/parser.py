import sqlparse
import re
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML

def parse_sql(query):
    """Parser SQL avançado para suportar construções complexas"""
    query = query.strip().strip(";")
    parsed = sqlparse.parse(query)[0]
    
    result = {
        "select": [],
        "from": "",
        "joins": [],
        "where": "",
        "group_by": [],
        "having": "",
        "order_by": [],
        "limit": ""
    }
    
    # Extrair seções principais
    query_parts = _split_query_parts(str(parsed))
    
    # Parse SELECT
    if "SELECT" in query_parts:
        result["select"] = _parse_select_clause(query_parts["SELECT"])
    
    # Parse FROM
    if "FROM" in query_parts:
        from_part, joins = _parse_from_clause(query_parts["FROM"])
        result["from"] = from_part
        result["joins"] = joins
    
    # Parse WHERE
    if "WHERE" in query_parts:
        result["where"] = query_parts["WHERE"].strip()
    
    # Parse GROUP BY
    if "GROUP BY" in query_parts:
        result["group_by"] = [col.strip() for col in query_parts["GROUP BY"].split(",")]
    
    # Parse HAVING
    if "HAVING" in query_parts:
        result["having"] = query_parts["HAVING"].strip()
    
    # Parse ORDER BY
    if "ORDER BY" in query_parts:
        result["order_by"] = [col.strip() for col in query_parts["ORDER BY"].split(",")]
    
    # Parse LIMIT
    if "LIMIT" in query_parts:
        result["limit"] = query_parts["LIMIT"].strip()
    
    return result

def _split_query_parts(query):
    """Divide a query em suas partes principais"""
    parts = {}
    keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT"]
    
    query_upper = query.upper()
    positions = {}
    
    for keyword in keywords:
        match = re.search(rf'\b{keyword}\b', query_upper)
        if match:
            positions[keyword] = match.start()
    
    sorted_keywords = sorted(positions.items(), key=lambda x: x[1])
    
    for i, (keyword, pos) in enumerate(sorted_keywords):
        start_pos = pos + len(keyword)
        end_pos = sorted_keywords[i + 1][1] if i + 1 < len(sorted_keywords) else len(query)
        parts[keyword] = query[start_pos:end_pos].strip()
    
    return parts

def _parse_select_clause(select_part):
    """Parse da cláusula SELECT"""
    columns = []
    for col in select_part.split(","):
        col = col.strip()
        # Detectar aliases
        if " AS " in col.upper():
            col_parts = re.split(r'\s+AS\s+', col, flags=re.IGNORECASE)
            columns.append({"expression": col_parts[0].strip(), "alias": col_parts[1].strip()})
        else:
            columns.append({"expression": col, "alias": None})
    return columns

def _parse_from_clause(from_part):
    """Parse da cláusula FROM incluindo JOINs"""
    joins = []
    
    # Detectar JOINs
    join_pattern = r'\b(INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\b'
    join_matches = list(re.finditer(join_pattern, from_part, re.IGNORECASE))
    
    if join_matches:
        # Extrair tabela principal
        main_table = from_part[:join_matches[0].start()].strip()
        
        # Extrair JOINs
        for i, match in enumerate(join_matches):
            start = match.start()
            end = join_matches[i + 1].start() if i + 1 < len(join_matches) else len(from_part)
            join_clause = from_part[start:end].strip()
            
            join_parts = re.split(r'\s+ON\s+', join_clause, flags=re.IGNORECASE)
            if len(join_parts) == 2:
                join_info = join_parts[0].strip().split()
                joins.append({
                    "type": " ".join(join_info[:-1]),
                    "table": join_info[-1],
                    "condition": join_parts[1].strip()
                })
        
        return main_table, joins
    else:
        return from_part.strip(), []