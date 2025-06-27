"""
Conversor de SQL Oracle para PostgreSQL
Traduz consultas DQL do Oracle para sintaxe PostgreSQL compatível
"""
import sqlparse
import re

def map_oracle_date_format_to_pg(oracle_format: str) -> str:
    """
    Maps Oracle date/time format codes to PostgreSQL.
    This is a simplified function covering common formats and may need expansion.
    """
    oracle_format_lower = oracle_format.lower()
    
    mapping = {
        'yyyy': 'YYYY',
        'yy': 'YY',
        'mm': 'MM',
        'mon': 'Mon', 
        'month': 'Month',
        'dd': 'DD',
        'hh24': 'HH24',
        'mi': 'MI',
        'ss': 'SS',
        'dy': 'Dy',
        'day': 'Day',
        'am': 'AM',
        'pm': 'PM',
        'fx': '', 
        'fm': '',
        'dd/mm/yyyy': 'DD/MM/YYYY',
        'yyyy-mm-dd': 'YYYY-MM-DD',
        'dd-mon-yyyy': 'DD-Mon-YYYY',
        'hh:mi:ss': 'HH24:MI:SS',
        'hh24:mi:ss': 'HH24:MI:SS'
    }
    
    pg_format = oracle_format_lower
    for ora_code, pg_code in mapping.items():
        pg_format = re.sub(r'\b' + re.escape(ora_code) + r'\b', pg_code, pg_format, flags=re.IGNORECASE)
    
    pg_format = pg_format.replace('MON', 'Mon').replace('MONTH', 'Month')
    pg_format = pg_format.replace('DAY', 'Day').replace('Day', 'Day')

    return pg_format

def translate_oracle_dql_to_postgresql(oracle_query: str) -> str:
    """
    Converts an Oracle DQL query to PostgreSQL, handling key differences.
    Uses sqlparse for initial formatting and regex for specific translations.
    This function is a manual translator and may not cover all edge cases.
    """
    print(f"\n--- Starting Oracle Query Translation ---")
    print(f"Original Oracle Query:\n{oracle_query}")

    # Format and normalize the query for easier parsing
    formatted_query = sqlparse.format(oracle_query, keyword_case='upper', 
                                     identifier_case='upper', strip_comments=True).strip()
    converted_query = formatted_query

    # 1. Handle ROWNUM to LIMIT conversion
    # This conversion is simplified and may not work for all complex cases.
    rownum_match = re.search(r'\bROWNUM\s*(<=|<)\s*(\d+)\b', converted_query, re.IGNORECASE)
    if rownum_match:
        operator = rownum_match.group(1)
        limit_value = rownum_match.group(2)
        
        # Remove the ROWNUM clause from the query
        converted_query = re.sub(r'(WHERE\s+)?\bROWNUM\s*(<=|<)\s*\d+\s*(AND|OR)?', '', converted_query, flags=re.IGNORECASE).strip()
        
        # Clean up empty WHERE or dangling AND/OR after removal
        converted_query = re.sub(r'WHERE\s*(AND|OR)?\s*$', '', converted_query, flags=re.IGNORECASE).strip()
        converted_query = re.sub(r'\s+(AND|OR)\s+(AND|OR)\s*', r' \1 ', converted_query, flags=re.IGNORECASE).strip()
        
        # Add LIMIT clause
        converted_query = f"{converted_query} LIMIT {limit_value}"
        print(f"   [CONVERT] ROWNUM {operator} {limit_value} -> LIMIT {limit_value}")
    else:
        print("   [INFO] ROWNUM clause not detected or not in simple conversion pattern (<=N or <N).")

    # 2. Remove 'FROM DUAL'
    if ' FROM DUAL' in converted_query.upper():
        converted_query = re.sub(r'\s+FROM\s+DUAL\s*;?', '', converted_query, flags=re.IGNORECASE).strip()
        print("   [CONVERT] 'FROM DUAL' removed.")

    # 3. Convert NVL() to COALESCE()
    converted_query = re.sub(r'NVL\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] NVL() -> COALESCE().")

    # 4. Date/Time Functions
    # SYSDATE -> NOW() / CURRENT_TIMESTAMP
    converted_query = re.sub(r'\bSYSDATE\b', 'NOW()', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] SYSDATE -> NOW().")

    # TRUNC(date) -> DATE_TRUNC('day', date) / TRUNC(date, 'fmt') -> DATE_TRUNC('fmt', date)
    def replace_trunc(match):
        arg1 = match.group(1).strip()
        arg2 = match.group(2)
        if arg2:
            fmt = arg2.strip(" '\"").upper()
            if fmt == 'YYYY': return f"DATE_TRUNC('year', {arg1})"
            if fmt == 'MM': return f"DATE_TRUNC('month', {arg1})"
            if fmt == 'DD': return f"DATE_TRUNC('day', {arg1})"
            return f"DATE_TRUNC('day', {arg1})"
        return f"DATE_TRUNC('day', {arg1})"
    
    converted_query = re.sub(r"TRUNC\s*\(([^,)]+)(,\s*('[^']+'|\"[^\"]+\"))?\)", replace_trunc, converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] TRUNC() -> DATE_TRUNC().")

    # ADD_MONTHS(date, N) -> date + INTERVAL 'N MONTH'
    converted_query = re.sub(r'ADD_MONTHS\(([^,]+),\s*(-?\d+)\)', r'\1 + INTERVAL \'\2 MONTH\'', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] ADD_MONTHS() -> INTERVAL 'N MONTH'.")

    # LAST_DAY(date) -> (DATE_TRUNC('MONTH', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
    converted_query = re.sub(r'LAST_DAY\(([^)]+)\)', r"(DATE_TRUNC('MONTH', \1) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')", converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] LAST_DAY() -> PG equivalent.")

    # TO_CHAR(date, format) and TO_DATE(string, format)
    def replace_to_char_to_date(match):
        func_name = match.group(1).upper()
        arg1 = match.group(2)
        oracle_format_raw = match.group(3).strip("'\"")
        pg_format = map_oracle_date_format_to_pg(oracle_format_raw)
        
        if func_name == 'TO_CHAR':
            return f"TO_CHAR({arg1}, '{pg_format}')"
        elif func_name == 'TO_DATE':
            return f"TO_DATE({arg1}, '{pg_format}')"
        return match.group(0)

    converted_query = re.sub(r"(TO_CHAR|TO_DATE)\s*\(([^,)]+)\s*,\s*('[^']+'|\"[^\"]+\")\)", replace_to_char_to_date, converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] TO_CHAR()/TO_DATE() - Format mapping applied.")

    # 5. Other generic substitutions
    if re.search(r'SELECT\s+COUNT\(\*\)\s+FROM\s+DUAL', converted_query, re.IGNORECASE):
        converted_query = re.sub(r'SELECT\s+COUNT\(\*\)\s+FROM\s+DUAL', 'SELECT 1', converted_query, flags=re.IGNORECASE)
        print("   [CONVERT] COUNT(*) FROM DUAL -> SELECT 1 (simplification).")

    # 6. Convert TO_NUMBER() to CAST(... AS NUMERIC)
    converted_query = re.sub(r'TO_NUMBER\(([^)]+)\)', r'CAST(\1 AS NUMERIC)', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] TO_NUMBER() -> CAST(... AS NUMERIC).")

    # 7. Handle Oracle specific data types in explicit CASTs
    # VARCHAR2 to VARCHAR
    converted_query = re.sub(r'VARCHAR2(\s*\(\d+\))?', r'VARCHAR\1', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] VARCHAR2 -> VARCHAR.")

    # NUMBER to NUMERIC
    converted_query = re.sub(r'NUMBER(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?', r'NUMERIC\1', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] NUMBER -> NUMERIC.")

    # CAST(... AS DATE) to CAST(... AS TIMESTAMP) (as Oracle DATE includes time)
    converted_query = re.sub(r'CAST\(([^)]+)\s+AS\s+DATE\)', r'CAST(\1 AS TIMESTAMP)', converted_query, flags=re.IGNORECASE)
    print("   [CONVERT] CAST(... AS DATE) -> CAST(... AS TIMESTAMP) (to include time).")

    # Ensure query ends with a semicolon
    if not converted_query.strip().endswith(';'):
        converted_query += ';'

    print(f"Converted PostgreSQL Query:\n{converted_query}")
    print(f"--- Translation Complete ---\n")
    return converted_query

def normalize_sql_query(query: str) -> str:
    """
    Normalizes an SQL query from Oracle dialect to PostgreSQL/Standard Spark SQL.
    This function assumes the input query is from Oracle.
    """
    print(f"\n--- Processing Query for Normalization ---")
    print(f"Input Query:\n{query}")

    return translate_oracle_dql_to_postgresql(query)