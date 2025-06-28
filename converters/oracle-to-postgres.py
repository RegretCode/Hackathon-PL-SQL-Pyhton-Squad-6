import sqlparse
import re

def map_oracle_date_format_to_pg(oracle_format: str) -> str:
    """
    Mapeia códigos comuns de formato de data/hora do Oracle para PostgreSQL.
    """
    oracle_format_lower = oracle_format.lower()
    
    mapping = {
        'yyyy': 'YYYY', 'yy': 'YY', 'year': 'YYYY', # Adicionado 'year' para consistência
        'mm': 'MM', 'mon': 'Mon', 'month': 'Month',
        'dd': 'DD', 'hh24': 'HH24', 'mi': 'MI', 'ss': 'SS',
        'dy': 'Dy', 'day': 'Day', 'am': 'AM', 'pm': 'PM',
        'fx': '', 'fm': '', # Modificadores de formato, geralmente removidos ou ignorados
        'dd/mm/yyyy': 'DD/MM/YYYY', 'yyyy-mm-dd': 'YYYY-MM-DD',
        'dd-mon-yyyy': 'DD-Mon-YYYY', 
        'hh:mi:ss': 'HH24:MI:SS', 'hh24:mi:ss': 'HH24:MI:SS'
    }
    
    pg_format = oracle_format_lower
    for ora_code, pg_code in mapping.items():
        # Usa limites de palavra para prevenir substituições parciais (ex: 'mm' em 'month')
        pg_format = re.sub(r'\b' + re.escape(ora_code) + r'\b', pg_code, pg_format, flags=re.IGNORECASE)
    
    # Garante a capitalização correta para abreviações/nomes completos de mês e dia
    pg_format = pg_format.replace('MON', 'Mon').replace('MONTH', 'Month')
    pg_format = pg_format.replace('DAY', 'Day').replace('DY', 'Dy')

    return pg_format

def translate_oracle_dql_to_postgresql(oracle_query: str, verbose: bool = True) -> str:
    """
    Converte uma consulta DQL Oracle para PostgreSQL, abordando as principais diferenças sintáticas.
    """
    if verbose:
        print(f"\n--- Iniciando Tradução da Consulta Oracle ---")
        print(f"Consulta Oracle Original:\n{oracle_query}")

    # Usa sqlparse para obter uma consulta formatada e normalizada, converte identificadores e palavras-chave
    # para maiúsculas e remove comentários. Isso ajuda a padronizar a entrada para as regex.
    formatted_query = sqlparse.format(oracle_query, 
                                      keyword_case='upper', 
                                      identifier_case='upper', 
                                      strip_comments=True,
                                      reindent=False,  # Mantém em uma linha para simplificar o processamento de cláusulas por regex
                                      indent_width=4).strip()
    converted_query = formatted_query

    # Armazena o valor do LIMIT se ROWNUM for encontrado
    limit_value = None
    
    # --- Conversão de ROWNUM para LIMIT (Processada primeiro para remover a condição) ---
    # Padrão para encontrar ROWNUM e seu valor (ex: ROWNUM <= 5)
    # Isso busca especificamente por condições ROWNUM.
    rownum_value_match = re.search(r'\bROWNUM\s*(<=|<)\s*(\d+)\b', converted_query, re.IGNORECASE)

    if rownum_value_match:
        limit_value = rownum_value_match.group(2)
        operator = rownum_value_match.group(1)
        if operator == "<":
            limit_value = str(int(limit_value)-1)

        if verbose:
            print(f"    [CONVERT] ROWNUM {operator} {limit_value} detectado.")

        # Prioriza remover ' AND ROWNUM <= N' ou ' OR ROWNUM <= N'.
        # Se ROWNUM for a primeira condição, ele removerá 'WHERE ROWNUM <= N' e a limpeza final removerá o 'WHERE' restante.

        # 1. Remover ' AND ROWNUM <= N' ou ' OR ROWNUM <= N'
        converted_query = re.sub(r'\s+(AND|OR)\s+ROWNUM\s*(?:<=|<)\s*\d+\b', '', converted_query, flags=re.IGNORECASE)

        # 2. Remover 'WHERE ROWNUM <= N'
        converted_query = re.sub(r'\bWHERE\s+ROWNUM\s*(?:<=|<)\s*\d+\b', 'WHERE', converted_query, flags=re.IGNORECASE)
        
        # 3. Remover ROWNUM <= N se não foi pego pelas regras acima (ex: ROWNUM como única condição após WHERE ser removido)
        converted_query = re.sub(r'\s*ROWNUM\s*(?:<=|<)\s*\d+\b', '', converted_query, flags=re.IGNORECASE)

        # Limpar 'WHERE' pendurado ou excesso de espaços em branco
        converted_query = re.sub(r'\s+WHERE\s*(AND|OR)\s+', ' WHERE ', converted_query, flags=re.IGNORECASE).strip()
        converted_query = re.sub(r'\bWHERE\s*$', '', converted_query, flags=re.IGNORECASE).strip() # Remove se WHERE ficar sozinho
        converted_query = re.sub(r'\s+', ' ', converted_query).strip() # Consolida espaços

    else:
        if verbose:
            print("    [INFO] Cláusula ROWNUM não detectada ou não está em um padrão de conversão simples.")

    # --- Tratamento antecipado de operadores de data [] ----

    converted_query = re.sub(
        r'([+\-])\s*(\d+)\s+(YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)\b',
        r"\1 INTERVAL '\2 \3'",
        converted_query,
        flags=re.IGNORECASE
    )
    if verbose:
        print("    [CONVERT] Operações temporais -> INTERVAL aplicado (+/- N unidade).")

    # --- Tratamento de FROM DUAL ---
    if re.search(r'SELECT\s+COUNT\(\*\)\s+FROM\s+DUAL\s*;?', converted_query, re.IGNORECASE):
        converted_query = re.sub(r'SELECT\s+COUNT\(\*\)\s+FROM\s+DUAL\s*;?', 'SELECT 1', converted_query, flags=re.IGNORECASE).strip()
        if verbose:
            print("    [CONVERT] COUNT(*) FROM DUAL -> SELECT 1 (simplificação).")
    elif re.search(r'\bFROM\s+\S+,\s*DUAL\b', converted_query, re.IGNORECASE):
        converted_query = re.sub(r',\s*DUAL\b', '', converted_query, flags=re.IGNORECASE).strip()
        if verbose:
            print("    [CONVERT] ', DUAL' removido da lista de tabelas.")
        converted_query = re.sub(r'FROM\s*,', 'FROM', converted_query, flags=re.IGNORECASE).strip() 
    elif re.search(r'\bFROM\s+DUAL\b', converted_query, re.IGNORECASE):
        converted_query = re.sub(r'\bFROM\s+DUAL\b', '', converted_query, flags=re.IGNORECASE).strip()
        if verbose:
            print("    [CONVERT] 'FROM DUAL' removido.")


    # --- Conversão de NVL() para COALESCE() ---
    converted_query = re.sub(r'NVL\(([^,]+),\s*([^)]+)\)', r'COALESCE(\1, \2)', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] NVL() -> COALESCE().")

    # --- Funções de Data/Hora ---
    # SYSDATE -> NOW() / CURRENT_TIMESTAMP
    converted_query = re.sub(r'\bSYSDATE\b', 'NOW()', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] SYSDATE -> NOW().")

    # TRUNC(date) -> DATE_TRUNC('day', date) / TRUNC(date, 'fmt') -> DATE_TRUNC('fmt', date)
    def replace_trunc(match):
        arg1 = match.group(1).strip()
        arg2_raw = match.group(2)

        if verbose:
            print(f"DEBUG dentro de replace_trunc: arg1='{arg1}', arg2_raw='{arg2_raw}'")

        if arg2_raw:
            fmt = arg2_raw.lstrip(", ").strip("'\"").upper()
            if verbose:
                print(f"DEBUG dentro de replace_trunc: arg1='{arg1}', arg2_raw='{arg2_raw}', fmt='{fmt}'")
            if fmt == 'YYYY': return f"DATE_TRUNC('year', {arg1})"
            if fmt == 'MM': return f"DATE_TRUNC('month', {arg1})"
            if fmt == 'DD': return f"DATE_TRUNC('day', {arg1})"
            return f"DATE_TRUNC('day', {arg1})"
        
        return f"DATE_TRUNC('day', {arg1})"
    
    converted_query = re.sub(r"TRUNC\s*\(([^,)]+)(,\s*('[^']+'|\"[^\"]+\"))?\)", replace_trunc, converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] TRUNC() -> DATE_TRUNC().")

    # ADD_MONTHS(date, N) -> date + INTERVAL 'N MONTH'
    converted_query = re.sub(r'ADD_MONTHS\(([^,]+),\s*(-?\d+)\)', r'\1 + INTERVAL \'\2 MONTH\'', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] ADD_MONTHS() -> INTERVAL 'N MONTH'.")

    # LAST_DAY(date) -> (DATE_TRUNC('MONTH', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')
    converted_query = re.sub(r'LAST_DAY\(([^)]+)\)', r"(DATE_TRUNC('MONTH', \1) + INTERVAL '1 MONTH' - INTERVAL '1 DAY')", converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] LAST_DAY() -> Equivalente PG.")

    if verbose:
        print("    [CONVERT] Subtração de data (DATA - N) -> INTERVAL 'N DAY'.")

    # TO_CHAR(date, format) e TO_DATE(string, format)
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
    if verbose:
        print("    [CONVERT] TO_CHAR()/TO_DATE() - Mapeamento de formato aplicado.")

    # --- Funções de Conversão de Tipo ---
    # TO_NUMBER() para CAST(... AS NUMERIC)
    converted_query = re.sub(r'TO_NUMBER\(([^)]+)\)', r'CAST(\1 AS NUMERIC)', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] TO_NUMBER() -> CAST(... AS NUMERIC).")

    # Trata tipos de dados específicos do Oracle em CASTs explícitos
    # VARCHAR2 para VARCHAR
    converted_query = re.sub(r'VARCHAR2(\s*\(\d+\))?', r'VARCHAR\1', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] VARCHAR2 -> VARCHAR.")

    # NUMBER para NUMERIC
    converted_query = re.sub(r'\bNUMBER(\s*\(\s*\d+\s*(,\s*\d+\s*)?\))?', r'NUMERIC\1', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] NUMBER -> NUMERIC.")

    # CAST(... AS DATE) para CAST(... AS TIMESTAMP) (já que DATE do Oracle inclui tempo)
    converted_query = re.sub(r'CAST\(([^)]+)\s+AS\s+DATE\)', r'CAST(\1 AS TIMESTAMP)', converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] CAST(... AS DATE) -> CAST(... AS TIMESTAMP) (para incluir tempo).")

    # --- Conversão de DECODE() para CASE WHEN ---
    def replace_decode_with_case_when(match):
        args_str = match.group(1)
        # Parseia manualmente os argumentos 
        args = []
        balance = 0 # Contador para balanceamento de parênteses
        current_arg_chars = []
        
        # Percorre a string de argumentos para separar cada um, respeitando o aninhamento de parênteses
        for char in args_str:
            if char == '(':
                balance += 1
            elif char == ')':
                balance -= 1
            # Separa os argumentos por vírgula SOMENTE se estiver no nível superior (balance == 0)
            elif char == ',' and balance == 0:
                args.append("".join(current_arg_chars).strip())
                current_arg_chars = []
                continue
            current_arg_chars.append(char)
        
        # Adiciona o último argumento após o loop
        args.append("".join(current_arg_chars).strip())
        
        # Filtra quaisquer strings vazias que possam resultar do parsing
        args = [arg for arg in args if arg]

        # DECODE(expr, search1, result1, [search2, result2, ...], [default])
        # Um DECODE válido deve ter no mínimo 3 argumentos (expr, search1, result1)
        if len(args) < 3: 
            if verbose:
                print(f"    [WARNING] DECODE com poucos argumentos ou não parseável: {match.group(0)}. Pulando a conversão.")
            return match.group(0)

        expr = args[0]
        case_parts = []
        default_value = 'NULL' # Valor padrão do Oracle se não houver um explícito em DECODE

        # Itera sobre os pares de busca e resultado
        i = 1
        while i < len(args):
            if i + 1 < len(args): # Se houver um par (search, result)
                search = args[i]
                result = args[i+1]
                case_parts.append(f"WHEN {expr} = {search} THEN {result}")
                i += 2 # Avança para o próximo par
            else: # Este é o último argumento e é o valor padrão
                default_value = args[i]
                i += 1 # Sai do loop
        
        case_statement = f"CASE {' '.join(case_parts)} ELSE {default_value} END"
        if verbose:
            print(f"    [CONVERT] DECODE() -> CASE WHEN: Convertido {match.group(0)} para {case_statement}")
        return case_statement

    converted_query = re.sub(r"DECODE\s*\((.*?)\)", replace_decode_with_case_when, converted_query, flags=re.IGNORECASE)
    if verbose:
        print("    [CONVERT] DECODE() -> Conversão CASE WHEN aplicada.")
    
    # --- Adição Final da Cláusula LIMIT (após todas as outras transformações) ---
    if limit_value:
        converted_query = converted_query.rstrip(';').strip()
        # Garante que WHERE não fique pendurado antes do LIMIT
        converted_query = re.sub(r'\bWHERE\s*$', '', converted_query, flags=re.IGNORECASE).strip()
        converted_query += f" LIMIT {limit_value}"
        if verbose:
            print(f"    [CONVERT] Adicionado LIMIT {limit_value} ao final da consulta.")

    # --- Limpeza Final da Consulta: Garante um único ponto e vírgula e espaçamento adequado ---
    # Remover espaços extras antes do ponto e vírgula e consolidar espaços
    converted_query = converted_query.strip() # Remove todos os espaços em branco iniciais/finais
    converted_query = re.sub(r'\s+', ' ', converted_query).strip() # Consolida múltiplos espaços e re-trim
    converted_query = converted_query.rstrip(';') # Remove todos os pontos e vírgulas finais primeiro
    converted_query += ';' # Então adiciona um único ponto e vírgula no final
    
    if verbose:
        print(f"Consulta PostgreSQL Convertida:\n{converted_query}")
        print(f"--- Tradução Concluída ---\n")
    if converted_query.endswith(' '):
        converted_query = converted_query.rstrip(' ') # rstrip removes trailing spaces

    # 2. After potential removal, check if the second-to-last character is a space
    #    Make sure the string has at least two characters before checking
    if len(converted_query) >= 2 and converted_query[-2] == ' ':
        # To remove the second-to-last character, we can slice the string
        converted_query = converted_query[:-2] + converted_query[-1:]
    return converted_query


def normalize_sql_query(query: str, verbose: bool = True) -> str:
    """
    Normaliza uma consulta SQL do dialeto Oracle para PostgreSQL/SQL padrão Spark.
    Esta função assume que a consulta de entrada é do Oracle.
    """
    if verbose:
        print(f"\n--- Processando Consulta para Normalização ---")
        print(f"Consulta de Entrada:\n{query}")

    return translate_oracle_dql_to_postgresql(query, verbose)

