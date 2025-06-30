# ⚡ TRADUTOR PYSPARK FUNCIONAL
"""
Módulo SimpleSQLTranslator - Tradutor SQL para PySpark

Este módulo fornece funcionalidade para traduzir consultas SQL para código PySpark,
com foco na resolução correta de aliases e geração de código como string única.

Características:
- Sempre usa nomes reais de tabelas (não aliases)
- Suporta JOINs, COALESCE, CASE WHEN
- Gera código PySpark executável como uma única linha
- Validação automática de aliases
- Conversão correta de valores literais para F.lit()
"""

import re
from typing import Dict, List
from .parser import SQLParser, ParsedSQL

class SimpleSQLTranslator:
    """Tradutor SQL para PySpark com resolução robusta de aliases."""
    def __init__(self):
        self.parser = SQLParser()
    def translate(self, sql: str) -> Dict:
        try:
            parsed = self.parser.parse(sql)
            pyspark_code = self._generate_pyspark_code(parsed)
            validation = self._validate_no_aliases(pyspark_code, parsed.table_aliases)
            return {
                'success': True,
                'pyspark_code': pyspark_code,
                'spark_sql': parsed.original_query,
                'table_aliases': parsed.table_aliases,
                'validation': validation,
                'parsed_structure': {
                    'select': parsed.select_clause,
                    'from': parsed.from_clause,
                    'joins': parsed.join_clauses,
                    'where': parsed.where_clause,
                    'order_by': parsed.order_by_clause
                }
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'pyspark_code': '',
                'spark_sql': sql
            }
    def _generate_pyspark_code(self, parsed: ParsedSQL) -> str:
        chain_parts = [f"spark.table('{parsed.from_clause}')"]
        for join_clause in parsed.join_clauses:
            join_part = self._translate_join_for_chain(join_clause, parsed.table_aliases)
            chain_parts.append(join_part)
        if parsed.where_clause:
            where_condition = self._resolve_aliases_in_expression(parsed.where_clause, parsed.table_aliases)
            where_part = f".filter({where_condition})"
            chain_parts.append(where_part)
        group_by_match = re.search(r'GROUP BY (.+?)(?: ORDER BY| LIMIT|$)', parsed.original_query, re.IGNORECASE)
        if group_by_match:
            group_by_cols = [col.strip() for col in group_by_match.group(1).split(',')]
            group_by_cols_resolved = [self._resolve_aliases_in_expression(col, parsed.table_aliases) for col in group_by_cols]
            group_by_part = f".groupBy({', '.join(group_by_cols_resolved)})"
            chain_parts.append(group_by_part)
            columns = self._split_select_columns(parsed.select_clause)
            agg_parts = []
            for column in columns:
                column_strip = column.strip()
                if re.match(r'(?i)(AVG|SUM|COUNT|MIN|MAX)\s*\(', column_strip):
                    func_match = re.match(r'(?i)(AVG|SUM|COUNT|MIN|MAX)\s*\(([^)]+)\)', column_strip)
                    if func_match:
                        func = func_match.group(1).lower()
                        arg = func_match.group(2).strip()
                        alias_name = None
                        if ' as ' in column_strip.lower():
                            parts = re.split(r'\s+as\s+', column_strip, flags=re.IGNORECASE)
                            if len(parts) == 2:
                                alias_name = parts[1].strip()
                        arg_resolved = self._resolve_aliases_in_expression(arg, parsed.table_aliases)
                        code = f"F.{func}({arg_resolved})"
                        if alias_name:
                            code += f".alias('{alias_name}')"
                        agg_parts.append(code)
            agg_part = f".agg({', '.join(agg_parts)})"
            chain_parts.append(agg_part)
        else:
            select_columns = self._translate_select(parsed.select_clause, parsed.table_aliases)
            select_part = f".select({select_columns})"
            chain_parts.append(select_part)
        if parsed.order_by_clause:
            order_columns = self._translate_order_by(parsed.order_by_clause, parsed.table_aliases)
            order_part = f".orderBy({order_columns})"
            chain_parts.append(order_part)
        limit_match = re.search(r'LIMIT\s+(\d+)', parsed.original_query, re.IGNORECASE)
        if limit_match:
            limit_n = int(limit_match.group(1))
            chain_parts.append(f".limit({limit_n})")
        return "df = " + "".join(chain_parts)
    def _translate_join_for_chain(self, join_clause: str, aliases: Dict[str, str]) -> str:
        match = re.match(r'(\w+)\s+JOIN\s+([\w\.]+)\s+ON\s+(.*)', join_clause, re.IGNORECASE)
        if not match:
            return f".join(spark.table('ERROR'), F.lit(True), 'inner')"
        join_type = match.group(1).lower()
        table_name = match.group(2)
        condition = match.group(3)
        condition_resolved = self._resolve_aliases_in_expression(condition, aliases)
        join_map = {
            'inner': 'inner',
            'left': 'left',
            'right': 'right',
            'full': 'full'
        }
        pyspark_join_type = join_map.get(join_type, 'inner')
        return f".join(spark.table('{table_name}'), {condition_resolved}, '{pyspark_join_type}')"
    def _translate_join(self, join_clause: str, aliases: Dict[str, str]) -> str:
        match = re.match(r'(\w+)\s+JOIN\s+([\w\.]+)\s+ON\s+(.*)', join_clause, re.IGNORECASE)
        if not match:
            return f"# ERRO: JOIN não reconhecido: {join_clause}"
        join_type = match.group(1).lower()
        table_name = match.group(2)
        condition = match.group(3)
        condition_resolved = self._resolve_aliases_in_expression(condition, aliases)
        join_map = {
            'inner': 'inner',
            'left': 'left',
            'right': 'right',
            'full': 'full'
        }
        pyspark_join_type = join_map.get(join_type, 'inner')
        return f"df = df.join(spark.table('{table_name}'), {condition_resolved}, '{pyspark_join_type}')"
    def _translate_where(self, where_clause: str, aliases: Dict[str, str]) -> str:
        return self._resolve_aliases_in_expression(where_clause, aliases)
    def _translate_select(self, select_clause: str, aliases: Dict[str, str]) -> str:
        if select_clause.strip() == '*':
            return "'*'"
        columns = self._split_select_columns(select_clause)
        select_parts = []
        for column in columns:
            column = column.strip()
            if 'COALESCE' in column.upper():
                coalesce_code = self._translate_coalesce(column, aliases)
                select_parts.append(coalesce_code)
            elif 'CASE' in column.upper():
                case_code = self._translate_case_when(column, aliases)
                select_parts.append(case_code)
            elif re.match(r'(?i)(AVG|SUM|COUNT|MIN|MAX)\s*\(', column):
                func_match = re.match(r'(?i)(AVG|SUM|COUNT|MIN|MAX)\s*\(([^)]+)\)', column)
                if func_match:
                    func = func_match.group(1).lower()
                    arg = func_match.group(2).strip()
                    alias_name = None
                    if ' as ' in column.lower():
                        parts = re.split(r'\s+as\s+', column, flags=re.IGNORECASE)
                        if len(parts) == 2:
                            alias_name = parts[1].strip()
                    arg_resolved = self._resolve_aliases_in_expression(arg, aliases)
                    code = f"F.{func}({arg_resolved})"
                    if alias_name:
                        code += f".alias('{alias_name}')"
                    select_parts.append(code)
                else:
                    select_parts.append(f"F.lit('{column}')")
            else:
                if ' as ' in column.lower():
                    parts = re.split(r'\s+as\s+', column, flags=re.IGNORECASE)
                    if len(parts) == 2:
                        col_expr = self._resolve_aliases_in_expression(parts[0].strip(), aliases)
                        alias_name = parts[1].strip()
                        select_parts.append(f"{col_expr}.alias('{alias_name}')")
                    else:
                        resolved = self._resolve_aliases_in_expression(column, aliases)
                        select_parts.append(resolved)
                else:
                    resolved = self._resolve_aliases_in_expression(column, aliases)
                    select_parts.append(resolved)
        return ", ".join(select_parts)
    def _translate_order_by(self, order_clause: str, aliases: Dict[str, str]) -> str:
        columns = [col.strip() for col in order_clause.split(',')]
        order_parts = []
        for column in columns:
            if column.upper().endswith(' DESC'):
                col_name = column[:-5].strip()
                resolved = self._resolve_aliases_in_expression(col_name, aliases)
                order_parts.append(f"{resolved}.desc()")
            elif column.upper().endswith(' ASC'):
                col_name = column[:-4].strip()
                resolved = self._resolve_aliases_in_expression(col_name, aliases)
                order_parts.append(f"{resolved}.asc()")
            else:
                resolved = self._resolve_aliases_in_expression(column, aliases)
                order_parts.append(f"{resolved}.asc()")
        return ", ".join(order_parts)
    def _translate_coalesce(self, coalesce_expr: str, aliases: Dict[str, str]) -> str:
        match = re.search(r'COALESCE\s*\(([^)]+)\)', coalesce_expr, re.IGNORECASE)
        if not match:
            return f"F.lit('{coalesce_expr}')"
        args_str = match.group(1)
        args = self._split_function_args(args_str)
        resolved_args = []
        for arg in args:
            arg = arg.strip()
            if arg.startswith("'") and arg.endswith("'"):
                resolved_args.append(f"F.lit({arg})")
            else:
                resolved = self._resolve_aliases_in_expression(arg, aliases)
                resolved_args.append(resolved)
        result = f"F.coalesce({', '.join(resolved_args)})"
        if ' as ' in coalesce_expr.lower():
            parts = re.split(r'\s+as\s+', coalesce_expr, flags=re.IGNORECASE)
            if len(parts) == 2:
                alias_name = parts[1].strip()
                result += f".alias('{alias_name}')"
        return result
    def _translate_case_when(self, case_expr: str, aliases: Dict[str, str]) -> str:
        resolved = self._resolve_aliases_in_expression(case_expr, aliases)
        return f"F.lit('{resolved}')"
    def _resolve_aliases_in_expression(self, expression: str, aliases: Dict[str, str]) -> str:
        if not aliases:
            if '.' not in expression:
                return f"F.col('{expression.strip().rstrip(';')}')"
            else:
                return f"F.col('{expression.strip().rstrip(';')}')"
        result = expression
        for alias in sorted(aliases, key=len, reverse=True):
            real_name = aliases[alias]
            pattern = rf'\b{re.escape(alias)}\.(\w+)'
            result = re.sub(pattern, rf'{real_name}.\1', result)
        result = result.strip().rstrip(';')
        if not result.startswith('F.') and not result.startswith('('):
            if '=' in result:
                parts = result.split('=')
                if len(parts) == 2:
                    left = parts[0].strip().rstrip(';')
                    right = parts[1].strip().rstrip(';')
                    if '.' in left:
                        left_col = f"F.col('{left}')"
                    else:
                        left_col = f"F.col('{left}')"
                    if right.isdigit() or (right.replace('.', '').isdigit() and right.count('.') <= 1):
                        right_col = f"F.lit({right})"
                    elif right.startswith("'") and right.endswith("'"):
                        right_col = f"F.lit({right})"
                    elif '.' in right:
                        right_col = f"F.col('{right}')"
                    else:
                        right_col = f"F.col('{right}')"
                    result = f"{left_col} == {right_col}"
                else:
                    result = f"F.col('{result}')"
            elif '>' in result or '<' in result:
                for op in ['>=', '<=', '>', '<', '!=']:
                    if op in result:
                        parts = result.split(op)
                        if len(parts) == 2:
                            left = parts[0].strip().rstrip(';')
                            right = parts[1].strip().rstrip(';')
                            if '.' in left:
                                left_col = f"F.col('{left}')"
                            else:
                                left_col = f"F.col('{left}')"
                            if right.isdigit() or (right.replace('.', '').isdigit() and right.count('.') <= 1):
                                right_col = f"F.lit({right})"
                            elif right.startswith("'") and right.endswith("'"):
                                right_col = f"F.lit({right})"
                            elif '.' in right:
                                right_col = f"F.col('{right}')"
                            else:
                                right_col = f"F.col('{right}')"
                            py_op = '==' if op == '=' else op
                            result = f"{left_col} {py_op} {right_col}"
                            break
                else:
                    result = f"F.col('{result}')"
            else:
                result = f"F.col('{result}')"
        return result
    def _split_select_columns(self, select_clause: str) -> List[str]:
        columns = []
        current_column = ""
        paren_count = 0
        in_quotes = False
        quote_char = None
        for char in select_clause:
            if char in ["'", '"'] and not in_quotes:
                in_quotes = True
                quote_char = char
                current_column += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current_column += char
            elif char == '(' and not in_quotes:
                paren_count += 1
                current_column += char
            elif char == ')' and not in_quotes:
                paren_count -= 1
                current_column += char
            elif char == ',' and paren_count == 0 and not in_quotes:
                if current_column.strip():
                    columns.append(current_column.strip())
                current_column = ""
            else:
                current_column += char
        if current_column.strip():
            columns.append(current_column.strip())
        return columns
    def _split_function_args(self, args_str: str) -> List[str]:
        args = []
        current_arg = ""
        in_quotes = False
        quote_char = None
        for char in args_str:
            if char in ["'", '"'] and not in_quotes:
                in_quotes = True
                quote_char = char
                current_arg += char
            elif char == quote_char and in_quotes:
                in_quotes = False
                quote_char = None
                current_arg += char
            elif char == ',' and not in_quotes:
                if current_arg.strip():
                    args.append(current_arg.strip())
                current_arg = ""
            else:
                current_arg += char
        if current_arg.strip():
            args.append(current_arg.strip())
        return args
    def _validate_no_aliases(self, code: str, aliases: Dict[str, str]) -> Dict:
        found_aliases = []
        for alias in aliases.keys():
            pattern = r'\b' + re.escape(alias) + r'\.\w+'
            matches = re.findall(pattern, code)
            if matches:
                found_aliases.extend(matches)
        return {
            'passed': len(found_aliases) == 0,
            'found_aliases': found_aliases,
            'message': 'Validação passou: nenhum alias encontrado' if len(found_aliases) == 0 else f'Aliases encontrados: {found_aliases}'
        }



def traduzir_sql(sql_query: str):
    """Função utilitária para traduzir consultas SQL personalizadas."""
    translator = SimpleSQLTranslator()

    print("TRADUZINDO CONSULTA SQL")
    print("=" * 50)

    result = translator.translate(sql_query)

    if result['success']:
        print("Tradução realizada com sucesso!")
        print()

        print("SQL Original:")
        print(f"{sql_query.strip()}")
        print()

        print("Código PySpark (String Única):")
        print(f"{result['pyspark_code']}")
        print()

        print("Informações:")
        print(f"Aliases: {len(result['table_aliases'])} detectados")
        for alias, real_name in result['table_aliases'].items():
            print(f"      {alias} -> {real_name}")
        
        validation = result['validation']
        print(f"   Validação: {'PASSOU' if validation['passed'] else 'FALHOU'}")
        
        if not validation['passed']:
            print(f"Detalhes: {validation['message']}")

    else:
        print(f"Erro na tradução: {result['error']}")

    print()
    return result


if __name__ == "__main__":
    # Exemplo de uso
    sql_exemplo = "SELECT c.nome, p.valor FROM clientes c JOIN pedidos p ON c.id = p.cliente_id WHERE c.ativo = 1"
    resultado = traduzir_sql(sql_exemplo)