# 🔧 SQL PARSER FUNCIONAL
"""
Módulo SQLParser - Parser SQL robusto para análise de consultas

Este módulo fornece funcionalidade para analisar consultas SQL e extrair:
- SELECT (colunas, aliases, funções)
- FROM (tabela principal)
- JOIN (tipos, condições, aliases)
- WHERE (condições de filtro)
- ORDER BY (ordenação)
- COALESCE e CASE WHEN

Foco na resolução correta de aliases de tabelas.
"""

import re
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class ParsedSQL:
    """Container para consulta SQL analisada."""
    select_clause: str
    from_clause: str
    join_clauses: List[str]
    where_clause: str
    order_by_clause: str
    table_aliases: Dict[str, str]  # alias -> real_name
    original_query: str

class SQLParser:
    """Parser SQL robusto com foco na resolução de aliases."""
    def __init__(self):
        self.patterns = {
            'select': r'SELECT\s+(.*?)(?=\s+FROM)',
            'from': r'FROM\s+([\w\.]+)(?:\s+(?:AS\s+)?([\w]+))?',
            'join': r'((?:INNER|LEFT|RIGHT|FULL)\s+)?JOIN\s+([\w\.]+)(?:\s+(?:AS\s+)?([\w]+))?\s+ON\s+([^\s]+(?:\s*[=<>!]+\s*[^\s]+)*)',
            'where': r'WHERE\s+(.*?)(?=\s+(?:GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)|$)',
            'order_by': r'ORDER\s+BY\s+(.*?)(?=\s+(?:LIMIT)|$)',
            'coalesce': r'COALESCE\s*\(([^)]+)\)',
            'case_when': r'CASE\s+.*?\s+END'
        }
    def parse(self, sql: str) -> ParsedSQL:
        sql_clean = re.sub(r'\s+', ' ', sql.strip())
        sql_clean = re.sub(r'\n', ' ', sql_clean)
        select_clause = self._extract_clause(sql_clean, 'select')
        from_match = re.search(self.patterns['from'], sql_clean, re.IGNORECASE)
        from_clause = ""
        table_aliases = {}
        if from_match:
            table_name = from_match.group(1)
            table_alias = from_match.group(2)
            from_clause = table_name
            if table_alias:
                table_aliases[table_alias] = table_name
        join_clauses = []
        join_matches = re.finditer(self.patterns['join'], sql_clean, re.IGNORECASE)
        for match in join_matches:
            join_type = (match.group(1) or "INNER").strip()
            join_table = match.group(2)
            join_alias = match.group(3)
            join_condition = match.group(4)
            if join_alias:
                table_aliases[join_alias] = join_table
            join_clauses.append(f"{join_type} JOIN {join_table} ON {join_condition}")
        where_clause = self._extract_clause(sql_clean, 'where')
        order_by_clause = self._extract_clause(sql_clean, 'order_by')
        return ParsedSQL(
            select_clause=select_clause,
            from_clause=from_clause,
            join_clauses=join_clauses,
            where_clause=where_clause,
            order_by_clause=order_by_clause,
            table_aliases=table_aliases,
            original_query=sql
        )
    def _extract_clause(self, sql: str, clause_type: str) -> str:
        pattern = self.patterns.get(clause_type, '')
        if not pattern:
            return ""
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()
        return ""


def parse_sql(sql):
    """Função de compatibilidade para manter interface existente."""
    parser = SQLParser()
    return parser.parse(sql)

if __name__ == "__main__":
    # Exemplo de uso
    sql = "SELECT nome, idade FROM clientes c WHERE c.idade > 30"
    parsed = parse_sql(sql)
    print(f"Parsed SQL: {parsed}")