"""
Testador Rápido do Analisador de Consultas SQL

Um script simples para testar rapidamente se o analisador entende corretamente suas consultas SQL.
"""

import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser


def quick_test_query(sql_query: str):
    """Teste rápido de uma única consulta SQL."""
    parser = SQLParser()
    
    print(f"🔍 Testando Consulta: {sql_query}")
    print(f"{'─'*80}")
    
    parsed = parser.parse(sql_query)
    
    if not parsed.is_valid:
        print(f"❌ ANÁLISE FALHOU!")
        print(f"   Erros: {', '.join(parsed.errors)}")
        return False
    
    print(f"✅ ANÁLISE BEM-SUCEDIDA!")
    print(f"\n📊 O que o analisador entendeu:")
    
    # Mostrar componentes
    print(f"   🗃️  Tabela: {parsed.from_table}")
    
    if parsed.select_columns:
        cols = []
        for col in parsed.select_columns:
            if col.is_wildcard:
                cols.append("*")
            elif col.is_aggregate:
                if col.alias:
                    cols.append(f"{col.aggregate_function}({col.name}) AS {col.alias}")
                else:
                    cols.append(f"{col.aggregate_function}({col.name})")
            elif col.alias:
                cols.append(f"{col.name} AS {col.alias}")
            else:
                cols.append(col.name)
        print(f"   📋 Colunas: {', '.join(cols)}")
    
    if parsed.join_clauses:
        joins = []
        for join in parsed.join_clauses:
            joins.append(f"{join.join_type} {join.table} ON {join.condition}")
        print(f"   🔗 JOINs: {', '.join(joins)}")
    
    if parsed.where_conditions:
        conditions = []
        for cond in parsed.where_conditions:
            if isinstance(cond.value, list):
                value = f"[{', '.join(map(str, cond.value))}]"
            else:
                value = f"'{cond.value}'" if isinstance(cond.value, str) else str(cond.value)
            conditions.append(f"{cond.column} {cond.operator} {value}")
            if cond.logical_operator:
                conditions.append(cond.logical_operator)
        print(f"   🔍 WHERE: {' '.join(conditions)}")
    
    if parsed.group_by_columns:
        group_by = [col.column for col in parsed.group_by_columns]
        print(f"   📊 GROUP BY: {', '.join(group_by)}")
    
    if parsed.having_conditions:
        having = []
        for cond in parsed.having_conditions:
            having.append(f"{cond.column} {cond.operator} {cond.value}")
        print(f"   🎯 HAVING: {', '.join(having)}")
    
    if parsed.order_by_columns:
        order_by = [f"{col.column} {col.direction}" for col in parsed.order_by_columns]
        print(f"   📈 ORDER BY: {', '.join(order_by)}")
    
    if parsed.limit_count:
        print(f"   🔢 LIMIT: {parsed.limit_count}")
    
    if parsed.has_aggregates:
        print(f"   ⭐ Contém funções de agregação")
    
    return True


def main():
    """Função principal para testes rápidos."""
    print(f"🚀 Testador Rápido do Analisador SQL")
    print(f"{'='*50}")
    print(f"Digite suas consultas SQL para ver como o analisador as entende.")
    print(f"Digite 'sair' para encerrar.")
    
    while True:
        print(f"\n{'─'*50}")
        query = input(f"Digite a consulta SQL: ").strip()
        
        if not query:
            continue
            
        if query.lower() in ['sair', 'exit', 'quit', 'q']:
            print(f"👋 Até logo!")
            break
        
        print()
        success = quick_test_query(query)
        
        if success:
            print(f"\n✅ Isso corresponde ao que você pretendia? (O analisador entendeu sua consulta)")
        else:
            print(f"\n❌ O analisador não conseguiu entender sua consulta corretamente.")


if __name__ == "__main__":
    main()
