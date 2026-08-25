"""
Script de demonstração para mostrar as ferramentas de validação de consultas em ação.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser


def demo_parser_validator():
    """Demonstrar o validador de parser com consultas de exemplo."""
    print("🎯 Demo de Validação do Parser SQL")
    print("=" * 60)
    
    parser = SQLParser()
    
    # Consultas de teste com resultados esperados
    test_cases = [
        {
            "query": "SELECT nome, idade FROM usuarios",
            "expected": {
                "table": "usuarios",
                "columns": ["nome", "idade"],
                "where": [],
                "order_by": [],
                "limit": None
            }
        },
        {
            "query": "SELECT nome, salario as salary FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC LIMIT 5",
            "expected": {
                "table": "funcionarios", 
                "columns": ["nome", "salario AS salary"],
                "where": ["departamento = 'TI'"],
                "order_by": ["salario DESC"],
                "limit": 5
            }
        },
        {
            "query": "SELECT * FROM produtos WHERE categoria IN ('Eletrônicos', 'Móveis') AND preco > 100",
            "expected": {
                "table": "produtos",
                "columns": ["*"],
                "where": ["categoria IN ['Eletrônicos', 'Móveis']", "preco > 100"],
                "order_by": [],
                "limit": None
            }
        }
    ]
    
    for i, test in enumerate(test_cases, 1):
        print(f"\n🧪 Caso de Teste {i}:")
        print(f"{'─'*50}")
        print(f"Consulta: {test['query']}")
        
        parsed = parser.parse(test['query'])
        
        if not parsed.is_valid:
            print(f"❌ Falha na análise: {', '.join(parsed.errors)}")
            continue
        
        # Mostrar o que o parser encontrou
        print(f"\n📊 Resultados do Parser:")
        print(f"   Tabela: {parsed.from_table}")
        
        # Colunas
        actual_columns = []
        for col in parsed.select_columns:
            if col.is_wildcard:
                actual_columns.append("*")
            elif col.alias:
                actual_columns.append(f"{col.name} AS {col.alias}")
            else:
                actual_columns.append(col.name)
        print(f"   Colunas: {actual_columns}")
        
        # Condições WHERE
        actual_where = []
        for cond in parsed.where_conditions:
            if isinstance(cond.value, list):
                value = f"[{', '.join(map(str, cond.value))}]"
            else:
                value = f"'{cond.value}'" if isinstance(cond.value, str) else str(cond.value)
            actual_where.append(f"{cond.column} {cond.operator} {value}")
        print(f"   WHERE: {actual_where}")
        
        # ORDER BY
        actual_order = [f"{col.column} {col.direction}" for col in parsed.order_by_columns]
        print(f"   ORDER BY: {actual_order}")
        
        # LIMIT
        print(f"   LIMIT: {parsed.limit_count}")
        
        # Validação
        print(f"\n✅ Esperado vs Real:")
        expected = test['expected']
        
        # Verificar cada componente
        checks = [
            ("Tabela", expected['table'] == parsed.from_table),
            ("Colunas", expected['columns'] == actual_columns),
            ("WHERE", expected['where'] == actual_where),
            ("ORDER BY", expected['order_by'] == actual_order),
            ("LIMIT", expected['limit'] == parsed.limit_count)
        ]
        
        all_correct = True
        for component, is_correct in checks:
            status = "✅" if is_correct else "❌"
            print(f"   {status} {component}: {'Correto' if is_correct else 'Divergência'}")
            if not is_correct:
                all_correct = False
        
        overall = "✅ PARSER ENTENDEU CORRETAMENTE A CONSULTA" if all_correct else "❌ PARSER TEVE ALGUNS PROBLEMAS"
        print(f"\n{overall}")
    
    print(f"\n{'='*60}")
    print(f"💡 Como usar as ferramentas de validação:")
    print(f"   • Execute 'python query_validator.py' para validação detalhada interativa")
    print(f"   • Execute 'python quick_parser_test.py' para teste rápido")
    print(f"   • Ambas as ferramentas ajudam a verificar se o parser entende seu SQL corretamente")


if __name__ == "__main__":
    demo_parser_validator()
