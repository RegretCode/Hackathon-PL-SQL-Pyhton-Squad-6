"""
Demo dos Recursos Aprimorados do Analisador SQL

Este script demonstra as capacidades aprimoradas do analisador SQL incluindo:
- JOINs INNER, LEFT, RIGHT
- Funções de agregação (COUNT, SUM, AVG, MAX, MIN)
- Cláusulas GROUP BY e HAVING
- Condições WHERE complexas
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from translator.sql_parser import SQLParser


def demo_enhanced_features():
    """Demonstrar recursos aprimorados do analisador SQL."""
    parser = SQLParser()
    
    demo_queries = [
        # Consultas básicas
        ("SELECT simples", "SELECT nome, idade FROM usuarios"),
        
        # Consultas JOIN
        ("INNER JOIN", "SELECT u.nome, d.nome FROM usuarios u INNER JOIN departamentos d ON u.depto_id = d.id"),
        ("LEFT JOIN com WHERE", "SELECT c.nome, p.titulo FROM clientes c LEFT JOIN pedidos p ON c.id = p.cliente_id WHERE c.ativo = true"),
        ("Múltiplos JOINs", """SELECT u.nome, d.nome, c.nome 
                             FROM usuarios u 
                             INNER JOIN departamentos d ON u.depto_id = d.id 
                             LEFT JOIN cidades c ON u.cidade_id = c.id"""),
        
        # Funções de agregação
        ("Função COUNT", "SELECT COUNT(*) AS total FROM usuarios"),
        ("SUM e AVG", "SELECT SUM(salario) AS total, AVG(salario) AS media FROM funcionarios"),
        ("MAX e MIN", "SELECT MAX(idade) AS mais_velho, MIN(idade) AS mais_novo FROM usuarios"),
        
        # GROUP BY e HAVING
        ("GROUP BY", "SELECT departamento, COUNT(*) AS total FROM funcionarios GROUP BY departamento"),
        ("GROUP BY com HAVING", "SELECT departamento, COUNT(*) AS total FROM funcionarios GROUP BY departamento HAVING COUNT(*) > 5"),
        ("Múltiplas colunas GROUP BY", "SELECT departamento, cidade, AVG(salario) FROM funcionarios GROUP BY departamento, cidade"),
        
        # Consultas complexas
        ("Consulta complexa com tudo", """
            SELECT d.nome, COUNT(f.id) AS total_funcionarios, AVG(f.salario) AS salario_medio
            FROM departamentos d
            LEFT JOIN funcionarios f ON d.id = f.depto_id
            WHERE f.ativo = true
            GROUP BY d.nome
            HAVING COUNT(f.id) > 2
            ORDER BY salario_medio DESC
            LIMIT 5
        """),
        
        # Várias condições WHERE
        ("Múltiplas condições WHERE", "SELECT nome FROM usuarios WHERE idade > 18 AND departamento = 'TI' AND ativo = true"),
        ("Cláusula IN", "SELECT nome FROM produtos WHERE categoria_id IN (1, 2, 3, 4)"),
        ("Cláusula LIKE", "SELECT nome FROM produtos WHERE nome LIKE '%smartphone%'"),
        ("IS NULL", "SELECT nome FROM usuarios WHERE telefone IS NOT NULL"),
    ]
    
    print("🚀 Demo do Analisador SQL Aprimorado")
    print("=" * 80)
    print("Este demo mostra a capacidade do analisador de entender recursos SQL complexos:")
    print("✓ JOINs (INNER, LEFT, RIGHT)")
    print("✓ Funções de agregação (COUNT, SUM, AVG, MAX, MIN)")
    print("✓ Cláusulas GROUP BY e HAVING")
    print("✓ Condições WHERE complexas")
    print("✓ ORDER BY e LIMIT")
    print("=" * 80)
    
    for i, (name, query) in enumerate(demo_queries, 1):
        print(f"\n🔍 Demo {i}: {name}")
        print(f"Consulta: {query.strip()}")
        print("-" * 80)
        
        result = parser.parse(query)
        
        if not result.is_valid:
            print(f"❌ Análise falhou: {', '.join(result.errors)}")
            continue
        
        print("✅ Análise bem-sucedida!")
        print(f"📊 Componentes Analisados:")
        print(f"   Tabela FROM: {result.from_table}")
        
        # Mostrar colunas SELECT com funções de agregação destacadas
        if result.select_columns:
            print(f"   Colunas SELECT: {len(result.select_columns)}")
            for col in result.select_columns:
                if col.is_aggregate:
                    display = f"🔢 {col.aggregate_function}({col.name})"
                    if col.alias:
                        display += f" AS {col.alias}"
                elif col.is_wildcard:
                    display = "📋 *"
                else:
                    display = f"📋 {col.name}"
                    if col.alias:
                        display += f" AS {col.alias}"
                print(f"      - {display}")
        
        # Mostrar JOINs
        if result.join_clauses:
            print(f"   Cláusulas JOIN: {len(result.join_clauses)}")
            for join in result.join_clauses:
                print(f"      - 🔗 {join.join_type} {join.table} ON {join.condition}")
        
        # Mostrar condições WHERE
        if result.where_conditions:
            print(f"   Condições WHERE: {len(result.where_conditions)}")
            for cond in result.where_conditions:
                print(f"      - 🔍 {cond.column} {cond.operator} {cond.value}")
        
        # Mostrar GROUP BY
        if result.group_by_columns:
            group_cols = [col.column for col in result.group_by_columns]
            print(f"   📊 GROUP BY: {', '.join(group_cols)}")
        
        # Mostrar HAVING
        if result.having_conditions:
            print(f"   Condições HAVING: {len(result.having_conditions)}")
            for cond in result.having_conditions:
                print(f"      - 🎯 {cond.column} {cond.operator} {cond.value}")
        
        # Mostrar ORDER BY
        if result.order_by_columns:
            order_cols = [f"{col.column} {col.direction}" for col in result.order_by_columns]
            print(f"   📈 ORDER BY: {', '.join(order_cols)}")
        
        # Mostrar LIMIT
        if result.limit_count:
            print(f"   🔢 LIMIT: {result.limit_count}")
        
        # Mostrar flag de agregação
        if result.has_aggregates:
            print(f"   ⭐ Contém funções de agregação")
    
    print(f"\n🎉 Demo concluído! O analisador SQL aprimorado analisou com sucesso todas as consultas.")


if __name__ == "__main__":
    demo_enhanced_features()
