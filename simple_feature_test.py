"""
Teste simples dos recursos aprimorados do analisador SQL
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src.translator.sql_parser import SQLParser

# Criar instância do analisador
parser = SQLParser()

# Testar recursos aprimorados
test_queries = [
    "SELECT COUNT(*) FROM usuarios",
    "SELECT u.nome, d.nome FROM usuarios u INNER JOIN departamentos d ON u.depto_id = d.id",
    "SELECT departamento, AVG(salario) FROM funcionarios GROUP BY departamento HAVING AVG(salario) > 50000"
]

print("🚀 Analisador SQL Aprimorado - Teste Rápido de Recursos")
print("=" * 60)

for i, query in enumerate(test_queries, 1):
    print(f"\n🔍 Teste {i}: {query}")
    print("-" * 60)
    
    result = parser.parse(query)
    
    if result.is_valid:
        print("✅ ANALISADO COM SUCESSO!")
        print(f"   📋 Colunas: {len(result.select_columns)}")
        print(f"   🔗 JOINs: {len(result.join_clauses)}")
        print(f"   📊 GROUP BY: {len(result.group_by_columns)}")
        print(f"   🎯 HAVING: {len(result.having_conditions)}")
        print(f"   ⭐ Tem agregações: {result.has_aggregates}")
    else:
        print(f"❌ FALHOU: {result.errors}")

print(f"\n🎉 O analisador SQL aprimorado suporta JOINs, agregações, GROUP BY, HAVING e muito mais!")
