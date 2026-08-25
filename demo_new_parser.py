"""
Teste rápido da aplicação principal atualizada com novas opções de parser.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser
from src.translator.sql_translator_v2 import SQLTranslator


def demo_new_features():
    """Demonstrar as novas funcionalidades do parser."""
    print("🎯 Demonstrando Novas Funcionalidades do Parser SQL")
    print("=" * 50)
    
    # Testar parser standalone
    print("\n1️⃣ Demo do Parser SQL Standalone:")
    print("-" * 40)
    
    parser = SQLParser()
    test_query = "SELECT nome, salario as salary FROM funcionarios WHERE departamento = 'TI' AND salario > 50000 ORDER BY salario DESC LIMIT 10"
    
    print(f"Consulta: {test_query}")
    
    parsed = parser.parse(test_query)
    print(f"\n📊 Resultados do Parser:")
    print(f"   ✅ Válido: {parsed.is_valid}")
    print(f"   📋 Tabela: {parsed.from_table}")
    print(f"   🔍 Colunas: {[str(col) for col in parsed.select_columns]}")
    print(f"   🎯 WHERE: {[str(cond) for cond in parsed.where_conditions]}")
    print(f"   📈 ORDER BY: {[str(col) for col in parsed.order_by_columns]}")
    print(f"   🔢 LIMIT: {parsed.limit_count}")
    
    # Testar tradutor aprimorado
    print("\n\n2️⃣ Demo do Tradutor Aprimorado:")
    print("-" * 40)
    
    translator = SQLTranslator()
    result = translator.translate(test_query)
    
    print(f"🔥 Spark SQL:")
    print(f"   {result['spark_sql']}")
    
    if result['translation_available']:
        print(f"\n🐍 API DataFrame PySpark:")
        pyspark_lines = result['pyspark_code'].split('\n')
        for line in pyspark_lines:
            if line.strip():
                print(f"   {line}")
    
    print(f"\n✅ Tradução: {'Sucesso' if result['translation_available'] else 'Falha'}")
    
    # Mostrar comparação
    print(f"\n\n3️⃣ Capacidades do Parser:")
    print("-" * 40)
    info = parser.get_parser_info()
    for key, value in info.items():
        print(f"   {key}: {value}")
    
    print(f"\n🎉 Resumo das Novas Funcionalidades:")
    print(f"   ✅ Análise estruturada com dataclasses")
    print(f"   ✅ Tratamento de erro aprimorado")
    print(f"   ✅ Análise detalhada de componentes") 
    print(f"   ✅ Melhor análise de cláusulas WHERE")
    print(f"   ✅ Melhoria no tratamento de aliases de colunas")
    print(f"   ✅ Compatibilidade com versões anteriores mantida")


if __name__ == "__main__":
    demo_new_features()
