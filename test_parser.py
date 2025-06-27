"""
Script de teste para o novo módulo do Parser SQL.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser
from src.translator.sql_translator_v2 import SQLTranslator


def test_parser_standalone():
    """Testar o parser SQL standalone."""
    print("🔍 Testando Parser SQL Standalone")
    print("=" * 50)
    
    parser = SQLParser()
    
    test_queries = [
        "SELECT nome, idade FROM usuarios",
        "SELECT nome, idade as user_age FROM usuarios WHERE idade > 18",
        "SELECT * FROM funcionarios WHERE departamento = 'TI' AND salario >= 50000 ORDER BY salario DESC LIMIT 5",
        "SELECT produto, preco FROM produtos WHERE categoria IN ('Eletrônicos', 'Informática') ORDER BY preco ASC"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📝 Teste {i}: {query}")
        print("-" * 60)
        
        parsed = parser.parse(query)
        
        if parsed.is_valid:
            print("✅ Análise bem-sucedida!")
            print(f"   📊 Tabela: {parsed.from_table}")
            print(f"   📋 Colunas: {[str(col) for col in parsed.select_columns]}")
            
            if parsed.where_conditions:
                print(f"   🔍 WHERE: {[str(cond) for cond in parsed.where_conditions]}")
            
            if parsed.order_by_columns:
                print(f"   📈 ORDER BY: {[str(col) for col in parsed.order_by_columns]}")
            
            if parsed.limit_count:
                print(f"   🔢 LIMIT: {parsed.limit_count}")
        else:
            print("❌ Falha na análise!")
            print(f"   Erros: {parsed.errors}")
    
    # Mostrar capacidades do parser
    print(f"\n🔧 Informações do Parser:")
    info = parser.get_parser_info()
    for key, value in info.items():
        print(f"   {key}: {value}")


def test_enhanced_translator():
    """Testar o tradutor aprimorado com parser."""
    print("\n\n🚀 Testando Tradutor SQL Aprimorado")
    print("=" * 50)
    
    translator = SQLTranslator()
    
    test_query = "SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC LIMIT 3"
    
    print(f"📝 Consulta: {test_query}")
    print("-" * 60)
    
    # Testar tradução
    result = translator.translate(test_query)
    
    print("🔥 Spark SQL:")
    print(f"   {result['spark_sql']}")
    
    if result['translation_available']:
        print("\n🐍 API DataFrame PySpark:")
        pyspark_lines = result['pyspark_code'].split('\n')
        for line in pyspark_lines:
            if line.strip():
                print(f"   {line}")
    
    print(f"\n📊 Status da Tradução: {'Sucesso' if result['translation_available'] else 'Falha'}")
    
    # Mostrar estrutura analisada
    if 'parsed_sql' in result:
        parsed = result['parsed_sql']
        print(f"\n📋 Estrutura Analisada:")
        print(f"   Válido: {parsed.is_valid}")
        print(f"   Tabela: {parsed.from_table}")
        print(f"   Colunas: {len(parsed.select_columns)}")
        print(f"   Condições: {len(parsed.where_conditions)}")
        print(f"   Order By: {len(parsed.order_by_columns)}")
        
    # Testar funcionalidade apenas análise
    print(f"\n🔍 Análise Apenas:")
    parse_result = translator.parse_only(test_query)
    for key, value in parse_result.items():
        if key != 'original_sql':
            print(f"   {key}: {value}")


def main():
    """Executar todos os testes."""
    test_parser_standalone()
    test_enhanced_translator()
    
    print("\n\n🎉 Todos os testes concluídos!")
    print("\n💡 O novo módulo parser fornece:")
    print("   ✅ Análise SQL estruturada com dataclasses")
    print("   ✅ Melhor tratamento de erro e validação")
    print("   ✅ Análise detalhada de componentes")
    print("   ✅ Capacidades de tradução aprimoradas")
    print("   ✅ Compatibilidade com versões anteriores com código existente")


if __name__ == "__main__":
    main()
