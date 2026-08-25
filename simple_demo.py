"""
Script de demonstração simples para testar o tradutor SQL sem dependências PySpark.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_translator import SQLTranslator
from src.translator.utils import get_test_cases, format_translation_output


def simple_demo():
    """Executar uma demonstração simples do tradutor."""
    print("🚀 Tradutor SQL para PySpark - Demo Simples")
    print("=" * 50)
    
    # Inicializar tradutor
    translator = SQLTranslator()
    
    # Casos de teste para demonstrar
    demo_queries = [
        "SELECT nome, idade FROM usuarios",
        "SELECT nome FROM usuarios WHERE idade > 18",
        "SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC",
        "SELECT nome, idade as user_age FROM usuarios WHERE idade > 18 ORDER BY idade DESC LIMIT 5"
    ]
    
    print(f"📋 Demonstrando {len(demo_queries)} traduções SQL...\n")
    
    for i, sql_query in enumerate(demo_queries, 1):
        print(f"🔄 Exemplo {i}:")
        print(f"   SQL: {sql_query}")
        
        result = translator.translate(sql_query)
        
        print(f"   🔥 Spark SQL: {result['spark_sql']}")
        
        if result['translation_available']:
            print(f"   🐍 PySpark:")
            # Dividir código PySpark para melhor exibição
            pyspark_lines = result['pyspark_code'].split('\n')
            for line in pyspark_lines:
                if line.strip():
                    print(f"      {line}")
        else:
            print("   ❌ PySpark: Não disponível")
        
        print(f"   ✅ Status: {'Sucesso' if result['translation_available'] else 'Parcial'}")
        print("-" * 80)
    
    print(f"\n📊 Total de traduções realizadas: {translator.get_translation_count()}")
    
    # Mostrar algumas estatísticas
    successful = sum(1 for q in demo_queries if translator.translate(q)['translation_available'])
    print(f"📈 Taxa de sucesso: {successful}/{len(demo_queries)} ({successful/len(demo_queries)*100:.1f}%)")


if __name__ == "__main__":
    simple_demo()
