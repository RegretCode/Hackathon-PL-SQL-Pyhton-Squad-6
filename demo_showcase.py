"""
Demo interativo da funcionalidade principal da aplicação.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_translator import SQLTranslator
from src.translator.utils import format_translation_output


def interactive_demo():
    """Mostrar a funcionalidade principal da aplicação."""
    print("🚀 Tradutor SQL para PySpark - Demo Interativo")
    print("=" * 55)
    
    translator = SQLTranslator()
    
    # Consultas de demonstração
    demo_queries = [
        "SELECT nome, idade FROM usuarios WHERE idade > 25",
        "SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC LIMIT 3",
        "SELECT produto, preco as valor FROM produtos WHERE categoria = 'Eletrônicos'"
    ]
    
    print("🎯 Demonstrando várias traduções de consultas SQL:\n")
    
    for i, query in enumerate(demo_queries, 1):
        print(f"📝 Consulta Demo {i}:")
        print(f"   {query}")
        print()
        
        result = translator.translate(query)
        
        print("🔥 Saída Spark SQL:")
        print(f"   {result['spark_sql']}")
        print()
        
        if result['translation_available']:
            print("🐍 Saída API DataFrame PySpark:")
            pyspark_lines = result['pyspark_code'].split('\n')
            for line in pyspark_lines:
                if line.strip():
                    print(f"   {line}")
        else:
            print("❌ API DataFrame PySpark: Não disponível para esta consulta")
        
        print(f"\n✅ Status da Tradução: {'Sucesso' if result['translation_available'] else 'Parcial'}")
        print("\n" + "-" * 70 + "\n")
    
    print(f"📊 Resumo: {translator.get_translation_count()} consultas traduzidas com sucesso!")
    print("\n🎉 O tradutor SQL para PySpark está funcionando perfeitamente!")
    print("\n💡 Dicas de uso:")
    print("   • Use 'python main.py' para executar a aplicação interativa completa")
    print("   • Importe a classe SQLTranslator para uso programático")
    print("   • Verifique o README.md para documentação completa")


if __name__ == "__main__":
    interactive_demo()
