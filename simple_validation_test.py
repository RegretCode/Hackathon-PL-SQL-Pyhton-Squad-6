"""
Teste simples para verificar se os scripts de validação funcionam.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser


def simple_validation_test():
    """Teste simples da funcionalidade de validação."""
    print("🧪 Teste de Validação Simples")
    print("=" * 40)
    
    parser = SQLParser()
    
    # Testar uma consulta simples
    query = "SELECT nome, idade FROM usuarios WHERE idade > 18"
    
    print(f"Testando: {query}")
    print("-" * 40)
    
    parsed = parser.parse(query)
    
    if parsed.is_valid:
        print("✅ Análise bem-sucedida!")
        print(f"   Tabela: {parsed.from_table}")
        print(f"   Colunas: {len(parsed.select_columns)}")
        print(f"   Condições WHERE: {len(parsed.where_conditions)}")
        
        # Mostrar detalhes
        for col in parsed.select_columns:
            print(f"   Coluna: {col.name}")
        
        for cond in parsed.where_conditions:
            print(f"   Condição: {cond.column} {cond.operator} {cond.value}")
        
        print("\n🎯 Os scripts de validação estão prontos para uso!")
        print("   • query_validator.py - Validação detalhada interativa")
        print("   • quick_parser_test.py - Teste rápido")
        
    else:
        print("❌ Falha na análise!")
        print(f"   Erros: {parsed.errors}")


if __name__ == "__main__":
    simple_validation_test()
