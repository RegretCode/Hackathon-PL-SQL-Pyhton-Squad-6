"""
Demo Final: Ferramentas de Validação de Consultas SQL

Este script demonstra todas as ferramentas de validação de consultas disponíveis no projeto.
"""

import sys
import os

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser


def demonstrate_validation_tools():
    """Demonstrar todas as ferramentas de validação com consultas de exemplo."""
    
    print("🎯 Demo das Ferramentas de Validação de Consultas SQL")
    print("=" * 60)
    print("Este projeto agora inclui ferramentas poderosas para validar análise SQL!")
    
    # Consultas de exemplo para testar
    test_queries = [
        "SELECT nome, idade FROM usuarios",
        "SELECT nome, salario as salary FROM funcionarios WHERE departamento = 'TI'",
        "SELECT * FROM produtos WHERE categoria IN ('Eletrônicos', 'Móveis') AND preco > 100 ORDER BY preco DESC LIMIT 10",
        "SELECT COUNT(*) FROM vendas WHERE data_venda >= '2023-01-01'"  # Isso pode falhar - bom caso de teste
    ]
    
    parser = SQLParser()
    
    print(f"\n📋 Testando {len(test_queries)} consultas de exemplo:")
    print("─" * 60)
    
    successful_parsing = 0
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n🧪 Consulta {i}: {query}")
        print("─" * 40)
        
        parsed = parser.parse(query)
        
        if parsed.is_valid:
            print("✅ Parser entendeu esta consulta corretamente!")
            print(f"   📊 Encontrado: tabela {parsed.from_table}, {len(parsed.select_columns)} colunas")
            if parsed.where_conditions:
                print(f"   🔍 WHERE: {len(parsed.where_conditions)} condições")
            if parsed.order_by_columns:
                print(f"   📈 ORDER BY: {len(parsed.order_by_columns)} colunas")
            if parsed.limit_count:
                print(f"   🔢 LIMIT: {parsed.limit_count}")
            successful_parsing += 1
        else:
            print("❌ Parser teve problemas com esta consulta")
            print(f"   Erros: {', '.join(parsed.errors)}")
        
        # Verificação rápida de validação
        print("   💡 Use ferramentas de validação para verificar se esta análise está correta!")
    
    # Resumo
    print(f"\n{'='*60}")
    print(f"📊 RESUMO DA ANÁLISE")
    print(f"   Analisadas com sucesso: {successful_parsing}/{len(test_queries)} consultas")
    print(f"   Taxa de sucesso: {successful_parsing/len(test_queries)*100:.1f}%")
    
    # Mostrar ferramentas disponíveis
    print(f"\n🛠️  FERRAMENTAS DE VALIDAÇÃO DISPONÍVEIS:")
    print(f"─" * 40)
    
    tools = [
        ("query_validator.py", "🔍 Validação detalhada interativa", 
         "Validação completa com confirmação do usuário e feedback"),
        ("quick_parser_test.py", "⚡ Teste rápido do parser", 
         "Forma rápida de testar análise de consultas"),
        ("main.py (opções 7-8)", "🎯 Validação integrada", 
         "Acesse ferramentas de validação do menu principal"),
        ("SQL Parser standalone", "🧪 Teste do parser", 
         "Teste o parser diretamente com python src/translator/sql_parser.py")
    ]
    
    for tool, title, description in tools:
        print(f"\n{title}")
        print(f"   📁 Arquivo: {tool}")
        print(f"   📖 Propósito: {description}")
    
    print(f"\n💡 COMO USAR ESSAS FERRAMENTAS:")
    print(f"─" * 40)
    print(f"1. 🔍 Para validação detalhada:")
    print(f"     python query_validator.py")
    print(f"   • Sessão interativa com feedback detalhado")
    print(f"   • Confirmação do usuário sobre precisão da análise")
    print(f"   • Resumo da sessão e estatísticas")
    
    print(f"\n2. ⚡ Para teste rápido:")
    print(f"     python quick_parser_test.py")
    print(f"   • Teste rápido de consultas")
    print(f"   • Resultados imediatos de análise")
    print(f"   • Validação simples sim/não")
    
    print(f"\n3. 🎯 Da aplicação principal:")
    print(f"     python main.py")
    print(f"   • Escolha opção 7 para validação detalhada")
    print(f"   • Escolha opção 8 para teste rápido")
    
    print(f"\n🎉 BENEFÍCIOS:")
    print(f"─" * 40)
    benefits = [
        "✅ Verificar se o parser entende corretamente suas consultas SQL",
        "✅ Identificar problemas de análise antes da tradução",
        "✅ Melhorar precisão do parser através de feedback",
        "✅ Testar consultas complexas com confiança",
        "✅ Depurar problemas de análise SQL facilmente"
    ]
    
    for benefit in benefits:
        print(f"   {benefit}")
    
    print(f"\n🚀 Pronto para validar suas consultas SQL!")
    print(f"Escolha a ferramenta de validação que melhor atende suas necessidades.")


if __name__ == "__main__":
    demonstrate_validation_tools()
