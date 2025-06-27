"""
Tradutor SQL para PySpark

Ponto de entrada principal para a aplicação tradutora SQL para PySpark.
Este script demonstra a funcionalidade de tradução e fornece
uma interface interativa para testar consultas SQL.
"""

import sys
import os

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_translator import SQLTranslator
from src.translator.sql_translator_v2 import SQLTranslator as SQLTranslatorV2
from src.translator.sql_parser import SQLParser
from src.translator.utils import get_test_cases, format_translation_output, analyze_translation_results, print_analysis_summary
from src.examples.demo_examples import run_translation_demo, run_practical_demo, interactive_translator


def main():
    """Função principal para executar a aplicação tradutora SQL."""
    print("🚀 Aplicação Tradutora SQL para PySpark")
    print("=" * 50)
    
    while True:
        print("\nEscolha uma opção:")
        print("1. Executar demo de tradução com casos de teste")
        print("2. Executar demo prático com dados de exemplo")
        print("3. Tradutor interativo")
        print("4. Tradução rápida (consulta única)")
        print("5. Testar novo analisador SQL (independente)")
        print("6. Tradutor aprimorado com analisador")
        print("7. Validador de consultas interativo (detalhado)")
        print("8. Teste rápido do analisador")
        print("9. Demo dos recursos aprimorados")
        print("10. Executar suíte de testes do analisador aprimorado")
        print("11. Sair")
        
        choice = input("\nDigite sua escolha (1-11): ").strip()
        
        if choice == '1':
            run_translation_demo()
        
        elif choice == '2':
            try:
                run_practical_demo()
            except Exception as e:
                print(f"Erro ao executar demo prático: {e}")
                print("Nota: PySpark é necessário para o demo prático. Instale com: pip install pyspark")
        
        elif choice == '3':
            interactive_translator()
        
        elif choice == '4':
            quick_translation()
        
        elif choice == '5':
            test_sql_parser()
        
        elif choice == '6':
            test_enhanced_translator()
        
        elif choice == '7':
            try:
                import query_validator
                query_validator.main()
            except ImportError:
                print("Validador de consultas não encontrado. Certifique-se de que query_validator.py existe.")
            except Exception as e:
                print(f"Erro ao executar validador de consultas: {e}")
        
        elif choice == '8':
            try:
                import quick_parser_test
                quick_parser_test.main()
            except ImportError:
                print("Teste rápido do analisador não encontrado. Certifique-se de que quick_parser_test.py existe.")
            except Exception as e:
                print(f"Erro ao executar teste rápido do analisador: {e}")
        
        elif choice == '9':
            try:
                import demo_enhanced_features
                demo_enhanced_features.demo_enhanced_features()
            except ImportError:
                print("Demo de recursos aprimorados não encontrado. Certifique-se de que demo_enhanced_features.py existe.")
            except Exception as e:
                print(f"Erro ao executar demo de recursos aprimorados: {e}")
        
        elif choice == '10':
            try:
                import test_enhanced_parser
                test_enhanced_parser.main()
            except ImportError:
                print("Suíte de testes do analisador aprimorado não encontrada. Certifique-se de que test_enhanced_parser.py existe.")
            except Exception as e:
                print(f"Erro ao executar suíte de testes do analisador aprimorado: {e}")
        
        elif choice == '11':
            print("👋 Obrigado por usar o Tradutor SQL para PySpark!")
            break
        
        else:
            print("❌ Escolha inválida. Tente novamente.")


def quick_translation():
    """Tradução rápida de consulta única."""
    print("\n🔧 Tradução Rápida")
    print("-" * 30)
    
    sql_query = input("Digite sua consulta SQL: ").strip()
    if not sql_query:
        print("Nenhuma consulta fornecida.")
        return
    
    translator = SQLTranslator()
    pyspark_code = translator.translate(sql_query)
    
    print(f"\n📝 Consulta SQL:")
    print(sql_query)
    print(f"\n🔄 Tradução PySpark:")
    print(pyspark_code)


def test_sql_parser():
    """Testar o analisador SQL independente."""
    print("\n🔍 Teste do Analisador SQL")
    print("-" * 25)
    
    parser = SQLParser()
    
    while True:
        query = input("\nDigite a consulta SQL (ou 'sair' para retornar): ").strip()
        if query.lower() in ['sair', 'exit', 'quit', 'q']:
            break
        
        if not query:
            continue
        
        result = parser.parse(query)
        
        print(f"\n📊 Resultado da Análise:")
        print(f"Válido: {result.is_valid}")
        if result.errors:
            print(f"Erros: {result.errors}")
        
        print(f"Tabela: {result.from_table}")
        print(f"Colunas: {[str(col) for col in result.select_columns]}")
        print(f"JOINs: {len(result.join_clauses)}")
        print(f"WHERE: {len(result.where_conditions)}")
        print(f"GROUP BY: {len(result.group_by_columns)}")
        print(f"ORDER BY: {len(result.order_by_columns)}")
        print(f"Tem agregações: {result.has_aggregates}")


def test_enhanced_translator():
    """Testar o tradutor aprimorado com novo analisador."""
    print("\n🚀 Teste do Tradutor Aprimorado")
    print("-" * 30)
    
    translator = SQLTranslatorV2()
    
    while True:
        query = input("\nDigite a consulta SQL (ou 'sair' para retornar): ").strip()
        if query.lower() in ['sair', 'exit', 'quit', 'q']:
            break
        
        if not query:
            continue
        
        pyspark_code = translator.translate(query)
        
        print(f"\n📝 Consulta SQL:")
        print(query)
        print(f"\n🔄 Tradução PySpark Aprimorada:")
        print(pyspark_code)
    print("\n🔄 Quick Translation")
    print("-" * 30)
    
    query = input("Enter SQL query: ").strip()
    
    if not query:
        print("❌ Empty query provided.")
        return
    
    translator = SQLTranslator()
    result = translator.translate(query)
    
    print(format_translation_output(result))


def test_sql_parser():
    """Test the standalone SQL parser."""
    print("\n🔍 Testing SQL Parser")
    print("-" * 40)
    
    parser = SQLParser()
    
    query = input("Enter SQL query to parse: ").strip()
    
    if not query:
        print("❌ Empty query provided.")
        return
    
    print(f"\n📝 Parsing: {query}")
    print("-" * 60)
    
    parsed = parser.parse(query)
    
    if parsed.is_valid:
        print("✅ Parsing successful!")
        print(f"   📊 Table: {parsed.from_table}")
        print(f"   📋 Columns: {[str(col) for col in parsed.select_columns]}")
        
        if parsed.where_conditions:
            print(f"   🔍 WHERE: {[str(cond) for cond in parsed.where_conditions]}")
        
        if parsed.order_by_columns:
            print(f"   📈 ORDER BY: {[str(col) for col in parsed.order_by_columns]}")
        
        if parsed.limit_count:
            print(f"   🔢 LIMIT: {parsed.limit_count}")
    else:
        print("❌ Parsing failed!")
        print(f"   Errors: {parsed.errors}")


def test_enhanced_translator():
    """Test the enhanced translator with new parser."""
    print("\n🚀 Testing Enhanced Translator")
    print("-" * 40)
    
    query = input("Enter SQL query to translate: ").strip()
    
    if not query:
        print("❌ Empty query provided.")
        return
    
    print(f"\n📝 Query: {query}")
    print("-" * 60)
    
    # Use the enhanced translator
    translator = SQLTranslatorV2()
    result = translator.translate(query)
    
    print("🔥 Spark SQL:")
    print(f"   {result['spark_sql']}")
    
    if result['translation_available']:
        print("\n🐍 PySpark DataFrame API:")
        pyspark_lines = result['pyspark_code'].split('\n')
        for line in pyspark_lines:
            if line.strip():
                print(f"   {line}")
    else:
        print("\n❌ PySpark translation: Not available")
    
    # Show parsed structure
    if 'parsed_sql' in result:
        parsed = result['parsed_sql']
        print(f"\n📋 Parsed Structure:")
        print(f"   Valid: {parsed.is_valid}")
        print(f"   Table: {parsed.from_table}")
        print(f"   Columns: {len(parsed.select_columns)}")
        print(f"   WHERE conditions: {len(parsed.where_conditions)}")
        print(f"   ORDER BY columns: {len(parsed.order_by_columns)}")
        
        if parsed.errors:
            print(f"   Errors: {parsed.errors}")
    
    print(f"\n✅ Translation Status: {'Success' if result['translation_available'] else 'Partial'}")


def run_query_validator():
    """Run the interactive query validator."""
    print("\n🔍 Starting Interactive Query Validator...")
    print("This will help you verify if the parser correctly understands your SQL queries.")
    
    try:
        # Import and run the validator
        from query_validator import QueryValidator
        validator = QueryValidator()
        validator.run_interactive_validation()
    except ImportError:
        print("❌ Query validator module not found.")
        print("Make sure query_validator.py is in the project directory.")
    except Exception as e:
        print(f"❌ Error running query validator: {e}")


def run_quick_parser_test():
    """Run the quick parser test."""
    print("\n⚡ Starting Quick Parser Test...")
    print("This provides a fast way to test SQL query parsing.")
    
    try:
        # Import and run the quick test
        from quick_parser_test import main as quick_test_main
        quick_test_main()
    except ImportError:
        print("❌ Quick parser test module not found.")
        print("Make sure quick_parser_test.py is in the project directory.")
    except Exception as e:
        print(f"❌ Error running quick parser test: {e}")


if __name__ == "__main__":
    main()
