"""
Exemplos de demonstração para tradução SQL para PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from ..translator.sql_translator import SQLTranslator
from ..translator.utils import get_test_cases, format_translation_output, analyze_translation_results, print_analysis_summary


def create_sample_data():
    """Criar DataFrames de exemplo para demonstração."""
    # Criar sessão Spark
    spark = SparkSession.builder.master("local[*]").appName("SQLTranslatorDemo").getOrCreate()
    
    # Dados de exemplo para funcionarios
    funcionarios_data = [
        {"nome": "Ana", "idade": 30, "salario": 70000, "departamento": "TI"},
        {"nome": "Bruno", "idade": 45, "salario": 90000, "departamento": "RH"},
        {"nome": "Carlos", "idade": 28, "salario": 50000, "departamento": "TI"},
        {"nome": "Diana", "idade": 35, "salario": 80000, "departamento": "Financeiro"},
        {"nome": "Eduardo", "idade": 40, "salario": 60000, "departamento": "TI"},
    ]
    funcionarios_df = spark.createDataFrame(funcionarios_data)
    funcionarios_df.createOrReplaceTempView("funcionarios")
    
    # Sample data for usuarios
    usuarios_data = [
        {"nome": "João", "idade": 25},
        {"nome": "Maria", "idade": 30},
        {"nome": "Pedro", "idade": 35},
        {"nome": "Ana", "idade": 20},
        {"nome": "Carlos", "idade": 45},
    ]
    usuarios_df = spark.createDataFrame(usuarios_data)
    usuarios_df.createOrReplaceTempView("usuarios")
    
    # Dados de exemplo para produtos
    produtos_data = [
        {"nome_produto": "Laptop", "preco": 2500.00, "categoria": "Eletrônicos"},
        {"nome_produto": "Mouse", "preco": 50.00, "categoria": "Eletrônicos"},
        {"nome_produto": "Cadeira", "preco": 300.00, "categoria": "Móveis"},
        {"nome_produto": "Mesa", "preco": 800.00, "categoria": "Móveis"},
        {"nome_produto": "Monitor", "preco": 400.00, "categoria": "Eletrônicos"},
    ]
    produtos_df = spark.createDataFrame(produtos_data)
    produtos_df.createOrReplaceTempView("produtos")
    
    return spark, funcionarios_df, usuarios_df, produtos_df


def run_translation_demo():
    """Executar uma demonstração abrangente do tradutor SQL."""
    print("🚀 Demo do Tradutor SQL para PySpark")
    print("=" * 50)
    
    # Inicializar tradutor
    translator = SQLTranslator()
    
    # Obter casos de teste
    test_cases = get_test_cases()
    
    print(f"📋 Executando {len(test_cases)} casos de teste...\n")
    
    # Traduzir todos os casos de teste
    results = []
    for i, test_case in enumerate(test_cases, 1):
        print(f"🔄 Processando Caso de Teste {i}: {test_case['description']}")
        result = translator.translate(test_case['sql'])
        results.append(result)
    
    print(f"\n✅ {len(results)} SQL queries translated successfully!\n")
    
    # Display results
    for result in results:
        print(format_translation_output(result))
        print()
    
    # Show analysis
    analysis = analyze_translation_results(results)
    print_analysis_summary(analysis)
    
    return results


def run_practical_demo():
    """Run a practical demonstration with actual DataFrames."""
    print("\n🧪 PRACTICAL DEMONSTRATION WITH DATAFRAMES")
    print("=" * 60)
    
    # Create sample data
    spark, funcionarios_df, usuarios_df, produtos_df = create_sample_data()
    
    print("📊 Sample Data Created:")
    print("\n--- Funcionários Table ---")
    funcionarios_df.show()
    
    print("\n--- Usuários Table ---")
    usuarios_df.show()
    
    print("\n--- Produtos Table ---")
    produtos_df.show()
    
    # Initialize translator
    translator = SQLTranslator()
    
    # Test query
    test_query = "SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC"
    
    print(f"\n🔍 Test Query: {test_query}")
    
    # Translate query
    result = translator.translate(test_query)
    
    print("\n--- Translation Results ---")
    print(format_translation_output(result))
    
    # Execute both versions
    print("\n--- Spark SQL Execution ---")
    spark_result = spark.sql(test_query)
    spark_result.show()
    
    print("\n--- PySpark DataFrame API Execution ---")
    # Execute the generated PySpark code
    pyspark_result = (funcionarios_df
                     .select(F.col('nome'), F.col('salario'))
                     .filter(F.col('departamento') == 'TI')
                     .orderBy(F.col('salario').desc()))
    pyspark_result.show()
    
    # Close Spark session
    spark.stop()
    
    return result


def interactive_translator():
    """Interactive translator for custom queries."""
    print("\n🎮 INTERACTIVE SQL TRANSLATOR")
    print("=" * 40)
    print("Enter SQL queries to translate (type 'exit' to quit):")
    
    translator = SQLTranslator()
    
    while True:
        try:
            query = input("\nSQL Query: ").strip()
            
            if query.lower() in ['exit', 'quit', 'q']:
                break
                
            if not query:
                continue
                
            result = translator.translate(query)
            print(format_translation_output(result))
            
        except KeyboardInterrupt:
            print("\n\nExiting interactive translator...")
            break
        except Exception as e:
            print(f"Error: {e}")
    
    print(f"\nTotal translations performed: {translator.get_translation_count()}")


if __name__ == "__main__":
    # Run demo
    run_translation_demo()
    
    # Run practical demo
    run_practical_demo()
    
    # Interactive translator
    interactive_translator()
