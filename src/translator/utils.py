"""
Funções utilitá    output.append("🧪 TRADUÇÃO #{result['translation_id']}")
    output.append("=" * 80)
    
    output.append(f"\n📝 SQL Original:")
    output.append(f"   {result['original_sql']}")
    
    output.append(f"\n🔥 Spark SQL:")
    output.append(f"   {result['spark_sql']}")
    
    if result['translation_available']:
        output.append(f"\n🐍 API PySpark DataFrame:")
        pyspark_lines = result['pyspark_code'].split('\n')
        for line in pyspark_lines:
            if line.strip():
                output.append(f"   {line}")
    else:
        output.append(f"\n❌ API PySpark DataFrame: Não disponível (consulta complexa)")
    
    status = 'Sucesso' if result['translation_available'] else 'Parcial (apenas Spark SQL)'
    output.append(f"\n✅ Status da Tradução: {status}") formatação SQL.
"""

import textwrap
from typing import List, Dict, Any


def format_translation_output(result: dict) -> str:
    """
    Formata os resultados da tradução para exibição.
    
    Args:
        result (dict): Resultado da tradução do SQLTranslator
        
    Returns:
        str: String de saída formatada
    """
    output = []
    output.append("=" * 80)
    output.append(f"🧪 TRADUÇÃO #{result['translation_id']}")
    output.append("=" * 80)
    
    output.append(f"\n📝 SQL Original:")
    output.append(f"   {result['original_sql']}")
    
    output.append(f"\n🔥 Spark SQL:")
    output.append(f"   {result['spark_sql']}")
    
    if result['translation_available']:
        output.append(f"\n🐍 API PySpark DataFrame:")
        pyspark_lines = result['pyspark_code'].split('\n')
        for line in pyspark_lines:
            if line.strip():
                output.append(f"   {line}")
    else:
        output.append(f"\n❌ API PySpark DataFrame: Não disponível (consulta complexa)")
    
    status = 'Sucesso' if result['translation_available'] else 'Parcial (apenas Spark SQL)'
    output.append(f"\n✅ Status da Tradução: {status}")
    
    return '\n'.join(output)


def create_sample_dataframe_code(table_name: str, sample_data: List[Dict[str, Any]]) -> str:
    """
    Gera código para criar um DataFrame de exemplo para testes.
    
    Args:
        table_name (str): Nome da tabela/DataFrame
        sample_data (List[Dict[str, Any]]): Dados de exemplo para o DataFrame
        
    Returns:
        str: String de código para criar o DataFrame
    """
    code_lines = [
        "from pyspark.sql import SparkSession",
        "from pyspark.sql import functions as F",
        "",
        "# Criar sessão Spark",
        'spark = SparkSession.builder.master("local[*]").appName("SQLTranslator").getOrCreate()',
        "",
        f"# Criar dados de exemplo para {table_name}",
        f"{table_name}_data = {sample_data}",
        f"{table_name}_df = spark.createDataFrame({table_name}_data)",
        f'{table_name}_df.createOrReplaceTempView("{table_name}")',
        "",
        f"# Exibir o DataFrame",
        f"{table_name}_df.show()"
    ]
    
    return '\n'.join(code_lines)


def get_test_cases() -> List[Dict[str, str]]:
    """
    Obtém casos de teste predefinidos para tradução SQL.
    
    Returns:
        List[Dict[str, str]]: Lista de casos de teste com descrição e SQL
    """
    return [
        {
            "description": "SELECT básico - Seleção simples de colunas",
            "sql": "SELECT nome, idade FROM usuarios"
        },
        {
            "description": "WHERE com comparação numérica",
            "sql": "SELECT nome, idade FROM usuarios WHERE idade > 18"
        },
        {
            "description": "WHERE com comparação de string",
            "sql": "SELECT nome, departamento FROM funcionarios WHERE departamento = 'TI'"
        },
        {
            "description": "ORDER BY ascendente (padrão)",
            "sql": "SELECT nome, salario FROM funcionarios ORDER BY salario"
        },
        {
            "description": "ORDER BY descendente",
            "sql": "SELECT nome, idade FROM usuarios WHERE idade > 18 ORDER BY idade DESC"
        },
        {
            "description": "Combinação de WHERE, ORDER BY e LIMIT",
            "sql": "SELECT nome, idade FROM usuarios WHERE idade > 18 ORDER BY idade DESC LIMIT 10"
        },
        {
            "description": "Aliases de coluna",
            "sql": "SELECT nome, idade as idade_usuario FROM usuarios WHERE idade > 18"
        },
        {
            "description": "SELECT todas as colunas",
            "sql": "SELECT * FROM produtos ORDER BY preco"
        },
        {
            "description": "WHERE complexo com condição AND",
            "sql": "SELECT nome, salario FROM funcionarios WHERE salario >= 50000 AND departamento = 'TI'"
        },
        {
            "description": "Múltiplas colunas em ORDER BY",
            "sql": "SELECT nome, idade, salario FROM funcionarios ORDER BY departamento, salario DESC"
        }
    ]


def analyze_translation_results(results: List[dict]) -> dict:
    """
    Analisa os resultados das traduções e fornece estatísticas.
    
    Args:
        results (List[dict]): Lista de resultados de tradução
        
    Returns:
        dict: Estatísticas da análise
    """
    total_translations = len(results)
    successful_translations = sum(1 for r in results if r['translation_available'])
    success_rate = (successful_translations / total_translations) * 100 if total_translations > 0 else 0
    
    return {
        'total_translations': total_translations,
        'successful_translations': successful_translations,
        'success_rate': success_rate,
        'partial_translations': total_translations - successful_translations
    }


def print_analysis_summary(analysis: dict):
    """
    Imprime um resumo formatado da análise.
    
    Args:
        analysis (dict): Resultados da análise de analyze_translation_results
    """
    print("📊 RESUMO DA TRADUÇÃO")
    print("=" * 40)
    print(f"Total de Casos de Teste: {analysis['total_translations']}")
    print(f"Traduções PySpark Bem-sucedidas: {analysis['successful_translations']}")
    print(f"Taxa de Sucesso: {analysis['success_rate']:.1f}%")
    print(f"Traduções Parciais (apenas Spark SQL): {analysis['partial_translations']}")
    print()
    
    print("🔧 FUNCIONALIDADES SQL SUPORTADAS:")
    features = [
        "SELECT com seleção de colunas",
        "Cláusulas WHERE com vários operadores",
        "ORDER BY com ASC/DESC",
        "Cláusulas LIMIT",
        "Aliases de coluna (AS)",
        "SELECT * (todas as colunas)",
        "Condições complexas com AND/OR"
    ]
    
    for feature in features:
        print(f"✅ {feature}")
    
    print()
    print("💡 RECOMENDAÇÕES DE USO:")
    recommendations = [
        "Use o código PySpark gerado como ponto de partida",
        "Teste com o esquema específico do seu DataFrame",
        "Ajuste nomes de colunas e tipos de dados conforme necessário",
        "Considere implicações de performance para grandes datasets"
    ]
    
    for rec in recommendations:
        print(f"• {rec}")
