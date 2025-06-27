"""
CLI para SQL to Spark Transformer
Uso corporativo em lote e automação
"""
import argparse
import os
import sys
from pathlib import Path
import json

from .parser import parse_sql
from .converter_pyspark import convert_to_pyspark
from .converter_sparksql import convert_to_sparksql
from .dialect_converter import DialectConverter

def process_file(input_file, output_format, output_dir=None):
    """Processa um arquivo SQL individual"""
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # Detectar dialeto
        dialect = DialectConverter.detect_dialect(sql_content)
        print(f"Dialeto detectado: {dialect}")
        
        # Converter dialeto se necessário
        if dialect in ['oracle', 'postgresql']:
            sql_content = DialectConverter.convert_to_spark_compatible(sql_content)
            print("SQL convertido para compatibilidade Spark")
        
        # Parse SQL
        parsed = parse_sql(sql_content)
        
        # Gerar saída
        input_path = Path(input_file)
        base_name = input_path.stem
        
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
        else:
            output_path = input_path.parent
        
        results = {}
        
        if output_format in ['pyspark', 'both']:
            pyspark_code = convert_to_pyspark(parsed)
            pyspark_file = output_path / f"{base_name}_pyspark.py"
            
            with open(pyspark_file, 'w', encoding='utf-8') as f:
                f.write("# Código PySpark gerado automaticamente\n")
                f.write(f"# Arquivo original: {input_file}\n")
                f.write(f"# Dialeto detectado: {dialect}\n\n")
                f.write("from pyspark.sql import SparkSession\n")
                f.write("from pyspark.sql.functions import *\n\n")
                f.write("# Inicializar Spark\n")
                f.write("spark = SparkSession.builder.appName('SQLTransformer').getOrCreate()\n\n")
                f.write("# Código convertido\n")
                f.write(pyspark_code)
            
            results['pyspark'] = str(pyspark_file)
            print(f"PySpark gerado: {pyspark_file}")
        
        if output_format in ['sparksql', 'both']:
            sparksql_code = convert_to_sparksql(parsed)
            sparksql_file = output_path / f"{base_name}_sparksql.sql"
            
            with open(sparksql_file, 'w', encoding='utf-8') as f:
                f.write(f"-- SparkSQL gerado automaticamente\n")
                f.write(f"-- Arquivo original: {input_file}\n")
                f.write(f"-- Dialeto detectado: {dialect}\n\n")
                f.write(sparksql_code)
            
            results['sparksql'] = str(sparksql_file)
            print(f"SparkSQL gerado: {sparksql_file}")
        
        return results
        
    except Exception as e:
        print(f"Erro processando {input_file}: {str(e)}")
        return None

def process_directory(input_dir, output_format, output_dir=None):
    """Processa todos os arquivos SQL em um diretório"""
    input_path = Path(input_dir)
    sql_files = list(input_path.glob("*.sql")) + list(input_path.glob("*.SQL"))
    
    if not sql_files:
        print(f"Nenhum arquivo SQL encontrado em {input_dir}")
        return
    
    print(f"Encontrados {len(sql_files)} arquivos SQL")
    
    results = []
    for sql_file in sql_files:
        print(f"\nProcessando: {sql_file}")
        result = process_file(sql_file, output_format, output_dir)
        if result:
            results.append({
                'input': str(sql_file),
                'outputs': result
            })
    
    # Gerar relatório
    if output_dir:
        report_path = Path(output_dir) / "conversion_report.json"
    else:
        report_path = input_path / "conversion_report.json"
    
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nRelatório gerado: {report_path}")
    print(f"Total processado: {len(results)} arquivos")

def main():
    parser = argparse.ArgumentParser(
        description="SQL to Spark Transformer CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  %(prog)s -f query.sql -o pyspark
  %(prog)s -d /path/to/sql/files -o both -od /output/dir
  %(prog)s -f legacy_query.sql -o sparksql --validate
        """
    )
    
    # Argumentos principais
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file', help='Arquivo SQL para converter')
    group.add_argument('-d', '--directory', help='Diretório com arquivos SQL')
    
    parser.add_argument('-o', '--output', 
                       choices=['pyspark', 'sparksql', 'both'],
                       default='both',
                       help='Formato de saída (padrão: both)')
    
    parser.add_argument('-od', '--output-dir',
                       help='Diretório de saída (padrão: mesmo do arquivo de entrada)')
    
    parser.add_argument('--validate', action='store_true',
                       help='Apenas validar SQL sem gerar saída')
    
    parser.add_argument('--dialect', 
                       choices=['auto', 'oracle', 'postgresql', 'standard'],
                       default='auto',
                       help='Forçar dialeto específico (padrão: auto-detect)')
    
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Saída detalhada')
    
    args = parser.parse_args()
    
    if args.validate:
        # Modo validação apenas
        if args.file:
            try:
                with open(args.file, 'r', encoding='utf-8') as f:
                    sql_content = f.read()
                
                parsed = parse_sql(sql_content)
                print("✅ SQL válido")
                
                if args.verbose:
                    print("\nEstrutura parseada:")
                    print(json.dumps(parsed, indent=2, ensure_ascii=False))
                    
            except Exception as e:
                print(f"❌ Erro na validação: {str(e)}")
                sys.exit(1)
        else:
            print("Modo validação disponível apenas para arquivos individuais")
            sys.exit(1)
    else:
        # Modo conversão
        if args.file:
            result = process_file(args.file, args.output, args.output_dir)
            if not result:
                sys.exit(1)
        elif args.directory:
            process_directory(args.directory, args.output, args.output_dir)

if __name__ == '__main__':
    main()