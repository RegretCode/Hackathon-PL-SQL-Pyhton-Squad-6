"""
Validador Interativo de Parser de Consultas SQL

Este script permite inserir consultas SQL e ver resultados detalhados de análise
para verificar se o parser entendeu corretamente sua consulta digitada.
"""

import sys
import os
from typing import Dict, Any

# Adicionar diretório src ao caminho para imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.translator.sql_parser import SQLParser, ParsedSQL


class QueryValidator:
    """Classe para validar e exibir resultados detalhados de análise."""
    
    def __init__(self):
        self.parser = SQLParser()
        self.validation_count = 0
    
    def validate_query(self, sql_query: str) -> Dict[str, Any]:
        """
        Valida uma consulta SQL e retorna resultados detalhados de análise.
        
        Args:
            sql_query (str): A consulta SQL para validar
            
        Returns:
            Dict[str, Any]: Resultados detalhados de validação
        """
        self.validation_count += 1
        
        print(f"\n{'='*80}")
        print(f"🔍 VALIDAÇÃO DE CONSULTA #{self.validation_count}")
        print(f"{'='*80}")
        
        print(f"\n📝 Sua Consulta SQL:")
        print(f"   {sql_query}")
        print(f"\n{'─'*80}")
        
        # Analisar a consulta
        parsed = self.parser.parse(sql_query)
        
        # Mostrar status de análise
        if parsed.is_valid:
            print(f"✅ STATUS DA ANÁLISE: SUCESSO")
        else:
            print(f"❌ STATUS DA ANÁLISE: FALHA")
            print(f"   Erros: {', '.join(parsed.errors)}")
            return self._create_validation_result(sql_query, parsed, False)
        
        # Exibir resultados detalhados
        self._display_parsed_components(parsed)
        
        # Pedir confirmação do usuário
        is_correct = self._ask_user_confirmation(parsed)
        
        return self._create_validation_result(sql_query, parsed, is_correct)
    
    def _display_parsed_components(self, parsed: ParsedSQL):
        """Exibir todos os componentes analisados em detalhes."""
        
        print(f"\n🎯 COMPONENTES ANALISADOS:")
        print(f"{'─'*50}")
        
        # Informações da tabela
        print(f"📊 Tabela FROM:")
        print(f"   └── {parsed.from_table}")
        
        # Colunas SELECT
        print(f"\n📋 Colunas SELECT:")
        if parsed.select_columns:
            for i, col in enumerate(parsed.select_columns, 1):
                if col.is_wildcard:
                    print(f"   {i}. * (Todas as colunas)")
                elif col.alias:
                    print(f"   {i}. {col.name} → com alias '{col.alias}'")
                else:
                    print(f"   {i}. {col.name}")
        else:
            print(f"   └── Nenhuma encontrada")
        
        # Condições WHERE
        print(f"\n🔍 Condições WHERE:")
        if parsed.where_conditions:
            for i, cond in enumerate(parsed.where_conditions, 1):
                logical_text = f" ({cond.logical_operator} com próxima)" if cond.logical_operator else ""
                if isinstance(cond.value, list):
                    value_text = f"[{', '.join(map(str, cond.value))}]"
                else:
                    value_text = f"'{cond.value}'" if isinstance(cond.value, str) else str(cond.value)
                print(f"   {i}. {cond.column} {cond.operator} {value_text}{logical_text}")
        else:
            print(f"   └── Nenhuma encontrada")
        
        # Colunas ORDER BY
        print(f"\n📈 Colunas ORDER BY:")
        if parsed.order_by_columns:
            for i, col in enumerate(parsed.order_by_columns, 1):
                print(f"   {i}. {col.column} ({col.direction})")
        else:
            print(f"   └── Nenhuma encontrada")
        
        # LIMIT
        print(f"\n🔢 LIMIT:")
        if parsed.limit_count:
            print(f"   └── {parsed.limit_count} linhas")
        else:
            print(f"   └── Nenhum limite especificado")
    
    def _ask_user_confirmation(self, parsed: ParsedSQL) -> bool:
        """Perguntar ao usuário se a análise está correta."""
        
        print(f"\n{'─'*80}")
        print(f"❓ PERGUNTA DE VALIDAÇÃO:")
        print(f"   O parser entendeu corretamente sua consulta SQL?")
        print(f"   Verifique se todos os componentes acima correspondem à sua intenção.")
        
        while True:
            answer = input(f"\n   Digite 's' para Sim, 'n' para Não, ou 'd' para Detalhes: ").strip().lower()
            
            if answer in ['s', 'sim']:
                print(f"   ✅ Ótimo! O parser entendeu corretamente sua consulta.")
                return True
            elif answer in ['n', 'não', 'nao']:
                print(f"   ❌ O parser não entendeu sua consulta.")
                self._ask_for_feedback(parsed)
                return False
            elif answer in ['d', 'detalhes']:
                self._show_technical_details(parsed)
                continue
            else:
                print(f"   Por favor digite 's', 'n', ou 'd'.")
    
    def _ask_for_feedback(self, parsed: ParsedSQL):
        """Perguntar ao usuário o que estava errado com a análise."""
        print(f"\n🤔 O que deu errado? Por favor descreva o que o parser não entendeu:")
        feedback = input(f"   Seu feedback: ").strip()
        
        if feedback:
            print(f"\n📝 Obrigado pelo seu feedback:")
            print(f"   '{feedback}'")
            print(f"   Isso ajuda a melhorar o parser!")
    
    def _show_technical_details(self, parsed: ParsedSQL):
        """Mostrar detalhes técnicos da análise."""
        print(f"\n🔧 DETALHES TÉCNICOS:")
        print(f"{'─'*40}")
        print(f"Consulta Original: {parsed.original_query}")
        print(f"Análise Válida: {parsed.is_valid}")
        print(f"Contagem de Componentes:")
        print(f"  • Colunas SELECT: {len(parsed.select_columns)}")
        print(f"  • Condições WHERE: {len(parsed.where_conditions)}")
        print(f"  • Colunas ORDER BY: {len(parsed.order_by_columns)}")
        print(f"  • Erros: {len(parsed.errors)}")
        
        if parsed.errors:
            print(f"Detalhes dos Erros: {parsed.errors}")
    
    def _create_validation_result(self, query: str, parsed: ParsedSQL, is_correct: bool) -> Dict[str, Any]:
        """Criar resumo do resultado de validação."""
        return {
            'query': query,
            'validation_id': self.validation_count,
            'parsing_successful': parsed.is_valid,
            'user_confirmed_correct': is_correct,
            'components_found': {
                'table': parsed.from_table,
                'columns': len(parsed.select_columns),
                'where_conditions': len(parsed.where_conditions),
                'order_by': len(parsed.order_by_columns),
                'limit': parsed.limit_count is not None
            },
            'errors': parsed.errors
        }
    
    def run_interactive_validation(self):
        """Executar sessão de validação interativa."""
        print(f"🚀 Validador Interativo de Parser de Consultas SQL")
        print(f"{'='*60}")
        print(f"📖 Instruções:")
        print(f"   • Digite sua consulta SQL e veja como o parser a entende")
        print(f"   • Valide se a análise corresponde à sua intenção")
        print(f"   • Digite 'sair', 'quit', ou 'q' para parar")
        print(f"   • Digite 'ajuda' para exemplos")
        
        validation_results = []
        
        while True:
            try:
                print(f"\n{'─'*60}")
                user_input = input(f"🎯 Digite sua consulta SQL: ").strip()
                
                if not user_input:
                    print(f"   ⚠️  Consulta vazia. Por favor digite uma declaração SQL.")
                    continue
                
                if user_input.lower() in ['sair', 'quit', 'q']:
                    break
                
                if user_input.lower() == 'ajuda':
                    self._show_examples()
                    continue
                
                # Validar a consulta
                result = self.validate_query(user_input)
                validation_results.append(result)
                
            except KeyboardInterrupt:
                print(f"\n\n🛑 Sessão de validação interrompida pelo usuário.")
                break
            except Exception as e:
                print(f"\n❌ Erro durante validação: {e}")
        
        # Mostrar resumo da sessão
        self._show_session_summary(validation_results)
    
    def _show_examples(self):
        """Mostrar consultas SQL de exemplo."""
        examples = [
            "SELECT nome, idade FROM usuarios",
            "SELECT * FROM produtos WHERE preco > 100",
            "SELECT nome, salario as salary FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC",
            "SELECT produto, preco FROM estoque WHERE categoria IN ('Eletrônicos', 'Informática') LIMIT 10"
        ]
        
        print(f"\n💡 Consultas SQL de exemplo para testar:")
        for i, example in enumerate(examples, 1):
            print(f"   {i}. {example}")
    
    def _show_session_summary(self, results):
        """Mostrar resumo da sessão de validação."""
        if not results:
            print(f"\n📊 Nenhuma consulta foi validada nesta sessão.")
            return
        
        print(f"\n{'='*60}")
        print(f"📊 RESUMO DA SESSÃO DE VALIDAÇÃO")
        print(f"{'='*60}")
        
        total_queries = len(results)
        successful_parsing = sum(1 for r in results if r['parsing_successful'])
        user_confirmed = sum(1 for r in results if r['user_confirmed_correct'])
        
        print(f"Total de consultas testadas: {total_queries}")
        print(f"Analisadas com sucesso: {successful_parsing}")
        print(f"Confirmadas pelo usuário: {user_confirmed}")
        print(f"Taxa de sucesso de análise: {successful_parsing/total_queries*100:.1f}%")
        print(f"Taxa de satisfação do usuário: {user_confirmed/total_queries*100:.1f}%")
        
        # Mostrar consultas problemáticas
        problematic = [r for r in results if not r['parsing_successful'] or not r['user_confirmed_correct']]
        if problematic:
            print(f"\n⚠️  Consultas que precisam de atenção:")
            for i, result in enumerate(problematic, 1):
                status = "Falha na análise" if not result['parsing_successful'] else "Usuário discordou da análise"
                print(f"   {i}. {result['query']} - {status}")
        
        print(f"\n🙏 Obrigado por ajudar a melhorar o parser SQL!")


def main():
    """Função principal para executar o validador de consultas."""
    validator = QueryValidator()
    validator.run_interactive_validation()


if __name__ == "__main__":
    main()
