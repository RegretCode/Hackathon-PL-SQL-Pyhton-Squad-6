# Tradutor SQL para PySpark

Um projeto Python que traduz consultas SQL para chamadas da API DataFrame do PySpark. Esta ferramenta ajuda desenvolvedores a converter declarações SQL em operações PySpark equivalentes, suportando tanto Spark SQL quanto saídas da API DataFrame.

## Funcionalidades

- 🔄 **Saída Dupla**: Gera equivalentes tanto em Spark SQL quanto na API DataFrame do PySpark
- 📝 **Suporte SELECT**: Manipula seleção de colunas com aliases
- 🔍 **Cláusulas WHERE**: Suporta vários operadores e condições
- 📊 **ORDER BY**: Ordenação ascendente e descendente
- 🔢 **LIMIT**: Limitação de resultados
- 🧩 **Expressões Complexas**: Operadores AND/OR e condições aninhadas
- 🎯 **Modo Interativo**: Interface de linha de comando para testar consultas

## Recursos SQL Suportados

✅ SELECT com seleção de colunas  
✅ Cláusulas WHERE com vários operadores  
✅ ORDER BY com ASC/DESC  
✅ Cláusulas LIMIT  
✅ Aliases de colunas (AS)  
✅ SELECT \* (todas as colunas)  
✅ Condições complexas com AND/OR

## Estrutura do Projeto

```
sql_translator_project/
├── src/
│   ├── translator/
│   │   ├── sql_translator.py    # Lógica central de tradução
│   │   └── utils.py             # Funções utilitárias
│   └── examples/
│       └── demo_examples.py     # Demos e exemplos
├── tests/
│   └── test_translator.py       # Testes unitários
├── main.py                      # Ponto de entrada principal da aplicação
├── requirements.txt             # Dependências
└── README.md                    # Este arquivo
```

## Instalação

1. Clone ou baixe este projeto
2. Navegue até o diretório do projeto
3. Instale as dependências:

```bash
pip install -r requirements.txt
```

## Uso

### Executando a Aplicação

Execute a aplicação principal:

```bash
python main.py
```

Isso apresentará várias opções:

1. Executar demo de tradução com casos de teste
2. Executar demo prático com dados de exemplo
3. Tradutor interativo
4. Tradução rápida (consulta única)

### Usando como um Módulo

```python
from src.translator.sql_translator import SQLTranslator

# Inicializar tradutor
translator = SQLTranslator()

# Traduzir uma consulta SQL
sql_query = "SELECT nome, idade FROM usuarios WHERE idade > 18 ORDER BY idade DESC"
result = translator.translate(sql_query)

print("SQL Original:", result['original_sql'])
print("Spark SQL:", result['spark_sql'])
print("Código PySpark:", result['pyspark_code'])
```

### Exemplo de Tradução

**SQL de Entrada:**

```sql
SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC LIMIT 5
```

**Saída Spark SQL:**

```python
spark.sql("""SELECT nome, salario FROM funcionarios WHERE departamento = 'TI' ORDER BY salario DESC LIMIT 5""")
```

**Saída API DataFrame PySpark:**

```python
from pyspark.sql import functions as F

funcionarios_df.select(F.col('nome'), F.col('salario')).filter((F.col('departamento') == 'TI')).orderBy(F.col('salario').desc()).limit(5)
```

## Testes

Execute os testes unitários:

```bash
python -m pytest tests/
```

Ou execute testes com cobertura:

```bash
python -m pytest tests/ --cov=src --cov-report=html
```

## Referência da API

### Classe SQLTranslator

#### `translate(sql_query: str) -> dict`

Traduz uma consulta SQL para tanto Spark SQL quanto API DataFrame PySpark.

**Parâmetros:**

- `sql_query` (str): A consulta SQL para traduzir

**Retorna:**

- `dict`: Resultado da tradução contendo:
  - `original_sql`: A consulta SQL de entrada
  - `spark_sql`: Equivalente Spark SQL
  - `pyspark_code`: Equivalente API DataFrame PySpark
  - `translation_available`: Boolean indicando se a tradução PySpark foi bem-sucedida
  - `translation_id`: Identificador único para a tradução

### Funções Utilitárias

#### `translate_sql(sql_query: str) -> Tuple[str, Optional[str]]`

Função central de tradução que converte SQL para PySpark.

#### `convert_where_clause(where_clause: str) -> str`

Converte cláusulas WHERE SQL para expressões filter PySpark.

#### `get_test_cases() -> List[Dict[str, str]]`

Retorna casos de teste predefinidos para validação.

## Limitações

- Atualmente suporta apenas declarações SELECT
- Subconsultas complexas podem não ser totalmente suportadas
- Algumas funções SQL avançadas podem exigir ajuste manual
- Operações JOIN ainda não estão implementadas

## Desenvolvimento

### Adicionando Novas Funcionalidades

1. Implemente nova funcionalidade em `src/translator/sql_translator.py`
2. Adicione funções utilitárias em `src/translator/utils.py`
3. Crie testes em `tests/test_translator.py`
4. Atualize exemplos em `src/examples/demo_examples.py`

### Estilo de Código

O projeto segue as melhores práticas do Python:

- Use nomes de variáveis descritivos
- Adicione docstrings para funções e classes
- Siga as diretrizes de estilo PEP 8
- Inclua type hints quando apropriado

## Contribuindo

1. Faça um fork do repositório
2. Crie uma branch de funcionalidade
3. Adicione testes para nova funcionalidade
4. Garanta que todos os testes passem
5. Envie um pull request

## Licença

Este projeto é código aberto e disponível sob a Licença MIT.

## Agradecimentos

Este projeto foi desenvolvido para ajudar a preencher a lacuna entre SQL e PySpark, facilitando a transição de desenvolvedores SQL para operações DataFrame PySpark.

## Suporte

Para dúvidas ou problemas, por favor:

1. Verifique casos de teste existentes para exemplos
2. Revise a documentação
3. Execute o tradutor interativo para testes
4. Examine o código fonte para detalhes de implementação
