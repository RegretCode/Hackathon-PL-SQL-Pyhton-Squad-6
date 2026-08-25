# Hackathon-PL-SQL-Pyhton-Squad-6

Ferramenta para transformar SQL legado (Oracle, PostgreSQL) em PySpark e SparkSQL.

## Funcionalidades
- Conversão automática SQL → PySpark/SparkSQL
- Detecção de dialetos Oracle/PostgreSQL
- Interface web Streamlit
- CLI para processamento em lote
- Suporte a JOINs, GROUP BY, funções agregadas

## Conversões de Dialetos
**Oracle → Spark:**
- ROWNUM → ROW_NUMBER()
- SYSDATE → CURRENT_TIMESTAMP
- NVL → COALESCE
- DECODE → CASE WHEN

**PostgreSQL → Spark:**
- ILIKE → LIKE
- NOW() → CURRENT_TIMESTAMP
- EXTRACT → DATE_PART

## Instalação
```bash
pip install -r requirements.txt
```

## Uso

### Interface Web
```bash
streamlit run interface/app.py
```

### CLI
```bash
python -m sql_transformer.cli -f query.sql -o both
```

### Programático
```python
from sql_transformer.parser import parse_sql
from sql_transformer.converter_pyspark import convert_to_pyspark

sql = "SELECT nome FROM clientes WHERE idade > 30"
parsed = parse_sql(sql)
result = convert_to_pyspark(parsed)
```

## 📊 Exemplos de Conversão

### Exemplo 1: Query Simples
**SQL Original:**
```sql
SELECT nome, idade FROM clientes WHERE idade > 30
```

**PySpark Gerado:**
```python
df = spark.table('clientes').filter(F.col('idade') > F.lit(30)).select(F.col('nome'), F.col('idade'))
```

**SparkSQL Gerado:**
```python
spark.sql("""
SELECT nome, idade FROM clientes WHERE idade > 30
""")
```

### Exemplo 2: Query com JOIN e Agregação
**SQL Original:**
```sql
SELECT c.categoria, COUNT(*) as total, AVG(c.preco) as preco_medio
FROM produtos c
INNER JOIN vendas v ON c.id = v.produto_id
WHERE v.data_venda >= '2024-01-01'
GROUP BY c.categoria
HAVING COUNT(*) > 10
ORDER BY preco_medio DESC
```

**PySpark Gerado:**
```python
df = spark.table('produtos').join(spark.table('vendas'), F.col('produtos.id') == F.col('vendas.produto_id'), 'inner').filter(F.col('vendas.data_venda >') == F.lit('2024-01-01')).select(F.col('produtos.categoria'), F.count(F.col('*')).alias('total'), F.avg(F.col('produtos.preco')).alias('preco_medio')).orderBy(F.col('preco_medio').desc())
```

### Exemplo 3: Conversão Oracle
**SQL Oracle Original:**
```sql
SELECT nome, salario, ROWNUM
FROM (
    SELECT nome, salario
    FROM funcionarios
    WHERE NVL(ativo, 'N') = 'S'
    ORDER BY salario DESC
)
WHERE ROWNUM <= 5
```

**SQL Convertido para Spark:**
```sql
SELECT nome, salario, ROW_NUMBER() OVER (ORDER BY 1)
FROM (
    SELECT nome, salario
    FROM funcionarios
    WHERE COALESCE(ativo, 'N') = 'S'
    ORDER BY salario DESC
)
WHERE ROW_NUMBER() OVER (ORDER BY 1) <= 5
```

## 🏗️ Arquitetura

```
sql_transformer/
├── __init__.py
├── main.py                 # Demonstrações
├── parser.py              # Parser SQL avançado
├── converter_pyspark.py   # Conversor para PySpark
├── converter_sparksql.py  # Conversor para SparkSQL
├── dialect_converter.py  # Conversão de dialetos
└── cli.py                # Interface de linha de comando

interface/
└── app.py                # Interface web Streamlit

requirements.txt          # Dependências
README.md                # Documentação
```

### Componentes Principais

1. **Parser SQL** (`parser.py`)
   - Análise sintática avançada
   - Suporte a construções complexas
   - Extração de metadados estruturados

2. **Conversores** (`converter_*.py`)
   - Geração de código PySpark otimizado
   - Formatação de SparkSQL limpo
   - Tratamento de casos especiais

3. **Conversor de Dialetos** (`dialect_converter.py`)
   - Detecção automática de dialetos
   - Transformações específicas Oracle/PostgreSQL
   - Compatibilidade com Spark SQL

4. **Interface Web** (`interface/app.py`)
   - Interface intuitiva com Streamlit
   - Validação em tempo real
   - Download de arquivos gerados

5. **CLI** (`cli.py`)
   - Processamento em lote
   - Automação corporativa
   - Relatórios de conversão

## 🎯 Casos de Uso

### 1. Migração de Data Warehouse
- Conversão de procedures Oracle para Spark jobs
- Modernização de ETL legado
- Migração para arquiteturas cloud-native

### 2. Desenvolvimento Ágil
- Prototipagem rápida de queries Spark
- Conversão de consultas ad-hoc
- Padronização de código

### 3. Treinamento e Educação
- Aprendizado de PySpark através de SQL familiar
- Comparação entre dialetos
- Demonstrações práticas

### 4. Automação Corporativa
- Processamento em lote de scripts legados
- Integração em pipelines CI/CD
- Relatórios de migração

## 🔧 Configuração Avançada

### Variáveis de Ambiente
```bash
# Configurar Spark (opcional)
export SPARK_HOME=/path/to/spark
export PYSPARK_PYTHON=python3

# Configurar logging
export SQL_TRANSFORMER_LOG_LEVEL=INFO
```

### Personalização
O sistema pode ser estendido através de:
- Novos conversores de dialetos
- Funções SQL customizadas
- Templates de código personalizados

## 🤝 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

## 🆘 Suporte

- **Issues**: Reporte bugs e solicite features no GitHub
- **Documentação**: Consulte este README e os comentários no código
- **Exemplos**: Execute `python sql_transformer/main.py` para ver demonstrações

## 🚀 Roadmap

### Próximas Versões
- [ ] Suporte a mais dialetos (SQL Server, MySQL)
- [ ] Otimizações de performance automáticas
- [ ] Integração com Databricks
- [ ] Suporte a Delta Lake
- [ ] API REST para integração
- [ ] Plugin para IDEs populares

---

**Desenvolvido para facilitar a modernização de sistemas legados e acelerar a adoção do Apache Spark em ambientes corporativos.**
