import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import json
from datetime import datetime

from sql_transformer.parser import parse_sql
from sql_transformer.converter_pyspark import convert_to_pyspark
from sql_transformer.converter_sparksql import convert_to_sparksql
from sql_transformer.dialect_converter import DialectConverter

# Configuração da página
st.set_page_config(
    page_title="SQL to Spark Transformer",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ Transformador SQL para Apache Spark")
st.markdown("**Converta scripts SQL legados (Oracle, PostgreSQL) para PySpark e SparkSQL**")

# Sidebar com informações
with st.sidebar:
    st.header("📋 Recursos Suportados")
    st.markdown("""
    **Construções SQL:**
    - SELECT com aliases
    - JOINs (INNER, LEFT, RIGHT, FULL)
    - WHERE e HAVING
    - GROUP BY e ORDER BY
    - Funções agregadas
    - LIMIT
    
    **Dialetos:**
    - Oracle (ROWNUM, SYSDATE, NVL, DECODE)
    - PostgreSQL (ILIKE, NOW(), EXTRACT)
    - SQL Padrão
    """)
    
    st.header("💡 Dicas")
    st.markdown("""
    - Cole queries complexas com múltiplas tabelas
    - O sistema detecta automaticamente o dialeto
    - Use o botão de validação antes da conversão
    """)

# Layout principal em colunas
col1, col2 = st.columns([1, 1])

with col1:
    st.header("📝 SQL de Entrada")
    
    # Exemplos pré-definidos
    example_queries = {
        "Simples": "SELECT nome, idade FROM clientes WHERE idade > 30",
        "Com JOIN": """SELECT c.nome, p.descricao 
                      FROM clientes c 
                      INNER JOIN pedidos p ON c.id = p.cliente_id 
                      WHERE c.ativo = 1""",
        "Com GROUP BY": """SELECT categoria, COUNT(*) as total, AVG(preco) as preco_medio 
                          FROM produtos 
                          GROUP BY categoria 
                          HAVING COUNT(*) > 5""",
        "Oracle (ROWNUM)": "SELECT * FROM (SELECT nome, salario FROM funcionarios ORDER BY salario DESC) WHERE ROWNUM <= 10",
        "PostgreSQL (ILIKE)": "SELECT nome FROM clientes WHERE nome ILIKE '%silva%' LIMIT 20"
    }
    
    selected_example = st.selectbox("Escolha um exemplo:", ["Personalizado"] + list(example_queries.keys()))
    
    if selected_example != "Personalizado":
        sql_input = st.text_area("SQL Query:", value=example_queries[selected_example], height=200)
    else:
        sql_input = st.text_area("SQL Query:", height=200, placeholder="Cole aqui seu SQL legado...")
    
    # Detecção automática de dialeto
    if sql_input:
        detected_dialect = DialectConverter.detect_dialect(sql_input)
        st.info(f"🔍 Dialeto detectado: **{detected_dialect.upper()}**")
        
        # Conversão de dialeto se necessário
        if detected_dialect in ['oracle', 'postgresql']:
            converted_sql = DialectConverter.convert_to_spark_compatible(sql_input)
            if converted_sql != sql_input:
                st.success("✅ SQL convertido para compatibilidade com Spark")
                with st.expander("Ver SQL convertido"):
                    st.code(converted_sql, language="sql")
                sql_to_parse = converted_sql
            else:
                sql_to_parse = sql_input
        else:
            sql_to_parse = sql_input
    
    # Opções de conversão
    st.subheader("⚙️ Opções")
    output_format = st.radio("Formato de saída:", ["PySpark", "SparkSQL", "Ambos"])
    
    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        validate_btn = st.button("🔍 Validar SQL", use_container_width=True)
    with col_btn2:
        convert_btn = st.button("⚡ Converter", type="primary", use_container_width=True)

with col2:
    st.header("🎯 Resultado")
    
    if sql_input and validate_btn:
        try:
            parsed = parse_sql(sql_to_parse)
            st.success("✅ SQL válido e parseado com sucesso!")
            
            with st.expander("Ver estrutura parseada"):
                st.json(parsed)
                
        except Exception as e:
            st.error(f"❌ Erro na validação: {str(e)}")
    
    if sql_input and convert_btn:
        try:
            parsed = parse_sql(sql_to_parse)
            
            if output_format == "PySpark" or output_format == "Ambos":
                st.subheader("🐍 Código PySpark")
                pyspark_result = convert_to_pyspark(parsed)
                st.code(pyspark_result, language="python")
                
                # Botão de download
                st.download_button(
                    label="📥 Download PySpark (.py)",
                    data=pyspark_result,
                    file_name=f"spark_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py",
                    mime="text/x-python"
                )
            
            if output_format == "SparkSQL" or output_format == "Ambos":
                st.subheader("🗃️ SparkSQL")
                sparksql_result = convert_to_sparksql(parsed)
                st.code(sparksql_result, language="sql")
                
                # Botão de download
                st.download_button(
                    label="📥 Download SparkSQL (.sql)",
                    data=sparksql_result,
                    file_name=f"spark_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql",
                    mime="text/sql"
                )
            
            st.success("✅ Conversão realizada com sucesso!")
            
        except Exception as e:
            st.error(f"❌ Erro na conversão: {str(e)}")
            st.info("💡 Verifique se o SQL está correto e tente novamente.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>🚀 <strong>SQL to Spark Transformer</strong> - Modernize seus scripts SQL legados</p>
    <p>Suporte para Oracle, PostgreSQL → Apache Spark (PySpark + SparkSQL)</p>
</div>
""", unsafe_allow_html=True)