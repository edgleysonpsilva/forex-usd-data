import os
import pandas as pd
import plotly.express as px
import streamlit as st
from agent import conectar, obter_schema, responder

for k, v in st.secrets.items():          # secrets → env (agent.py lê de os.environ)
    os.environ[k] = str(v)

st.set_page_config(page_title="Agente de Câmbio", page_icon="💱", layout="wide")
st.title("💱 Agente de Câmbio — pergunte em português")
st.caption("Análise histórica (Fed H.10) · foco no Real (BRL) · ex.: "
           "*evolução do dólar por ano* · *moeda mais volátil na pandemia* · *quantos reais por euro hoje*")

@st.cache_resource
def _boot():
    conn = conectar()
    return conn, obter_schema(conn)

conn, schema = _boot()

def sugerir_grafico(df):
    if df is None or len(df) < 2:
        return None
    num = df.select_dtypes(include="number").columns.tolist()
    if not num:
        return None
    x = df.columns[0]
    if any(t in str(x).lower() for t in ("data", "semana", "ano", "mês", "mes", "dia")):
        return ("line", x, num)
    if len(df) <= 20:
        return ("bar", x, num)
    return ("line", x, num)

if "hist" not in st.session_state:
    st.session_state.hist = []
for papel, msg in st.session_state.hist:
    st.chat_message(papel).write(msg)

if pergunta := st.chat_input("Sua pergunta sobre o câmbio..."):
    st.chat_message("user").write(pergunta)
    st.session_state.hist.append(("user", pergunta))
    with st.spinner("🤔 Gerando SQL e consultando..."):
        r = responder(conn, schema, pergunta)
    with st.chat_message("assistant"):
        st.write(r["resposta"])
        if r.get("linhas"):
            df = pd.DataFrame(r["linhas"], columns=r["colunas"])
            sug = sugerir_grafico(df)
            if sug:
                tipo, x, ys = sug
                fig = (px.line(df, x=x, y=ys) if tipo == "line" else px.bar(df, x=x, y=ys))
                fig.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10),
                                  legend_title_text="", xaxis_title=None)
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
        with st.expander(f"🔎 SQL gerado · {r['tentativas']} tentativa(s)"):
            st.code(r["sql"], language="sql")
    st.session_state.hist.append(("assistant", r["resposta"]))