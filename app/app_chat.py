import os
import pandas as pd
import plotly.express as px
import streamlit as st
from agent import conectar, obter_schema
from hibrido import responder_hibrido

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
    with st.spinner("Decidindo a melhor abordagem..."):
        r = responder_hibrido(conn, schema, pergunta)
    with st.chat_message("assistant"):
        st.caption(f"Rota escolhida: {r['rota']}")          # transparência do roteamento
        st.write(r["resposta"])

        if r.get("linhas"):
            import pandas as pd
            import plotly.express as px

            df = pd.DataFrame(r["linhas"], columns=r["colunas"])

            for col in df.columns[1:]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            num = [c for c in df.columns[1:] if pd.api.types.is_numeric_dtype(df[c])]
            st.caption(f"🐞 debug — colunas: {list(df.columns)} · numéricas: {num} · linhas: {len(df)}")
        
            if num and len(df) > 1:
                x = df.columns[0]
                df = df.sort_values(by=x)
                eh_tempo = any(t in str(x).lower() for t in ("data", "semana", "ano", "mes", "dia"))
                try:
                    if eh_tempo:
                        fig = px.line(df, x=x, y=num, markers=True)
                       else:
                fig = px.bar(df, x=x, y=num)
                    fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10), xaxis_title=None)
                    st.plotly_chart(fig, width='stretch')          # ← era use_container_width=True
                except Exception as e:
                    st.warning(f"Não consegui plotar: {e}")
        
            st.dataframe(df, width='stretch', hide_index=True)     # ← era use_container_width=True
