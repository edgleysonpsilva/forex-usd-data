"""app_chat.py — Interface web do agente híbrido de câmbio (text-to-SQL + RAG).
Streamlit >= 1.60 (usa width='stretch', não use_container_width)."""
import os
import pandas as pd
import plotly.express as px
import streamlit as st

from agent import conectar, obter_schema
from hibrido import responder_hibrido

# ─── secrets → variáveis de ambiente (agent.py/rag.py leem de os.environ) ───
for k, v in st.secrets.items():
    os.environ[k] = str(v)

st.set_page_config(page_title="Agente de Câmbio", page_icon="💱", layout="wide")
st.title("💱 Agente de Câmbio — pergunte em português")
st.caption("Análise histórica (Fed H.10) · foco no Real (BRL) · ex.: "
           "*evolução da taxa do dólar por ano* · *moeda mais volátil na pandemia* · "
           "*por que o dólar disparou em 2020?*")

# ─── conexão + schema, cacheados (1x por sessão) ───
@st.cache_resource
def _boot():
    conn = conectar()
    return conn, obter_schema(conn)

conn, schema = _boot()


# ─── plota o resultado quando faz sentido (série temporal → linha; senão barras) ───
def plotar(colunas, linhas):
    if not linhas:
        return
    df = pd.DataFrame(linhas, columns=colunas)

    # força colunas (menos a 1ª) a virarem número — Postgres traz Decimal/texto
    for col in df.columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    num = [c for c in df.columns[1:] if pd.api.types.is_numeric_dtype(df[c])
           and df[c].notna().any()]

    if num and len(df) > 1:
        x = df.columns[0]
        try:
            df = df.sort_values(by=x)
        except Exception:
            pass
        eh_tempo = any(t in str(x).lower() for t in ("data", "semana", "ano", "mes", "mês", "dia"))
        try:
            fig = (px.line(df, x=x, y=num, markers=True) if eh_tempo
                   else px.bar(df, x=x, y=num))
            fig.update_layout(height=400, margin=dict(l=10, r=10, t=30, b=10),
                              xaxis_title=None, legend_title_text="")
            st.plotly_chart(fig, width="stretch")
        except Exception as e:
            st.warning(f"Não consegui gerar o gráfico: {e}")

    st.dataframe(df, width="stretch", hide_index=True)


# ─── memória da conversa ───
if "hist" not in st.session_state:
    st.session_state.hist = []

# redesenha o histórico salvo (texto). Gráficos aparecem só na resposta atual.
for msg in st.session_state.hist:
    with st.chat_message(msg["role"]):
        if msg.get("rota"):
            st.caption(f"Rota escolhida: {msg['rota']}")
        st.write(msg["conteudo"])
        if msg.get("fontes"):
            st.caption("📚 Fontes: " + ", ".join(msg["fontes"]))

# ─── nova pergunta ───
if pergunta := st.chat_input("Sua pergunta sobre o câmbio..."):
    st.chat_message("user").write(pergunta)
    st.session_state.hist.append({"role": "user", "conteudo": pergunta})

    with st.spinner("🤔 Decidindo a melhor abordagem..."):
        r = responder_hibrido(conn, schema, pergunta)

    with st.chat_message("assistant"):
        st.caption(f"Rota escolhida: {r.get('rota', '—')}")
        st.write(r["resposta"])

        # gráfico + tabela, se a rota trouxe dados numéricos
        plotar(r.get("colunas", []), r.get("linhas", []))

        if r.get("fontes"):
            st.caption("📚 Fontes: " + ", ".join(set(r["fontes"])))
        if r.get("sql"):
            with st.expander(f"🔎 SQL gerado · {r.get('tentativas', 1)} tentativa(s)"):
                st.code(r["sql"], language="sql")

    # salva a resposta no histórico (texto + metadados)
    st.session_state.hist.append({
        "role": "assistant",
        "conteudo": r["resposta"],
        "rota": r.get("rota"),
        "fontes": sorted(set(r["fontes"])) if r.get("fontes") else None,
    })
