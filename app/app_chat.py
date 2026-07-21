import os, streamlit as st
from agent import conectar, obter_schema, responder

# bridge: secrets do Streamlit → variáveis de ambiente (que o agente.py lê)
for k, v in st.secrets.items():
    os.environ[k] = str(v)

st.set_page_config(page_title="Agente de Câmbio", page_icon="🤖")
st.title("🤖 Agente de Câmbio — pergunte em português")

@st.cache_resource
def _boot():
    conn = conectar()
    return conn, obter_schema(conn)

conn, schema = _boot()

if "hist" not in st.session_state:          # [C4] estado conversacional
    st.session_state.hist = []

for msg in st.session_state.hist:           # re-renderiza o histórico
    st.chat_message(msg["role"]).write(msg["content"])

if pergunta := st.chat_input("Ex.: qual moeda foi mais volátil na pandemia?"):
    st.chat_message("user").write(pergunta)
    st.session_state.hist.append({"role": "user", "content": pergunta})
    with st.spinner("Pensando..."):
        r = responder(conn, schema, pergunta)
    with st.chat_message("assistant"):
        st.write(r["resposta"])
        with st.expander(f"🔎 SQL gerado ({r['tentativas']} tentativa(s))"):
            st.code(r["sql"], language="sql")   # transparência = ponto forte
    st.session_state.hist.append({"role": "assistant", "content": r["resposta"]})