"""hibrido.py — decide a rota e orquestra text-to-SQL + RAG."""
from agent import chamar_llm, responder as responder_sql, obter_schema
from rag import recuperar

def rotear(pergunta):
    """Classifica a intenção usando o próprio LLM (barato e flexível)."""
    prompt = f"""Classifique a pergunta sobre câmbio em UMA palavra:
- NUMERICA: pede número/cálculo/ranking/valor/comparação quantitativa.
- CONCEITUAL: pede explicação, definição, contexto histórico, "por quê".
- HIBRIDA: pede número E explicação.

Pergunta: "{pergunta}"
Responda só a palavra:"""
    r = chamar_llm(prompt, temperature=0.0).strip().upper()
    return r if r in {"NUMERICA", "CONCEITUAL", "HIBRIDA"} else "NUMERICA"

def _responder_rag(pergunta, trechos):
    contexto = "\n\n---\n".join(f"[{t['origem']}] {t['texto']}" for t in trechos)
    prompt = f"""Responda à pergunta usando SOMENTE o contexto abaixo (documentos do projeto).
Se o contexto não bastar, diga isso. Cite a origem entre colchetes. Português, didático.

CONTEXTO:
{contexto}

PERGUNTA: {pergunta}

RESPOSTA:"""
    return chamar_llm(prompt, temperature=0.3)

def responder_hibrido(conn, schema, pergunta):
    rota = rotear(pergunta)

    if rota == "NUMERICA":
        r = responder_sql(conn, schema, pergunta)
        r["rota"] = "🧮 Text-to-SQL"
        return r

    if rota == "CONCEITUAL":
        trechos = recuperar(pergunta, k=3)
        return {"resposta": _responder_rag(pergunta, trechos), "rota": "📚 RAG",
                "fontes": [t["origem"] for t in trechos],
                "sql": "", "tentativas": 0, "colunas": [], "linhas": []}

    # HIBRIDA: número (SQL) + contexto (RAG), fundidos numa resposta
    r_sql = responder_sql(conn, schema, pergunta)
    trechos = recuperar(pergunta, k=2)
    contexto = "\n".join(f"[{t['origem']}] {t['texto']}" for t in trechos)
    fusao = chamar_llm(
        f"""Combine o DADO calculado e o CONTEXTO documental numa resposta única, didática.

DADO (resultado SQL): {r_sql['resposta']}

CONTEXTO (documentos): {contexto}

PERGUNTA: {pergunta}

Responda em português, primeiro o número, depois a explicação do porquê. Cite [origem].""",
        temperature=0.3)
    return {"resposta": fusao, "rota": "🔀 Híbrida (SQL + RAG)",
            "sql": r_sql.get("sql", ""), "tentativas": r_sql.get("tentativas", 0),
            "colunas": r_sql.get("colunas", []), "linhas": r_sql.get("linhas", []),
            "fontes": [t["origem"] for t in trechos]}