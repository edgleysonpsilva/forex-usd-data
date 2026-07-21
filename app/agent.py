"""
Núcleo do agente text-to-SQL (reutilizável por CLI e web)
Rags: [C1]=agente [C2]=loop/ReAct [C3]=LLM [C4]=estado [C5]=ferramenta [C6]=guardrail
"""
import os
import re
import psycopg2

MAX_TENTATIVAS   = 3     # [C2] nº de ciclos de auto-correção do loop ReAct
LIMITE_LINHAS    = 100   # [C6] teto de linhas retornadas 
LIMITE_LINHAS_LLM = 20   # [C4] amostra enviada ao LLM 

# [C6] Guardrail camada 1: comandos que nunca podem aparecer
PROIBIDO = re.compile(
    r"\b(drop|delete|update|insert|alter|truncate|grant|revoke|create|"
    r"merge|call|copy|vacuum|reindex)\b", re.IGNORECASE)


# ─── [C3] INTEGRAÇÃO COM LLM — abstração multi-provedor ────────────────────
# O "cérebro" do agente. Troca-se o provedor por variável de ambiente:
#   LLM_PROVIDER=groq    → nuvem, grátis (deploy público)
def chamar_llm(prompt: str, temperature: float) -> str:
    provider = os.environ.get("LLM_PROVIDER", "ollama").lower()

    if provider == "groq":                    # OpenAI-compatível
        from groq import Groq
        client = Groq(api_key=os.environ["GROQ_API_KEY"])
        r = client.chat.completions.create(
            model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
            messages=[{"role": "user", "content": prompt}],
            temperature=temperature,
        )
        return r.choices[0].message.content.strip()

    raise ValueError(f"LLM_PROVIDER desconhecido: {provider}")


# ─── CONEXÃO (Supabase via pooler, SSL) ────────────────────────────────────
def conectar():
    try:
        conn = psycopg2.connect(
            host=os.environ.get("SUPABASE_HOST", "aws-1-ca-central-1.pooler.supabase.com"),
            port=os.environ.get("SUPABASE_PORT", "5432"),
            dbname=os.environ.get("SUPABASE_DB", "postgres"),
            user=os.environ["SUPABASE_USER"],          # use o usuário READ-ONLY!
            password=os.environ["SUPABASE_PASSWORD"],
            sslmode="require", connect_timeout=10,
        )
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '15s'")   # [C6] nenhuma query trava o agente
        conn.commit()
        return conn
    except KeyError as e:
        raise SystemExit(f"❌ Variável ausente: {e}. Defina SUPABASE_USER e SUPABASE_PASSWORD.")


# ─── [C1] PERCEPÇÃO DO AMBIENTE — o agente "enxerga" o banco sozinho ────────
# lê o schema real.
def obter_schema(conn) -> str:
    q = """SELECT table_name, column_name, data_type
           FROM information_schema.columns
           WHERE table_schema='public'
           ORDER BY table_name, ordinal_position"""
    with conn.cursor() as cur:
        cur.execute(q)
        linhas = cur.fetchall()
    tabelas = {}
    for t, c, tipo in linhas:
        tabelas.setdefault(t, []).append(f"{c} {tipo}")
    return "\n".join(f"- {t}({', '.join(cols)})" for t, cols in tabelas.items())


# ─── [C5] A FERRAMENTA — a única "mão" do agente: executar SELECT ───────────
def executar_sql(conn, sql: str):
    sql = sql.strip().rstrip(";")
    # [C6] Guardrails: valida ANTES de executar (saída do LLM = input não confiável)
    if not sql.lower().startswith(("select", "with")):
        raise ValueError("Apenas SELECT/WITH são permitidos.")
    if PROIBIDO.search(sql):
        raise ValueError("Comando proibido detectado.")
    if ";" in sql:
        raise ValueError("Múltiplas statements não são permitidas.")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM ({sql}) AS _sub LIMIT {LIMITE_LINHAS}")  # [C6] teto
        colunas = [d[0] for d in cur.description]
        linhas = cur.fetchall()
    return colunas, linhas


# ─── [C3] PROMPTS — o "programa" do LLM ────────────────────────────────────
def _prompt_sql(schema, pergunta, erro=""):
    correcao = (f'\n\nA tentativa anterior falhou: "{erro}"\nCorrija levando isso em conta.'
                if erro else "")   # [C2] realimenta o erro no loop de auto-correção
    return f"""Você é um especialista em PostgreSQL. Gere UMA consulta SQL que responda à pergunta.
REGRAS:
- Use APENAS as tabelas/colunas do schema abaixo.
- SOMENTE o SQL, sem explicações e sem ```.
- Apenas SELECT. Sintaxe PostgreSQL (use ::numeric em ROUND, ex: ROUND(x::numeric,2)).
- "top N" / "mais/menos" → use ORDER BY + LIMIT.

SCHEMA:
{schema}

PERGUNTA: {pergunta}{correcao}

SQL:"""

def _prompt_resposta(pergunta, colunas, linhas):
    cab = " | ".join(colunas)
    corpo = "\n".join(" | ".join(str(v) for v in lin) for lin in linhas[:LIMITE_LINHAS_LLM])
    return f"""Pergunta: {pergunta}

Resultado da consulta:
{cab}
{corpo}

Responda em português brasileiro, direto, citando os números. Baseie-se APENAS nos dados acima. Não invente."""


def gerar_sql(schema, pergunta, erro=""):
    bruto = chamar_llm(_prompt_sql(schema, pergunta, erro), temperature=0.1)  # [C3] baixa temp
    return re.sub(r"```sql|```", "", bruto).strip()                           # parsing

def formatar_resposta(pergunta, colunas, linhas):
    return chamar_llm(_prompt_resposta(pergunta, colunas, linhas), temperature=0.3)


# ─── [C1][C2][C4] O LOOP DO AGENTE: raciocina → age → observa → corrige ─────
def responder(conn, schema, pergunta) -> dict:
    erro_anterior = ""                                  # [C4] estado efêmero
    sql = ""
    for tentativa in range(1, MAX_TENTATIVAS + 1):      # [C2] loop ReAct
        sql = gerar_sql(schema, pergunta, erro_anterior)      # raciocínio
        try:
            colunas, linhas = executar_sql(conn, sql)         # ação
            if not linhas:
                return {"resposta": "A consulta rodou, mas não retornou resultados.",
                        "sql": sql, "tentativas": tentativa}
            texto = formatar_resposta(pergunta, colunas, linhas)   # observação → resposta
            return {"resposta": texto, "sql": sql, "tentativas": tentativa}
        except Exception as e:                                # falhou → auto-correção
            erro_anterior = str(e).strip().splitlines()[0]    # [C4] guarda o erro
            try: conn.rollback()
            except Exception: pass
    return {"resposta": f"Não consegui após {MAX_TENTATIVAS} tentativas. Último erro: {erro_anterior}",
            "sql": sql, "tentativas": MAX_TENTATIVAS}