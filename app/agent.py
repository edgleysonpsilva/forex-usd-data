"""agent.py — Agente text-to-SQL de análise cambial (v2 com contexto de negócio)."""
import os, re, psycopg2

MAX_TENTATIVAS    = 3
LIMITE_LINHAS     = 500     # teto de linhas retornadas (segurança)
LIMITE_LINHAS_LLM = 25      # amostra enviada ao LLM (gestão de contexto)

PROIBIDO = re.compile(r"\b(drop|delete|update|insert|alter|truncate|grant|"
                      r"revoke|create|merge|call|copy|vacuum|reindex)\b", re.IGNORECASE)

CONTEXTO_NEGOCIO = """
PROPÓSITO: sistema de ANÁLISE HISTÓRICA de taxas de câmbio (NÃO é tempo real / trading).
Dados: Federal Reserve H.10, frequência diária. "Mais recente" = última data existente na base.
MOEDA PRIORITÁRIA: BRL. Se a pergunta NÃO indicar a moeda, assuma 'BRL'.
Moedas: BRL, EUR, GBP, JPY, CNY, CAD, AUD, CHF, MXN, KRW, INR (todas contra USD).

COLUNAS:
- taxa_usd: unidades da moeda por 1 DÓLAR. Ex.: BRL=5.2 → US$1=R$5,20. MAIOR=mais fraca.
- variacao_diaria_pct: variação % dia a dia.
- volatilidade_movel_30d: desvio-padrão das variações diárias em 30d (risco, não direção).
- regime: 'pre_pandemia' | 'pandemia' | 'pos_pandemia'.  moeda_codigo: código ISO.

ONDE BUSCAR:
- valor do dólar / série / volatilidade diária → volatilidade_movel
  (data, moeda_codigo, taxa_usd, variacao_diaria_pct, volatilidade_movel_30d, regime)
- agregado por regime → variacao_cambial_por_regime
- ranking 30d → variacao_cambial_30d   ·   base bruta → fato_taxas_historico

REGRAS:
- SEMPRE filtre por moeda_codigo (default 'BRL').
- atual/hoje → ORDER BY data DESC LIMIT 1.
- evolução/série/gráfico → SELECT data (ou DATE_TRUNC('week',data)::date) + valor, ORDER BY data.
- top/maior/menor N → ORDER BY ... LIMIT N.
- ROUND sobre cálculo exige ::numeric.
- par cruzado A/B (sem USD) → taxa_usd_A / taxa_usd_B na mesma data (JOIN por data).
"""

def chamar_llm(prompt, temperature):
    provider = os.environ.get("LLM_PROVIDER", "ollama").lower()
    if provider == "ollama":
        import ollama
        r = ollama.chat(model=os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:7b"),
                        messages=[{"role": "user", "content": prompt}],
                        options={"temperature": temperature})
        return r["message"]["content"].strip()
    if provider == "groq":
        from groq import Groq
        cli = Groq(api_key=os.environ["GROQ_API_KEY"])
        r = cli.chat.completions.create(
            model=os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile"),
            messages=[{"role": "user", "content": prompt}], temperature=temperature)
        return r.choices[0].message.content.strip()
    raise ValueError(f"LLM_PROVIDER desconhecido: {provider}")

def conectar():
    conn = psycopg2.connect(
        host=os.environ.get("SUPABASE_HOST", "aws-1-ca-central-1.pooler.supabase.com"),
        port=os.environ.get("SUPABASE_PORT", "5432"),
        dbname=os.environ.get("SUPABASE_DB", "postgres"),
        user=os.environ["SUPABASE_USER"], password=os.environ["SUPABASE_PASSWORD"],
        sslmode="require", connect_timeout=10)
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = '15s'")
    conn.commit()
    return conn

def obter_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name, column_name, data_type FROM information_schema.columns
                       WHERE table_schema='public' ORDER BY table_name, ordinal_position""")
        linhas = cur.fetchall()
    t = {}
    for tabela, col, tipo in linhas:
        t.setdefault(tabela, []).append(f"{col} {tipo}")
    return "\n".join(f"- {k}({', '.join(v)})" for k, v in t.items())

def executar_sql(conn, sql):
    sql = sql.strip().rstrip(";")
    if not sql.lower().startswith(("select", "with")):
        raise ValueError("Apenas SELECT/WITH são permitidos.")
    if PROIBIDO.search(sql):
        raise ValueError("Comando proibido detectado.")
    if ";" in sql:
        raise ValueError("Múltiplas statements não são permitidas.")
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM ({sql}) AS _s LIMIT {LIMITE_LINHAS}")
        colunas = [d[0] for d in cur.description]
        linhas = cur.fetchall()
    return colunas, linhas

def _prompt_sql(schema, pergunta, erro=""):
    corr = f'\n\nA tentativa anterior falhou: "{erro}"\nCorrija.' if erro else ""
    return f"""Você é um especialista em PostgreSQL e câmbio. Gere UMA consulta SQL.
{CONTEXTO_NEGOCIO}
SINTAXE: use APENAS o schema; SOMENTE o SQL, sem ``` e sem explicação; apenas SELECT.

SCHEMA:
{schema}

PERGUNTA: {pergunta}{corr}

SQL:"""

def _prompt_resposta(pergunta, colunas, linhas):
    cab = " | ".join(colunas)
    corpo = "\n".join(" | ".join(str(v) for v in l) for l in linhas[:LIMITE_LINHAS_LLM])
    return f"""Pergunta: {pergunta}

Resultado (Federal Reserve H.10 — análise histórica, não tempo real):
{cab}
{corpo}

Responda em português, DIDÁTICO e ACIONÁVEL: cite os números; se houver data, cite a data de
referência; se for taxa_usd lembre que maior=moeda mais fraca; se for tendência diga direção+magnitude.
Baseie-se SÓ nos dados acima. Não invente."""

def gerar_sql(schema, pergunta, erro=""):
    bruto = chamar_llm(_prompt_sql(schema, pergunta, erro), 0.1)
    return re.sub(r"```sql|```", "", bruto).strip()

def formatar_resposta(pergunta, colunas, linhas):
    return chamar_llm(_prompt_resposta(pergunta, colunas, linhas), 0.3)

def responder(conn, schema, pergunta):
    erro, sql = "", ""
    for tentativa in range(1, MAX_TENTATIVAS + 1):
        sql = gerar_sql(schema, pergunta, erro)
        try:
            colunas, linhas = executar_sql(conn, sql)
            if not linhas:
                return {"resposta": "A consulta rodou, mas não retornou resultados.",
                        "sql": sql, "tentativas": tentativa, "colunas": [], "linhas": []}
            texto = formatar_resposta(pergunta, colunas, linhas)
            return {"resposta": texto, "sql": sql, "tentativas": tentativa,
                    "colunas": colunas, "linhas": linhas}    # ← dados p/ plotagem
        except Exception as e:
            erro = str(e).strip().splitlines()[0]
            try: conn.rollback()
            except Exception: pass
    return {"resposta": f"Não consegui após {MAX_TENTATIVAS} tentativas. Último erro: {erro}",
            "sql": sql, "tentativas": MAX_TENTATIVAS, "colunas": [], "linhas": []}