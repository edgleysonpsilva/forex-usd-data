# 📈 Pipeline de Dados Cambiais

![Databricks](https://img.shields.io/badge/Databricks-Free_Serverless-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medalhão-00ADD8?logo=delta&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-PostgreSQL-3ECF8E?logo=supabase&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-Window_Functions-4169E1?logo=postgresql&logoColor=white)
![Status](https://img.shields.io/badge/status-concluído-2ea44f)
![License](https://img.shields.io/badge/license-MIT-blue)

> Pipeline de engenharia de dados **end-to-end** que coleta cotações de câmbio reais, processa em arquitetura Medalhão (Bronze → Silver → Gold) e entrega métricas prontas para análise num banco PostgreSQL — tudo rodando no tier gratuito do Databricks.

---

## Em resumo

Este projeto pega dados de câmbio de fontes públicas, refina em camadas de qualidade crescente e serve o resultado para dashboards e automações. Nada revolucionário no *tema* — o valor está em **como** foi construído: modelagem dimensional, window functions, idempotência, gestão de segredos e, principalmente, **decisões de engenharia para contornar as limitações de um ambiente gratuito com rede restrita**.

Spoiler: metade do aprendizado aqui foi descobrir o que *não* funciona no Databricks Free e engenheirar em cima disso. Está tudo documentado abaixo.

### O que ele entrega
- 📊 **11 moedas** rastreadas (BRL, EUR, GBP, JPY, MXN, CAD, AUD, CHF, CNY, INR, KRW) com **dados reais** do Federal Reserve
- 🥇 **Métricas de negócio**: variação diária, volatilidade, ranking de desvalorização, correlação entre moedas e alertas de queda
- 🗄️ **4 tabelas servidas** num PostgreSQL (Supabase), prontas para BI
- ⚙️ **Pipeline orquestrado** — roda de ponta a ponta com um clique

---

## Soluções principais

| Desafio | Solução |
|---|---|
| API de câmbio original (Frankfurter) **bloqueada por DNS** no Free | Migração para o dataset oficial do **Federal Reserve H.10** hospedado no GitHub (acessível) |
| Necessidade de **histórico versionado** das taxas | **SCD Tipo 2 com threshold** — só versiona variações > 2%, evitando inflar a dimensão |
| Cálculo de **variação temporal** sem gambiarra | **Window functions** (`LAG`, `RANK`, `ROW_NUMBER`) sobre a tabela fato |
| **Reexecução** não pode duplicar dados | **MERGE idempotente** (upsert por chave) no fato e na dimensão |
| Escrita no Supabase **bloqueada** (JDBC + conexão direta) | Conector nativo `postgresql` via **Connection Pooler (Session mode)** |
| **Credenciais** fora do código | Senha no **Databricks Secrets**, lida em runtime |

---

## Resultados

O pipeline não só roda — produz insights **coerentes com a realidade econômica**, o que serve de validação:

| Moeda | Volatilidade | Interpretação |
|---|---|---|
| 🇧🇷 BRL | **0.85** (maior) | Real reflete o prêmio de risco emergente |
| 🇰🇷 KRW | 0.76 | Won sensível a tensões na Ásia |
| 🇲🇽 MXN | 0.54 | Peso mexicano, emergente |
| ... | ... | ... |
| 🇨🇳 CNY | **0.16** (menor) | Yuan é câmbio *administrado* pela China |

> O CNY aparecer como o **menos volátil** e o BRL como o **mais volátil** é exatamente o que a teoria prevê — um bom *sanity check* de que os dados e cálculos estão corretos.

---

## Arquitetura

```
   FONTES              INGESTÃO           REFINO            MÉTRICAS          ENTREGA
┌───────────┐      ┌────────────┐    ┌─────────────┐   ┌──────────────┐  ┌────────────┐
│ open.er   │─────▶│ 🥉 BRONZE  │───▶│ 🥈 SILVER   │──▶│ 🥇 GOLD      │─▶│  Supabase  │
│ -api.com  │      │ taxas_     │    │ dim (SCD2)  │   │ variacao_30d │  │ PostgreSQL │
│ (snapshot)│      │ atual_raw  │    │ + fato      │   │ ranking      │  │            │
├───────────┤      │ taxas_     │    │ (MERGE)     │   │ alertas      │  │ (pooler,   │
│ Fed H.10  │─────▶│ historico_ │    │             │   │ correlacao   │  │  session)  │
│ (GitHub)  │      │ raw        │    │             │   │              │  │            │
└───────────┘      └────────────┘    └─────────────┘   └──────────────┘  └────────────┘
                        │                  │                  │
                        └──────── orquestração via %run (nb_05_master) ────────┘
```

**11 moedas:** BRL, EUR, GBP, JPY, MXN, CAD, AUD, CHF, CNY, INR, KRW
*(majors · Ásia · commodity currencies · LatAm)*

---

| Notebook | Camada | Papel |
|---|---|---|
| `nb_00_config` | — | Configuração central (schemas, moedas, HTTP, helpers) |
| `nb_01_bronze` | 🥉 | Ingestão crua das 2 fontes + metadados de linhagem |
| `nb_02_silver` | 🥈 | Dimensão SCD2 + tabela fato (MERGE idempotente) |
| `nb_03_gold` | 🥇 | Métricas com window functions |
| `nb_04_data_serving` | — | Export para o Supabase (+ fallback CSV) |
| `nb_05_master` | — | Orquestração ponta a ponta |

---

## Como rodar

1. Importe a pasta `notebooks/` (`nb_00` a `nb_05`) num workspace Databricks
2. Configure o secret da senha do Supabase:
```bash
   databricks secrets put --scope infisical --key postgres_password --string-value "SUA_SENHA"
```
3. Rode o `nb_05_master` — ele executa todo o pipeline em sequência
4. Confira as tabelas no SQL Editor do Supabase

> **Rodando localmente (fora do Databricks):** instale as dependências com `pip install -r requirements.txt`. Note que isso cobre `pyspark` e `delta-spark` para testes locais — no Databricks Free essas bibliotecas já vêm prontas no runtime, então esse passo só é necessário se você quiser rodar partes do pipeline fora da plataforma.

> **Stack:** Databricks Free· PySpark · Delta Lake · SQL · Supabase (PostgreSQL)

---

<details>
<summary><h2> A história técnica (para quem quer aprofundar)</h2></summary>

Aqui é onde o projeto fica interessante de verdade. O tema (câmbio) é só o pano de fundo — o aprendizado real veio de **bater de frente com as limitações do Databricks Free Serverless** e engenheirar soluções. Segue a jornada honesta, com os becos sem saída incluídos.

### Fonte de dados

O plano original era usar a **Frankfurter API** para o histórico de câmbio. Não funcionou. Ao investigar, descobri que o egress do Free Serverless resolve DNS apenas para uma *allowlist* mínima — um `socket.gethostbyname()` revelou `DNS FAIL` para praticamente todas as APIs de câmbio (Frankfurter, exchangerate.host, currencyapi, fixer, openexchangerates) **e** para CDNs populares (jsdelivr, Cloudflare Pages).

Tentei então a Currency-API via CDN — mesmo bloqueio. Fui atrás do repositório de origem no GitHub e descobri que ele **não commita** os JSONs (só publica no npm/CDN). Beco sem saída.

O que **funcionava**? Apenas dois domínios de câmbio: `open.er-api.com` (snapshot atual) e — a virada de chave — o **GitHub** (`raw.githubusercontent.com` e `api.github.com`). Isso abriu o caminho: o **dataset oficial do Federal Reserve H.10**, hospedado no GitHub via datahub.io, tem histórico diário real. Migrei para ele.

> **Lição:** documentar a limitação de rede (com o diagnóstico de DNS direto no notebook) virou um dos pontos mais fortes do projeto. Mostra investigação empírica, não tentativa-e-erro cega.

### Tratando o dado do Fed

O CSV do Fed tem particularidades que exigiram tratamento:
- Usa **nomes de país** ("Brazil", "South Korea"), não códigos ISO → mapa de tradução
- **EUR, GBP e AUD** vêm cotadas de forma invertida (USD por moeda) → normalização com `1/taxa`
- A janela de "30 dias" é ancorada nas **datas de negociação reais** (não dias de calendário), evitando buracos de fim de semana/feriado

Cobertura honesta: o Fed H.10 tem 11 das 12 moedas que eu queria. ARS, CLP e COP não têm histórico público diário gratuito acessível — foram documentadas como fora de escopo em vez de inventar dados.

### Entrega ao Supabase

Escrever no Supabase foi uma sequência de obstáculos, cada um com sua solução:

1. **`InfisicalSDKClient` para buscar a senha** → `app.infisical.com` bloqueado por DNS. Migrei a senha para o **Databricks Secrets** (alimentado uma vez via Infisical CLI local).
2. **`.format("jdbc")`** → `UNSUPPORTED_DATA_SOURCE_WRITE`. O Serverless bloqueia o JDBC genérico, mas aceita o conector nativo **`.format("postgresql")`**.
3. **`.option("sslmode", "require")`** → não suportado no conector nativo. Removido (SSL é automático).
4. **Host `db.xxx.supabase.co:5432`** → `gaierror` (DNS bloqueado). Só o **Connection Pooler** (`aws-1-...pooler.supabase.com`) resolve.
5. **Senha via `os.environ`** → `SCRAM... empty password`. O Serverless ignora env vars de cluster; passei a usar `dbutils.secrets.get()`.
6. **Pooler porta 6543 (transaction)** → `prepared statement "S_1" already exists`. Troquei para a **porta 5432 (session mode)** + `coalesce(1)` para serializar a escrita.

Depois disso: **4/4 tabelas exportadas com sucesso**.

> **Lição:** cada erro trouxe um conceito novo (data sources permitidos no Serverless, connection pooling, modos transaction vs session do PgBouncer/Supavisor). O stacktrace é seu amigo — a solução quase sempre estava na primeira linha do `Caused by`.

### Conceitos de engenharia aplicados

- **Arquitetura Medalhão** — separação em Bronze/Silver/Gold para rastreabilidade e reprocessamento seletivo
- **Modelagem dimensional** — tabela fato (`fato_taxas_historico`) + dimensão (`dim_moeda_cambio`)
- **SCD Tipo 2 com threshold** — histórico de versões, só para mudanças significativas
- **Window functions** — `LAG` (variação diária), `RANK` (ranking), `ROW_NUMBER` (dia da variação máxima)
- **Idempotência** — `MERGE`/upsert garante que reexecutar não duplica
- **Data quality** — validações `assert_not_empty` e sanity checks de domínio
- **Data lineage** — colunas `_ingested_at`, `_pipeline_run`, `_origem_dados`, `_source_api`
- **Secrets management** — credenciais no cofre, nunca no código
- **Graceful degradation** — fallback automático em CSV/Volume se a rede falhar

### Decisões de escopo

O desafio original pedia uma correlação BRL × commodities via API externa — bloqueada por DNS. **Adaptei** para uma correlação intra-dataset (cada moeda vs. BRL, via `F.corr`), entregando o valor analítico (medir relação entre ativos) por um caminho viável. Trade-off consciente e documentado.

</details>

---

## Estrutura do repositório

```
.
├── assets/
│   └── architecture.png     # diagrama da arquitetura
├── notebooks/
│   ├── nb_00_config.py      # configuração central
│   ├── nb_01_bronze.py      # ingestão (snapshot + histórico Fed)
│   ├── nb_02_silver.py      # SCD2 + fato
│   ├── nb_03_gold.py        # métricas (LAG, RANK, correlação)
│   ├── nb_04_data_serving.py # export → Supabase
│   └── nb_05_master.py      # orquestração
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Próximos Passos

- [ ] **Dashboard** de visualização (Metabase) conectado ao Supabase
- [ ] **Notificação diária** por e-mail (GitHub Actions cron + Resend) lendo o data mart
- [ ] **Expandir a janela histórica** para correlações mais robustas

---

## Notas

- Ambiente: **Databricks Free**
- Dados históricos: **Federal Reserve H.10** (fonte oficial, domínio público)
- Este é um projeto de **portfólio** — foco em boas práticas de engenharia, não em complexidade artificial