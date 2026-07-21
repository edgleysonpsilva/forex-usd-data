## Ranking de volatilidade (curto prazo, 30 dias)

```sql
SELECT moeda_codigo, volatilidade, variacao_media_diaria
FROM variacao_cambial_30d
ORDER BY volatilidade DESC;
```
- **Gráfico:** Bar chart · X = `moeda_codigo`, Y = `volatilidade`
- **Insight:** quais moedas estão mudando mais no momento. BRL costuma liderar; moedas administradas (CNY) ficam no fundo.

---

## Valor do dólar em reais (nível da taxa, série completa)

```sql
SELECT data, taxa_usd
FROM volatilidade_movel
WHERE moeda_codigo = {{moeda}}
ORDER BY data;
```
- **Gráfico:** Line chart · X = `data`, Y = `taxa_usd`
- **Insight:** a trajetória do dólar (ex.: ~3,20 em 2015 → passa de 5,00 na pandemia). Mostra *quanto* o real desvalorizou.

---

## Volatilidade móvel 30d (a "onda" do risco, série completa)

```sql
SELECT data, volatilidade_movel_30d
FROM volatilidade_movel
WHERE moeda_codigo = {{moeda}}
ORDER BY data;
```
- **Gráfico:** Line chart · Y = `volatilidade_movel_30d` (sufixo `%`)
- **Insight:** o pico de **mar/2020**. Mostra *quão turbulento* foi o caminho.

---

## Nível do dólar por regime (pré / durante / pós-pandemia)

```sql
SELECT
  moeda_codigo,
  ROUND(AVG(CASE WHEN regime='pre_pandemia' THEN taxa_usd END)::numeric, 3) AS "R$ pré-pandemia",
  ROUND(AVG(CASE WHEN regime='pandemia'     THEN taxa_usd END)::numeric, 3) AS "R$ durante",
  ROUND(AVG(CASE WHEN regime='pos_pandemia' THEN taxa_usd END)::numeric, 3) AS "R$ pós-pandemia"
FROM fato_taxas_historico
WHERE moeda_codigo = 'BRL'
GROUP BY moeda_codigo;
```
- **Gráfico:** Bar chart (3 barras) ou Table
- **Insight:** o dólar médio saltou de patamar entre os regimes (ex.: ~3,7 → ~5,2 → ~5,3). Quantifica a **mudança de nível** do real.
