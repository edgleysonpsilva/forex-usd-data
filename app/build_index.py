"""Lê app/knowledge/*.md, quebra em chunks, gera embeddings e salva o índice.
Rode LOCALMENTE uma vez (e sempre que editar os documentos):
    python app/build_index.py
"""
import os, glob, json, re
import numpy as np
from google import genai

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
EMB_MODEL = "gemini-embedding-001"
PASTA = os.path.join(os.path.dirname(__file__), "knowledge")

def chunk(texto, alvo=600):
    """Quebra por parágrafos, agrupando até ~alvo caracteres (mantém contexto)."""
    paras = [p.strip() for p in re.split(r"\n\s*\n", texto) if p.strip()]
    blocos, atual = [], ""
    for p in paras:
        if len(atual) + len(p) < alvo:
            atual += "\n\n" + p
        else:
            if atual: blocos.append(atual.strip())
            atual = p
    if atual: blocos.append(atual.strip())
    return blocos

def embed(texto):
    r = client.models.embed_content(model=EMB_MODEL, contents=texto)
    return np.array(r.embeddings[0].values, dtype="float32")  # lista de vetores

def main():
    chunks, origens = [], []
    for caminho in glob.glob(os.path.join(PASTA, "*.md")):
        nome = os.path.basename(caminho)
        with open(caminho, encoding="utf-8") as f:
            for bloco in chunk(f.read()):
                chunks.append(bloco); origens.append(nome)
    if not chunks:
        raise SystemExit("Nenhum .md em app/knowledge/")
    print(f"Gerando embeddings de {len(chunks)} chunks...")
    vetores = np.array(embed(chunks), dtype="float32")
    np.savez(os.path.join(PASTA, "index.npz"), vetores=vetores)
    with open(os.path.join(PASTA, "chunks.json"), "w", encoding="utf-8") as f:
        json.dump({"chunks": chunks, "origens": origens}, f, ensure_ascii=False)
    print(f"✅ Índice salvo: {len(chunks)} chunks em {PASTA}")

if __name__ == "__main__":
    main()