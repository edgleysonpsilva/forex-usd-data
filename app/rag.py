import os, json
import numpy as np
from google import genai

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
EMB_MODEL = "gemini-embedding-001"
PASTA = os.path.join(os.path.dirname(__file__), "knowledge")

_cache = {"vetores": None, "chunks": None, "origens": None}

def _carregar():
    if _cache["vetores"] is None:
        _cache["vetores"] = np.load(os.path.join(PASTA, "index.npz"))["vetores"]
        d = json.load(open(os.path.join(PASTA, "chunks.json"), encoding="utf-8"))
        _cache["chunks"], _cache["origens"] = d["chunks"], d["origens"]
    return _cache

def _embed_query(texto):
    r = client.models.embed_content(model=EMB_MODEL, contents=texto)
    return np.array(r.embeddings[0].values, dtype="float32")

def recuperar(pergunta, k=3):
    """Retorna os k trechos mais relevantes (cosine similarity)."""
    d = _carregar()
    V, q = d["vetores"], _embed_query(pergunta)
    sims = V @ q / (np.linalg.norm(V, axis=1) * np.linalg.norm(q) + 1e-9)
    top = np.argsort(-sims)[:k]
    return [{"texto": d["chunks"][i], "origem": d["origens"][i],
             "score": float(sims[i])} for i in top]