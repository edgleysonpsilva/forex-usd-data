from agent import conectar, obter_schema, responder
conn = conectar(); schema = obter_schema(conn)
print("Agente de Câmbio — pergunte em português (ou 'sair')")
while (p := input("\nVocê: ").strip().lower()) not in {"sair","quit","q",""}:
    r = responder(conn, schema, p)
    print(f"\n🔎 SQL: {r['sql']}\n🤖 {r['resposta']}")