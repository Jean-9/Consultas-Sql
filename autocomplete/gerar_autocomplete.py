from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import URL
from dotenv import load_dotenv
import os
import json

load_dotenv()

def gerar_autocomplete():
    engine = create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("username_protheus"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            database=os.getenv("database"),
            query={"sslmode": "disable"}
        )
    )

    query = """
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public'
    """

    autocomplete_itens = set()

    with engine.connect() as conn:
        result = conn.execute(text(query))
        for tabela, coluna in result:
            if tabela and tabela.strip():
                autocomplete_itens.add(tabela.strip())
                autocomplete_itens.add(coluna.strip())

    # Palavras-chave SQL
    keywords = [
        "SELECT", "FROM", "WHERE", "JOIN", "INNER", "LEFT",
        "GROUP BY", "ORDER BY", "AND", "OR", "NOT"
    ]
    autocomplete_itens.update(keywords)

    return [{"value": item.upper(), "meta": "autocomplete"} for item in sorted(autocomplete_itens)]

# Executa e salva no JSON
if __name__ == "__main__":
    autocomplete = gerar_autocomplete()
    with open("autocomplete_cache.json", "w", encoding="utf-8") as f:
        json.dump(autocomplete, f, indent=2)