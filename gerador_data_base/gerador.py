from pathlib import Path
from math import ceil
import random
from datetime import datetime, timedelta
import csv
import json


# =========================
# CONFIGURAÇÕES
# =========================
QTD_CLIENTES = 120_000
QTD_PRODUTOS = 15_000
QTD_VENDAS = 300_000

CHUNK_SIZE = 50_000
BASE_DIR = Path("data")

SEED = 42
random.seed(SEED)


# =========================
# DADOS AUXILIARES
# =========================
NOMES = [
    "Ana", "Bruno", "Carlos", "Daniela", "Eduardo", "Fernanda", "Gabriel",
    "Helena", "Igor", "Juliana", "Karina", "Leonardo", "Mariana", "Nicolas",
    "Olivia", "Paulo", "Quezia", "Rafael", "Sabrina", "Thiago", "Vanessa"
]

SOBRENOMES = [
    "Silva", "Souza", "Oliveira", "Santos", "Lima", "Pereira", "Costa",
    "Gomes", "Ribeiro", "Almeida", "Martins", "Rocha", "Barbosa"
]

CIDADES_ESTADOS = [
    ("São Paulo", "SP"),
    ("Campinas", "SP"),
    ("Rio de Janeiro", "RJ"),
    ("Niterói", "RJ"),
    ("Belo Horizonte", "MG"),
    ("Contagem", "MG"),
    ("Curitiba", "PR"),
    ("Londrina", "PR"),
    ("Porto Alegre", "RS"),
    ("Canoas", "RS"),
    ("Salvador", "BA"),
    ("Feira de Santana", "BA"),
    ("Recife", "PE"),
    ("Olinda", "PE"),
    ("Fortaleza", "CE"),
    ("Brasília", "DF"),
]

CATEGORIAS = [
    "Eletrônicos",
    "Informática",
    "Casa",
    "Cozinha",
    "Livros",
    "Esporte",
    "Escritório",
    "Automotivo",
]

PRODUTOS_BASE = [
    "Notebook", "Mouse", "Teclado", "Monitor", "Cadeira", "Mesa",
    "Luminária", "Fone", "Webcam", "HD Externo", "Cafeteira", "Liquidificador",
    "Livro Técnico", "Mochila", "Garrafa Térmica", "Impressora"
]

INTERESSES = [
    "esportes",
    "tecnologia",
    "financas",
    "viagem",
    "games",
    "filmes",
    "musica",
    "culinaria",
    "livros",
    "fitness",
    "moda",
]


# =========================
# FUNÇÕES UTILITÁRIAS
# =========================
def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def write_chunk_csv(output_file: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def chunked_write(
    table_name: str,
    fieldnames: list[str],
    row_generator,
    total_rows: int,
    chunk_size: int = CHUNK_SIZE,
) -> None:
    table_dir = BASE_DIR / table_name
    ensure_dir(table_dir)

    total_chunks = ceil(total_rows / chunk_size)

    for chunk_num in range(1, total_chunks + 1):
        rows = []
        remaining = total_rows - ((chunk_num - 1) * chunk_size)
        current_chunk_size = min(chunk_size, remaining)

        for _ in range(current_chunk_size):
            rows.append(next(row_generator))

        output_file = table_dir / f"{table_name}_{chunk_num:05d}.csv"
        write_chunk_csv(output_file, fieldnames, rows)
        print(f"[OK] {output_file} -> {len(rows)} linhas")


# =========================
# GERADORES DE TABELAS
# =========================
def generate_clientes():
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)

    for cliente_id in range(1, QTD_CLIENTES + 1):
        nome = f"{random.choice(NOMES)} {random.choice(SOBRENOMES)}"
        cidade, estado = random.choice(CIDADES_ESTADOS)
        data_cadastro = random_date(start_date, end_date)

        qtd_interesses = random.randint(1, 5)
        interesses = random.sample(INTERESSES, qtd_interesses)

        yield {
            "id_cliente": cliente_id,
            "nome": nome,
            "cidade": cidade,
            "estado": estado,
            "data_cadastro": data_cadastro.strftime("%Y-%m-%d %H:%M:%S"),
            "interesses": json.dumps(interesses, ensure_ascii=False),
        }


def generate_produtos():
    for produto_id in range(1, QTD_PRODUTOS + 1):
        produto_base = random.choice(PRODUTOS_BASE)
        categoria = random.choice(CATEGORIAS)
        preco = round(random.uniform(10, 5000), 2)

        yield {
            "id_produto": produto_id,
            "nome_produto": f"{produto_base} {produto_id}",
            "categoria": categoria,
            "preco_unitario": f"{preco:.2f}",
        }


def generate_vendas():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)

    for venda_id in range(1, QTD_VENDAS + 1):
        id_cliente = random.randint(1, QTD_CLIENTES)
        id_produto = random.randint(1, QTD_PRODUTOS)
        quantidade = random.randint(1, 10)
        valor_unitario = round(random.uniform(10, 5000), 2)
        valor_total = round(quantidade * valor_unitario, 2)
        data_venda = random_date(start_date, end_date)

        yield {
            "id_venda": venda_id,
            "id_cliente": id_cliente,
            "id_produto": id_produto,
            "data_venda": data_venda.strftime("%Y-%m-%d %H:%M:%S"),
            "quantidade": quantidade,
            "valor_unitario": f"{valor_unitario:.2f}",
            "valor_total": f"{valor_total:.2f}",
        }


# =========================
# EXECUÇÃO
# =========================
def main():
    ensure_dir(BASE_DIR)

    print("Gerando tabela: clientes")
    chunked_write(
        table_name="clientes",
        fieldnames=[
            "id_cliente",
            "nome",
            "cidade",
            "estado",
            "data_cadastro",
            "interesses",
        ],
        row_generator=generate_clientes(),
        total_rows=QTD_CLIENTES,
        chunk_size=CHUNK_SIZE,
    )

    print("\nGerando tabela: produtos")
    chunked_write(
        table_name="produtos",
        fieldnames=[
            "id_produto",
            "nome_produto",
            "categoria",
            "preco_unitario",
        ],
        row_generator=generate_produtos(),
        total_rows=QTD_PRODUTOS,
        chunk_size=CHUNK_SIZE,
    )

    print("\nGerando tabela: vendas")
    chunked_write(
        table_name="vendas",
        fieldnames=[
            "id_venda",
            "id_cliente",
            "id_produto",
            "data_venda",
            "quantidade",
            "valor_unitario",
            "valor_total",
        ],
        row_generator=generate_vendas(),
        total_rows=QTD_VENDAS,
        chunk_size=CHUNK_SIZE,
    )

    print("\nProcesso finalizado com sucesso.")


if __name__ == "__main__":
    main()
