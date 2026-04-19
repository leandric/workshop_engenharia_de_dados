
# 🚀 Workshop Engenharia de Dados

Este projeto simula um fluxo completo de **ingestão e processamento de dados**, reproduzindo cenários comuns do dia a dia em engenharia de dados.

O pipeline contempla desde a coleta de arquivos em um servidor SFTP até o carregamento em bancos de dados.


---

## 🧠 Arquitetura do fluxo

```text
SFTP → RAW → TRANSFORM → EXPORT → LOAD (MySQL / BigQuery)
````

---

## 📁 Estrutura do projeto

```text
datasets/
  ├── raw/        # dados brutos (ingestão do SFTP)
  ├── exports/    # dados tratados/exportados

gerador_data_base/
  └── data/       # dados fictícios gerados

SFTP/
  └── docker/     # ambiente SFTP local
```

---

## ⚙️ Tecnologias utilizadas

* **Python 3.14.4**
* **Polars** → processamento de dados (lazy/eager)
* **fsspec** → acesso ao SFTP
* **Docker** → servidor SFTP local
* **SQLAlchemy** → integração com bancos de dados
* **MySQL** → banco relacional
* **BigQuery** → cloud

---

## 🔄 Etapas do pipeline

### 1. Geração de dados fictícios

Script responsável por criar dados simulados de:

* Clientes
* Produtos
* Vendas

```bash
python gerador_data_base/gerador.py
```

---

### 2. Upload para o SFTP

Os arquivos gerados devem ser enviados para o servidor SFTP.

Ferramentas recomendadas:

* FileZilla
* WinSCP
* SCP

---

### 3. Ingestão de dados

Leitura dos arquivos diretamente do SFTP ou do diretório local (`datasets/raw`).

---

### 4. Transformação

Tratamentos realizados com **Polars**, incluindo:

* Conversão de tipos
* Normalização de dados
* Explosão de listas
* Junções entre datasets

---

### 5. Exportação

Os dados tratados podem ser exportados para:

* CSV
* Parquet
* JSON
* Excel

---

### 6. Carga em banco de dados

Suporte para carga em:

* **MySQL**
* **BigQuery**

Inclui:

* criação automática de tabelas
* inferência de schema
* inserção de dados

---

## 🚀 Como executar

### 1. Instalar dependências

```bash
uv sync
```

---

### 2. Subir o servidor SFTP

```bash
cd SFTP/docker
make up
```

**Credenciais:**

* usuário: `teste`
* senha: `teste`

---

### 3. Gerar dados

```bash
python gerador_data_base/gerador.py
```

---

### 4. Enviar arquivos para o SFTP

Envie os arquivos da pasta:

```text
gerador_data_base/data/
```

para o diretório `/upload` no SFTP.

---

### 5. Executar pipeline

Execute os scripts de ingestão e transformação conforme o fluxo desejado.

---

## ⚠️ Observações

* O projeto foi desenvolvido para ambiente **Linux**
* No Windows, recomenda-se uso de **WSL**
* Para gerenciamento de versões do Python, recomenda-se **pyenv**

---

## 🧠 Conceitos abordados

* Data Ingestion
* Data Lake (RAW layer)
* Transformação de dados (ETL/ELT)
* Lazy evaluation com Polars
* Integração com SFTP
* Carga em bancos