# arquivo: estoque_app.py
# App de controle de estoque com importação inteligente (merge) a partir de Excel.
# Requisitos:
#   pip install streamlit pandas openpyxl
# (se usar ambiente conda: conda install pandas openpyxl; pip install streamlit)
#
# Como rodar:
#   streamlit run estoque_app.py

import streamlit as st
import sqlite3
import pandas as pd
import io
import re
from typing import Dict, List, Tuple

st.set_page_config(page_title="Estoque - Importação Inteligente", layout="wide")

# ---------- UTILITÁRIOS ----------

def normalize_colname(name: str) -> str:
    """Normaliza o nome da coluna para um nome de coluna SQL seguro: sem acentos, espaços -> underscore, lowercase."""
    if pd.isna(name):
        return ""
    s = str(name).strip()
    # Substituir acentos por não-acento (simples): apenas mapear alguns comuns
    accents = str.maketrans("ÁÀÂÃáàâãÉÈÊéèêÍÌÎíìîÓÒÔÕóòôõÚÙÛúùûÇçÑñ", "AAAAaaaaEEEEeeeeIIIiiiOOOOooooUUUuuuCcNn")
    s = s.translate(accents)
    s = s.lower()
    # troca qualquer caracter não alfanumérico por underscore
    s = re.sub(r"[^0-9a-z]+", "_", s)
    # remover underscores duplicados
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    if s == "":
        s = "col"
    return s

def ensure_table_and_columns(conn: sqlite3.Connection, table_name: str, columns: List[Tuple[str,str]]):
    """
    Garante que a tabela exista e que contenha as colunas indicadas.
    columns: list of (colname, sql_type) where colname already normalized.
    """
    cur = conn.cursor()
    # criar tabela mínima com código se não existir
    cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    codigo TEXT PRIMARY KEY
                   )""")
    conn.commit()
    # obter colunas atuais
    cur.execute(f"PRAGMA table_info({table_name})")
    existing = {row[1] for row in cur.fetchall()}  # name is at index 1
    # adicionar colunas faltantes
    for col, ctype in columns:
        if col not in existing:
            cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {ctype}')
    conn.commit()

def get_table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]

def row_has_value(val):
    """Considera valor válido se não é NaN e não é empty string."""
    if isinstance(val, float) and pd.isna(val):
        return False
    if val is None:
        return False
    s = str(val).strip()
    return s != ""

# ---------- CONFIGURAÇÕES INICIAIS ----------

DB_FILE = "estoque.db"
TABLE = "mercadorias"

# Conectar ao DB (cria arquivo local)
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
c = conn.cursor()

# Garantir tabela base (apenas codigo inicialmente)
c.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
                codigo TEXT PRIMARY KEY
              )""")
conn.commit()

# Mapeamento de colunas "amigáveis" -> colunas normalizadas no DB
# Colunas que o usuário indicou (adaptamos):
friendly_to_normal = {
    "código": "codigo",
    "codigo": "codigo",
    "produto": "produto",
    "categoria": "categoria",
    "rua": "rua",
    "nível": "nivel",
    "nivel": "nivel",
    "prédio": "predio",
    "predio": "predio",
    "qtde": "qtde",
    "quantidade": "qtde"
}

# Garantimos colunas base no DB (tipo TEXT; qtde pode ser INTEGER)
ensure_table_and_columns(conn, TABLE, [
    ("produto", "TEXT"),
    ("categoria", "TEXT"),
    ("rua", "TEXT"),
    ("nivel", "TEXT"),
    ("predio", "TEXT"),
    ("qtde", "INTEGER")
])

# ---------- FUNÇÕES PRINCIPAIS ----------

def import_excel_inteligente(uploaded_file) -> Tuple[int,int,int]:
    """
    Faz importação inteligente:
    - adiciona colunas novas dinamicamente
    - para cada linha: se codigo existe -> atualiza somente campos preenchidos,
                  se nao existe -> insere
    Retorna (n_inseridos, n_atualizados, n_ignorados)
    """
    df = pd.read_excel(uploaded_file, dtype=object)
    # strip column names
    df.columns = [str(c).strip() for c in df.columns]
    # normalizar colnames mapeando os conhecidos e normalizando os demais
    colmap: Dict[str,str] = {}  # original -> normalized
    for orig in df.columns:
        low = orig.lower()
        if low in friendly_to_normal:
            colmap[orig] = friendly_to_normal[low]
        else:
            # gerar nome normalizado
            colmap[orig] = normalize_colname(orig)

    # garantia: 'codigo' deve existir no DataFrame original
    # procurar coluna que corresponde ao 'codigo'
    codigo_cols = [orig for orig,norm in colmap.items() if norm == "codigo"]
    if not codigo_cols:
        st.error("A planilha importada precisa ter a coluna de Código (ex.: 'Código' ou 'Código').")
        return (0,0,0)
    codigo_col = codigo_cols[0]

    # Preparar lista de colunas que vamos garantir no DB (tipo TEXT), exceto qtde -> INTEGER
    columns_to_ensure = []
    for orig, norm in colmap.items():
        if norm == "qtde":
            columns_to_ensure.append((norm, "INTEGER"))
        else:
            columns_to_ensure.append((norm, "TEXT"))
    # garantir colunas no banco
    ensure_table_and_columns(conn, TABLE, columns_to_ensure)

    # agora atualiza/insere linha a linha
    inserted = 0
    updated = 0
    ignored = 0

    db_cols = get_table_columns(conn, TABLE)  # colunas atuais no DB

    for _, row in df.iterrows():
        codigo_val = row.get(codigo_col, None)
        if codigo_val is None or (isinstance(codigo_val, float) and pd.isna(codigo_val)) or str(codigo_val).strip() == "":
            # pular linhas sem código
            ignored += 1
            continue
        codigo_val = str(codigo_val).strip()

        # checar se existe
        c.execute(f"SELECT 1 FROM {TABLE} WHERE codigo=?", (codigo_val,))
        exists = c.fetchone() is not None

        # preparar mapeamento normalized_col -> value (stripped)
        values = {}
        for orig, norm in colmap.items():
            val = row.get(orig, None)
            # normalização simples: strings strip; numerics keep
            if pd.isna(val) if isinstance(val, float) else (val is None):
                # treat as empty
                continue
            if isinstance(val, str):
                v = val.strip()
                if v == "":
                    continue
                values[norm] = v
            else:
                # numeric or others
                values[norm] = val

        # remover 'codigo' da lista de fields (não atualizamos a chave)
        if "codigo" in values:
            values.pop("codigo")

        if exists:
            # Se existir, atualizamos apenas as colunas que vieram com valor
            if not values:
                ignored += 1
                continue
            set_parts = []
            params = []
            for colname, val in values.items():
                # se coluna não está no DB (situação improvável porque garantimos), pular
                if colname not in db_cols:
                    continue
                set_parts.append(f'"{colname}" = ?')
                # garantir tipo inteiro para qtde
                if colname == "qtde":
                    try:
                        v = int(float(val))
                    except Exception:
                        # se não conseguir converter, pula esse campo
                        continue
                    params.append(v)
                else:
                    params.append(val)
            if not set_parts:
                ignored += 1
                continue
            params.append(codigo_val)
            sql = f'UPDATE {TABLE} SET {", ".join(set_parts)} WHERE codigo=?'
            c.execute(sql, tuple(params))
            updated += 1
        else:
            # Inserir novo registro: montar lista de colunas presentes + codigo
            insert_cols = ["codigo"]
            insert_vals = [codigo_val]
            for colname, val in values.items():
                if colname not in db_cols:
                    continue
                insert_cols.append(colname)
                if colname == "qtde":
                    try:
                        v = int(float(val))
                    except Exception:
                        v = None
                    insert_vals.append(v)
                else:
                    insert_vals.append(val)
            # montar placeholders
            placeholders = ", ".join(["?"] * len(insert_cols))
            cols_sql = ", ".join([f'"{c}"' for c in insert_cols])
            sql = f'INSERT INTO {TABLE} ({cols_sql}) VALUES ({placeholders})'
            c.execute(sql, tuple(insert_vals))
            inserted += 1

    conn.commit()
    return (inserted, updated, ignored)

def fetch_all_dataframe() -> pd.DataFrame:
    df = pd.read_sql_query(f"SELECT * FROM {TABLE}", conn)
    return df

def update_single_record(codigo: str, updates: Dict[str, object]) -> bool:
    """Atualiza as colunas passadas (keys devem existir no DB)."""
    if not updates:
        return False
    db_cols = get_table_columns(conn, TABLE)
    set_parts = []
    params = []
    for col, val in updates.items():
        if col not in db_cols:
            continue
        set_parts.append(f'"{col}" = ?')
        params.append(val)
    if not set_parts:
        return False
    params.append(codigo)
    sql = f'UPDATE {TABLE} SET {", ".join(set_parts)} WHERE codigo=?'
    c.execute(sql, tuple(params))
    conn.commit()
    return True

# ---------- INTERFACE STREAMLIT ----------

st.title("Controle de Estoque — Importação Inteligente")

menu = ["Cadastrar / Importar", "Consultar / Atualizar", "Exportar"]
choice = st.sidebar.selectbox("Menu", menu)

# -------------------- CADASTRO / IMPORTAÇÃO --------------------
if choice == "Cadastrar / Importar":
    st.header("Cadastro manual")
    col1, col2, col3 = st.columns(3)
    with col1:
        codigo_in = st.text_input("Código (obrigatório)")
        produto_in = st.text_input("Produto")
        categoria_in = st.text_input("Categoria")
    with col2:
        rua_in = st.selectbox("Rua", ["", "RUA A", "RUA B", "RUA C", "RUA D"])
        nivel_in = st.text_input("Nível")
        predio_in = st.text_input("Prédio")
    with col3:
        qtde_in = st.number_input("Qtde", min_value=0, step=1, value=0)
        other_label = st.text_input("Campo extra (nome) — opcional")
        other_value = st.text_input("Valor do campo extra — opcional")
    if st.button("Cadastrar manualmente"):
        if not codigo_in.strip():
            st.error("Preencha o Código.")
        else:
            # garantir coluna extra se foi preenchida
            if other_label.strip():
                norm = normalize_colname(other_label)
                ensure_table_and_columns(conn, TABLE, [(norm, "TEXT")])
                # inserir or atualizar
                c.execute(f"SELECT 1 FROM {TABLE} WHERE codigo=?", (codigo_in.strip(),))
                if c.fetchone():
                    # atualiza
                    updates = {}
                    if produto_in: updates["produto"]=produto_in
                    if categoria_in: updates["categoria"]=categoria_in
                    if rua_in: updates["rua"]=rua_in
                    if nivel_in: updates["nivel"]=nivel_in
                    if predio_in: updates["predio"]=predio_in
                    updates["qtde"] = int(qtde_in)
                    updates[norm] = other_value if other_value else None
                    update_single_record(codigo_in.strip(), updates)
                    st.success("Registro atualizado (manual).")
                else:
                    # inserir
                    # garantir coluna existe
                    cols = ["codigo","produto","categoria","rua","nivel","predio","qtde", norm]
                    vals = [codigo_in.strip(), produto_in, categoria_in, rua_in, nivel_in, predio_in, int(qtde_in), other_value if other_value else None]
                    # construir insert dinâmico
                    ensure_table_and_columns(conn, TABLE, [(norm, "TEXT")])
                    existing = get_table_columns(conn, TABLE)
                    insert_cols = []
                    insert_vals = []
                    for idx, col in enumerate(cols):
                        if col in existing:
                            insert_cols.append(f'"{col}"')
                            insert_vals.append(vals[idx])
                    placeholders = ", ".join(["?"]*len(insert_cols))
                    sql = f'INSERT INTO {TABLE} ({", ".join(insert_cols)}) VALUES ({placeholders})'
                    c.execute(sql, tuple(insert_vals))
                    conn.commit()
                    st.success("Registro inserido (manual).")
            else:
                # sem campo extra
                c.execute(f"SELECT 1 FROM {TABLE} WHERE codigo=?", (codigo_in.strip(),))
                if c.fetchone():
                    # atualizar
                    updates = {}
                    if produto_in: updates["produto"]=produto_in
                    if categoria_in: updates["categoria"]=categoria_in
                    if rua_in: updates["rua"]=rua_in
                    if nivel_in: updates["nivel"]=nivel_in
                    if predio_in: updates["predio"]=predio_in
                    updates["qtde"] = int(qtde_in)
                    update_single_record(codigo_in.strip(), updates)
                    st.success("Registro atualizado (manual).")
                else:
                    # inserir novo
                    # garantir colunas base
                    ensure_table_and_columns(conn, TABLE, [("produto","TEXT"),("categoria","TEXT"),("rua","TEXT"),("nivel","TEXT"),("predio","TEXT"),("qtde","INTEGER")])
                    sql = f'INSERT INTO {TABLE} (codigo, produto, categoria, rua, nivel, predio, qtde) VALUES (?,?,?,?,?,?,?)'
                    c.execute(sql, (codigo_in.strip(), produto_in, categoria_in, rua_in, nivel_in, predio_in, int(qtde_in)))
                    conn.commit()
                    st.success("Registro inserido (manual).")

    st.markdown("---")
    st.header("Importar planilha Excel (merge inteligente)")
    st.markdown("A planilha precisa ter a coluna de Código (nome da coluna pode ser 'Código', 'codigo', etc.). Outras colunas serão detectadas automaticamente.")
    uploaded_file = st.file_uploader("Escolha um arquivo Excel (.xlsx)", type=["xlsx"])
    if uploaded_file:
        with st.spinner("Importando e mesclando..."):
            inserted, updated, ignored = import_excel_inteligente(uploaded_file)
        st.success(f"Importação finalizada — Inseridos: {inserted}, Atualizados: {updated}, Ignorados (sem código ou sem mudanças): {ignored}")

# -------------------- CONSULTA / ATUALIZAÇÃO --------------------
elif choice == "Consultar / Atualizar":
    st.header("Consultar e atualizar registros")
    busca = st.text_input("Pesquisar por Código ou Produto (opcional)")

    if st.button("Buscar / Listar"):
        if busca.strip() == "":
            df = fetch_all_dataframe()
        else:
            df = pd.read_sql_query(f"SELECT * FROM {TABLE} WHERE codigo LIKE ? OR produto LIKE ?", conn, params=(f"%{busca}%", f"%{busca}%"))

        if df.empty:
            st.warning("Nenhum registro encontrado.")
        else:
            # mostrar tabela
            st.subheader("Lista de registros")
            st.dataframe(df)

            st.subheader("Editar registros (linha a linha)")
            # Para estabilidade, criamos um formulário por registro
            for idx, row in df.iterrows():
                codigo = row["codigo"]
                with st.form(key=f"form_{codigo}"):
                    cols = st.columns(6)
                    # preenche com valores se existirem
                    produto_val = row.to_dict().get("produto", "") or ""
                    categoria_val = row.to_dict().get("categoria", "") or ""
                    rua_val = row.to_dict().get("rua", "") or ""
                    nivel_val = row.to_dict().get("nivel", "") or ""
                    predio_val = row.to_dict().get("predio", "") or ""
                    qtde_val = row.to_dict().get("qtde", 0) if row.to_dict().get("qtde", None) is not None else 0

                    cols[0].markdown(f"**Código:** `{codigo}`")
                    produto_inp = cols[1].text_input("Produto", value=produto_val, key=f"produto_{codigo}")
                    categoria_inp = cols[2].text_input("Categoria", value=categoria_val, key=f"categoria_{codigo}")
                    rua_inp = cols[3].selectbox("Rua", ["", "RUA A", "RUA B", "RUA C", "RUA D"], index=(["", "RUA A", "RUA B", "RUA C", "RUA D"].index(rua_val) if rua_val in ["RUA A","RUA B","RUA C","RUA D"] else 0), key=f"rua_{codigo}")
                    nivel_inp = cols[4].text_input("Nível", value=nivel_val, key=f"nivel_{codigo}")
                    predio_inp = cols[5].text_input("Prédio", value=predio_val, key=f"predio_{codigo}")
                    # abaixo fora dos 6 cols
                    qtde_inp = st.number_input(f"Qtde - {codigo}", min_value=0, step=1, value=int(qtde_val), key=f"qtde_{codigo}")

                    submitted = st.form_submit_button("Atualizar este registro")
                    if submitted:
                        updates = {
                            "produto": produto_inp,
                            "categoria": categoria_inp,
                            "rua": rua_inp,
                            "nivel": nivel_inp,
                            "predio": predio_inp,
                            "qtde": int(qtde_inp)
                        }
                        # remove chaves cujo valor é None (não deve ocorrer) ou empty string se quiser preservar (decisão: atualiza com valor mesmo vazio)
                        # Aqui vamos atualizar com o que usuário forneceu (mesmo string vazia), porque usuário está editando manualmente.
                        update_single_record(codigo, updates)
                        st.success(f"Registro {codigo} atualizado.")

# -------------------- EXPORTAÇÃO --------------------
elif choice == "Exportar":
    st.header("Exportar base")
    df = fetch_all_dataframe()
    if df.empty:
        st.warning("Não há dados para exportar.")
    else:
        st.dataframe(df)
        # Excel em memória
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        st.download_button("Baixar Excel (.xlsx)", data=output, file_name="estoque_export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        st.download_button("Baixar CSV (.csv)", data=df.to_csv(index=False).encode("utf-8"), file_name="estoque_export.csv", mime="text/csv")

# ---------- FIM ----------
st.markdown("---")
st.caption("Importação inteligente: evita duplicatas por Código e atualiza apenas campos preenchidos na planilha importada.")
