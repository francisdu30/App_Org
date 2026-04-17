# =============================================================================
#  WorkSpace Manager — Streamlit Community Cloud Edition
#  Stockage : Cloudflare R2 (boto3 / S3-compatible)
#  Architecture : fichier unique, sections délimitées par des bandeaux
# =============================================================================
#
#  Secrets requis dans .streamlit/secrets.toml :
#    R2_ACCOUNT_ID = "..."
#    R2_ACCESS_KEY  = "..."
#    R2_SECRET_KEY  = "..."
#    R2_BUCKET      = "..."
#
#  Structure des objets dans R2 :
#    sessions.parquet          → annuaire utilisateurs + arborescence JSON
#    tables/<file_id>.parquet  → tableurs
#    maps/<file_id>.parquet    → maps brainstorming
#
# =============================================================================

from __future__ import annotations

import io
import json
import uuid
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import boto3
from botocore.exceptions import ClientError


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 1 — COUCHE R2 / STOCKAGE
# ══════════════════════════════════════════════════════════════════════════════

SESSIONS_KEY = "sessions.parquet"
SESSIONS_COLS = ["key", "value"]   # key=utilisateur ou "_meta_", value=JSON


@st.cache_resource
def get_r2():
    """Connexion singleton au bucket R2."""
    return boto3.client(
        "s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto",
    )


def _bucket() -> str:
    return st.secrets["R2_BUCKET"]


def load_parquet(key: str, cols: list[str]) -> pd.DataFrame:
    """Charge un fichier Parquet depuis R2. Retourne un DataFrame vide si absent."""
    try:
        obj = get_r2().get_object(Bucket=_bucket(), Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return pd.DataFrame(columns=cols)
        raise
    except Exception:
        return pd.DataFrame(columns=cols)


def save_parquet(df: pd.DataFrame, key: str) -> None:
    """Sauvegarde un DataFrame en Parquet dans R2."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    get_r2().put_object(Bucket=_bucket(), Key=key, Body=buf.getvalue())


def delete_r2_object(key: str) -> None:
    """Supprime un objet R2 (silencieux si absent)."""
    try:
        get_r2().delete_object(Bucket=_bucket(), Key=key)
    except Exception:
        pass


def list_r2_keys(prefix: str) -> list[str]:
    """Liste les clés R2 avec un préfixe donné."""
    try:
        r2 = get_r2()
        paginator = r2.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=_bucket(), Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys
    except Exception:
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 2 — DATASTORE  (sessions + arborescence + tables + maps)
# ══════════════════════════════════════════════════════════════════════════════
#
#  sessions.parquet stocke deux types de lignes :
#    key="_users_meta_"  value=JSON {username: {created_at: ...}, ...}
#    key="<username>"    value=JSON (arborescence de catégories)
#

class DataStore:

    # ── Initialisation ────────────────────────────────────────────────────────

    def __init__(self):
        """Crée sessions.parquet dans R2 s'il n'existe pas encore."""
        try:
            df = load_parquet(SESSIONS_KEY, SESSIONS_COLS)
            if df.empty or "_users_meta_" not in df["key"].values:
                self._bootstrap_sessions()
        except Exception:
            self._bootstrap_sessions()

    def _bootstrap_sessions(self):
        df = pd.DataFrame([
            {"key": "_users_meta_", "value": json.dumps({})},
        ])
        try:
            save_parquet(df, SESSIONS_KEY)
        except Exception:
            pass  # Ne pas bloquer l'app si R2 est indisponible

    # ── Lecture / écriture brute sessions ────────────────────────────────────

    def _load_sessions_df(self) -> pd.DataFrame:
        return load_parquet(SESSIONS_KEY, SESSIONS_COLS)

    def _save_sessions_df(self, df: pd.DataFrame):
        save_parquet(df, SESSIONS_KEY)

    def _get_row(self, df: pd.DataFrame, key: str) -> str | None:
        rows = df[df["key"] == key]
        return rows.iloc[0]["value"] if not rows.empty else None

    def _upsert_row(self, df: pd.DataFrame, key: str, value: str) -> pd.DataFrame:
        if key in df["key"].values:
            df = df.copy()
            df.loc[df["key"] == key, "value"] = value
        else:
            df = pd.concat(
                [df, pd.DataFrame([{"key": key, "value": value}])],
                ignore_index=True,
            )
        return df

    def _delete_row(self, df: pd.DataFrame, key: str) -> pd.DataFrame:
        return df[df["key"] != key].reset_index(drop=True)

    # ── Utilisateurs ─────────────────────────────────────────────────────────

    def get_users(self) -> list[str]:
        df = self._load_sessions_df()
        raw = self._get_row(df, "_users_meta_")
        if not raw:
            return []
        try:
            return list(json.loads(raw).keys())
        except Exception:
            return []

    def add_user(self, username: str) -> bool:
        df = self._load_sessions_df()
        raw = self._get_row(df, "_users_meta_") or "{}"
        users = json.loads(raw)
        if username in users:
            return False
        users[username] = {"created_at": str(pd.Timestamp.now())}
        df = self._upsert_row(df, "_users_meta_", json.dumps(users))
        # Initialise l'arborescence vide pour cet utilisateur
        df = self._upsert_row(df, username, json.dumps([]))
        self._save_sessions_df(df)
        return True

    def remove_user(self, username: str):
        df = self._load_sessions_df()
        raw = self._get_row(df, "_users_meta_") or "{}"
        users = json.loads(raw)
        users.pop(username, None)
        df = self._upsert_row(df, "_users_meta_", json.dumps(users))
        df = self._delete_row(df, username)
        self._save_sessions_df(df)

    # ── Arborescence de catégories ────────────────────────────────────────────

    def get_category_tree(self, username: str) -> list:
        df = self._load_sessions_df()
        raw = self._get_row(df, username)
        if not raw:
            return []
        try:
            return json.loads(raw)
        except Exception:
            return []

    def save_category_tree(self, username: str, tree: list):
        df = self._load_sessions_df()
        df = self._upsert_row(df, username, json.dumps(tree))
        self._save_sessions_df(df)

    def add_category(self, username: str, name: str, parent_id: str | None = None) -> dict:
        tree = self.get_category_tree(username)
        new_cat = {
            "id": uuid.uuid4().hex[:8],
            "name": name,
            "children": [],
            "items": [],
            "collapsed": False,
        }
        if parent_id is None:
            tree.append(new_cat)
        else:
            _tree_insert(tree, parent_id, new_cat)
        self.save_category_tree(username, tree)
        return new_cat

    def delete_category(self, username: str, cat_id: str):
        tree = self.get_category_tree(username)
        tree = _tree_remove(tree, cat_id)
        self.save_category_tree(username, tree)

    def rename_category(self, username: str, cat_id: str, new_name: str):
        tree = self.get_category_tree(username)
        _tree_rename(tree, cat_id, new_name)
        self.save_category_tree(username, tree)

    def toggle_collapsed(self, username: str, cat_id: str):
        tree = self.get_category_tree(username)
        _tree_toggle(tree, cat_id)
        self.save_category_tree(username, tree)

    def move_category(self, username: str, node_id: str, new_parent_id: str | None):
        tree = self.get_category_tree(username)
        node = _tree_find(tree, node_id)
        if node is None:
            return
        if new_parent_id and _is_descendant(node, new_parent_id):
            return  # Anti-cycle
        tree = _tree_remove(tree, node_id)
        if new_parent_id is None:
            tree.append(node)
        else:
            _tree_insert(tree, new_parent_id, node)
        self.save_category_tree(username, tree)

    # ── Items (tables/maps) dans les catégories ───────────────────────────────

    def add_item_to_category(self, username: str, cat_id: str, item: dict):
        tree = self.get_category_tree(username)
        _tree_add_item(tree, cat_id, item)
        self.save_category_tree(username, tree)

    def remove_item_from_category(self, username: str, cat_id: str, item_id: str):
        tree = self.get_category_tree(username)
        _tree_remove_item(tree, cat_id, item_id)
        self.save_category_tree(username, tree)

    # ── Génération d'ID unique ────────────────────────────────────────────────

    def generate_unique_file_id(self, prefix: str = "tbl") -> str:
        """Génère un ID de fichier non-collisionnel en vérifiant R2."""
        existing = set(
            k.replace("tables/", "").replace("maps/", "").replace(".parquet", "")
            for k in list_r2_keys("tables/") + list_r2_keys("maps/")
        )
        while True:
            uid = f"{prefix}_{uuid.uuid4().hex[:10]}"
            if uid not in existing:
                return uid

    # ── Tables ────────────────────────────────────────────────────────────────

    def create_table(self, file_id: str, breadcrumb: list[str],
                     rows: int = 20, cols: int = 10) -> pd.DataFrame:
        col_names = [f"Col_{chr(65 + i % 26)}{'_' + str(i // 26) if i >= 26 else ''}"
                     for i in range(cols)]
        data = {c: [""] * rows for c in col_names}
        df = pd.DataFrame(data)
        location = " > ".join(breadcrumb)
        df.insert(0, "_location_", [location] + [""] * (rows - 1))
        save_parquet(df, f"tables/{file_id}.parquet")
        return df

    def load_table(self, file_id: str) -> pd.DataFrame | None:
        df = load_parquet(f"tables/{file_id}.parquet", [])
        return None if df.empty and "_location_" not in df.columns else df

    def save_table(self, file_id: str, df: pd.DataFrame):
        save_parquet(df, f"tables/{file_id}.parquet")

    def delete_table(self, file_id: str):
        delete_r2_object(f"tables/{file_id}.parquet")

    # ── Maps brainstorming ────────────────────────────────────────────────────

    def create_map(self, file_id: str, breadcrumb: list[str]) -> pd.DataFrame:
        location = " > ".join(breadcrumb)
        df = pd.DataFrame([{
            "_location_": location,
            "object_id": "_meta_",
            "type": "meta",
            "label": "map",
            "coords": "{}",
        }])
        save_parquet(df, f"maps/{file_id}.parquet")
        return df

    def load_map(self, file_id: str) -> pd.DataFrame | None:
        cols = ["_location_", "object_id", "type", "label", "coords"]
        df = load_parquet(f"maps/{file_id}.parquet", cols)
        return None if df.empty and "object_id" not in df.columns else df

    def save_map(self, file_id: str, df: pd.DataFrame):
        save_parquet(df, f"maps/{file_id}.parquet")

    def delete_map(self, file_id: str):
        delete_r2_object(f"maps/{file_id}.parquet")

    # ── Utilitaires de navigation ─────────────────────────────────────────────

    def get_category_path(self, username: str, cat_id: str) -> list[str]:
        tree = self.get_category_tree(username)
        path: list[str] = []
        _find_path_names(tree, cat_id, path)
        return path

    def get_all_categories_flat(self, username: str) -> list[dict]:
        tree = self.get_category_tree(username)
        result: list[dict] = []
        _flatten_tree(tree, [], result, username)
        return result


# ── Helpers récursifs pour l'arborescence (fonctions pures) ──────────────────

def _tree_insert(nodes: list, parent_id: str, new_node: dict) -> bool:
    for n in nodes:
        if n["id"] == parent_id:
            n.setdefault("children", []).append(new_node)
            return True
        if _tree_insert(n.get("children", []), parent_id, new_node):
            return True
    return False

def _tree_remove(nodes: list, node_id: str) -> list:
    result = []
    for n in nodes:
        if n["id"] == node_id:
            continue
        n["children"] = _tree_remove(n.get("children", []), node_id)
        result.append(n)
    return result

def _tree_rename(nodes: list, node_id: str, new_name: str) -> bool:
    for n in nodes:
        if n["id"] == node_id:
            n["name"] = new_name
            return True
        if _tree_rename(n.get("children", []), node_id, new_name):
            return True
    return False

def _tree_toggle(nodes: list, node_id: str) -> bool:
    for n in nodes:
        if n["id"] == node_id:
            n["collapsed"] = not n.get("collapsed", False)
            return True
        if _tree_toggle(n.get("children", []), node_id):
            return True
    return False

def _tree_find(nodes: list, node_id: str) -> dict | None:
    for n in nodes:
        if n["id"] == node_id:
            return n
        found = _tree_find(n.get("children", []), node_id)
        if found:
            return found
    return None

def _is_descendant(node: dict, target_id: str) -> bool:
    for child in node.get("children", []):
        if child["id"] == target_id or _is_descendant(child, target_id):
            return True
    return False

def _tree_add_item(nodes: list, cat_id: str, item: dict) -> bool:
    for n in nodes:
        if n["id"] == cat_id:
            n.setdefault("items", []).append(item)
            return True
        if _tree_add_item(n.get("children", []), cat_id, item):
            return True
    return False

def _tree_remove_item(nodes: list, cat_id: str, item_id: str) -> bool:
    for n in nodes:
        if n["id"] == cat_id:
            n["items"] = [i for i in n.get("items", []) if i["id"] != item_id]
            return True
        if _tree_remove_item(n.get("children", []), cat_id, item_id):
            return True
    return False

def _find_path_names(nodes: list, target_id: str, path: list) -> bool:
    for n in nodes:
        path.append(n["name"])
        if n["id"] == target_id:
            return True
        if _find_path_names(n.get("children", []), target_id, path):
            return True
        path.pop()
    return False

def _find_path_ids(nodes: list, target_id: str, path: list) -> bool:
    for n in nodes:
        path.append(n["id"])
        if n["id"] == target_id:
            return True
        if _find_path_ids(n.get("children", []), target_id, path):
            return True
        path.pop()
    return False

def _flatten_tree(nodes: list, path: list, result: list, username: str):
    for n in nodes:
        cp = path + [n["name"]]
        result.append({"id": n["id"], "name": n["name"], "path": cp,
                        "items": n.get("items", []), "username": username})
        _flatten_tree(n.get("children", []), cp, result, username)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 3 — GESTION DU STATE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════════

def get_ds() -> DataStore:
    if "_ds" not in st.session_state:
        st.session_state._ds = DataStore()
    return st.session_state._ds


def init_state():
    defaults: dict = {
        "current_user": None,
        "current_cat_id": None,
        "current_item": None,
        "view": "folder",           # "folder" | "table" | "map"
        "undo_stack": [],
        "redo_stack": [],
        "table_df": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
    get_ds()  # Initialise le DataStore (et R2) au premier démarrage


def set_current_user(username: str | None):
    st.session_state.current_user = username
    st.session_state.current_cat_id = None
    st.session_state.current_item = None
    st.session_state.view = "folder"


def set_current_category(cat_id: str | None):
    st.session_state.current_cat_id = cat_id
    st.session_state.current_item = None
    st.session_state.view = "folder"


def open_item(item: dict):
    st.session_state.current_item = item
    st.session_state.view = item["type"]
    st.session_state.undo_stack = []
    st.session_state.redo_stack = []
    st.session_state.table_df = None


def go_back_to_folder():
    st.session_state.current_item = None
    st.session_state.view = "folder"
    st.session_state.table_df = None


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 4 — CSS GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

CSS = """
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Syne:wght@400;600;800&display=swap');

:root {
    --bg-primary: #0f0f13;
    --bg-secondary: #16161d;
    --bg-card: #1c1c26;
    --bg-hover: #22222e;
    --accent: #6c63ff;
    --accent-light: #8b85ff;
    --accent-dim: rgba(108,99,255,0.15);
    --text-primary: #e8e8f0;
    --text-secondary: #8888aa;
    --text-muted: #55556a;
    --border: #2a2a3a;
    --border-accent: rgba(108,99,255,0.4);
    --success: #4ade80;
    --danger: #f87171;
}

.stApp { background: var(--bg-primary) !important; font-family: 'JetBrains Mono', monospace !important; color: var(--text-primary) !important; }
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

section[data-testid="stSidebar"] { background: var(--bg-secondary) !important; border-right: 1px solid var(--border) !important; }

.session-header { font-family: 'Syne', sans-serif; font-weight: 800; font-size: 1.05rem; color: var(--accent-light); padding: 14px 10px 8px; border-bottom: 1px solid var(--border); letter-spacing: 0.05em; text-transform: uppercase; }
.page-title { font-family: 'Syne', sans-serif; font-size: 1.5rem; font-weight: 800; color: var(--text-primary); margin-bottom: 18px; }
.breadcrumb { display: flex; align-items: center; gap: 5px; font-size: 0.78rem; color: var(--text-muted); margin-bottom: 14px; }
.bc-item { cursor: pointer; color: var(--text-secondary); padding: 2px 5px; border-radius: 3px; }
.bc-item:hover { color: var(--accent-light); }
.bc-item.current { color: var(--accent-light); cursor: default; }
.bc-sep { color: var(--text-muted); }
.user-section-header { font-family: 'Syne', sans-serif; font-size: 0.85rem; font-weight: 700; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.1em; padding: 8px 0 4px; border-bottom: 1px solid var(--border); margin: 14px 0 8px; }

.stButton > button { font-family: 'JetBrains Mono', monospace !important; background: var(--bg-card) !important; color: var(--text-secondary) !important; border: 1px solid var(--border) !important; border-radius: 6px !important; transition: all 0.15s !important; font-size: 0.8rem !important; }
.stButton > button:hover { border-color: var(--border-accent) !important; color: var(--accent-light) !important; background: var(--accent-dim) !important; }
.stButton > button[kind="primary"] { background: var(--accent-dim) !important; color: var(--accent-light) !important; border-color: var(--border-accent) !important; }

.stTextInput > div > div > input { background: var(--bg-card) !important; color: var(--text-primary) !important; border: 1px solid var(--border) !important; border-radius: 6px !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.83rem !important; }
.stTextInput > div > div > input:focus { border-color: var(--accent) !important; box-shadow: 0 0 0 2px var(--accent-dim) !important; }
.stNumberInput > div > div > input { background: var(--bg-card) !important; color: var(--text-primary) !important; border: 1px solid var(--border) !important; border-radius: 6px !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.83rem !important; }
.stSelectbox > div > div { background: var(--bg-card) !important; color: var(--text-primary) !important; border: 1px solid var(--border) !important; border-radius: 6px !important; }
.stTextArea > div > div > textarea { background: var(--bg-card) !important; color: var(--text-primary) !important; border: 1px solid var(--border) !important; border-radius: 6px !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.8rem !important; }
.stMarkdown p { font-family: 'JetBrains Mono', monospace !important; font-size: 0.83rem !important; color: var(--text-secondary) !important; }
div[data-testid="stHorizontalBlock"] { gap: 6px !important; }

::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: var(--bg-primary); }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
"""


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 5 — BARRE LATÉRALE
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar():
    ds = get_ds()
    users = ds.get_users()
    current_user = st.session_state.get("current_user")

    st.markdown('<div class="session-header">⬡ Sessions</div>', unsafe_allow_html=True)

    # Bouton "Toutes les sessions"
    if st.button("🌐 Toutes les sessions", key="btn_all_sessions",
                 type="primary" if current_user is None else "secondary",
                 use_container_width=True):
        set_current_user(None)
        st.rerun()

    # Boutons utilisateurs
    for user in users:
        if st.button(f"👤 {user}", key=f"btn_user_{user}",
                     type="primary" if current_user == user else "secondary",
                     use_container_width=True):
            set_current_user(user)
            st.rerun()

    st.divider()

    # Créer une nouvelle session
    with st.expander("➕ Nouvelle session"):
        new_user = st.text_input("Nom", key="new_user_input",
                                 label_visibility="collapsed",
                                 placeholder="Nom d'utilisateur...")
        if st.button("Créer", key="btn_create_user", use_container_width=True):
            if new_user.strip():
                if ds.add_user(new_user.strip()):
                    set_current_user(new_user.strip())
                    st.success(f"Session '{new_user}' créée !")
                    st.rerun()
                else:
                    st.error("Ce nom existe déjà.")

    # Arborescence de catégories
    if current_user:
        st.markdown(
            '<div style="padding:8px 0 4px;font-size:0.72rem;color:#6c63ff;'
            'text-transform:uppercase;letter-spacing:0.1em;">📁 Arborescence</div>',
            unsafe_allow_html=True,
        )
        _render_nodes(ds, current_user, ds.get_category_tree(current_user), depth=0)

        st.divider()
        with st.expander("➕ Catégorie racine"):
            cat_name = st.text_input("Nom", key="new_root_cat",
                                     label_visibility="collapsed",
                                     placeholder="Nom de catégorie...")
            if st.button("Créer", key="btn_create_root_cat", use_container_width=True):
                if cat_name.strip():
                    ds.add_category(current_user, cat_name.strip())
                    st.rerun()

        st.divider()
        with st.expander("⚠️ Supprimer session"):
            st.warning(f"Supprimer « {current_user} » ?")
            if st.button("Confirmer", key="btn_del_user", type="primary"):
                ds.remove_user(current_user)
                set_current_user(None)
                st.rerun()


def _render_nodes(ds: DataStore, username: str, nodes: list,
                  depth: int, max_depth: int = 1):
    """Rendu récursif de l'arborescence (2 niveaux max visibles)."""
    current_cat = st.session_state.get("current_cat_id")

    for node in nodes:
        node_id = node["id"]
        name = node["name"]
        children = node.get("children", [])
        collapsed = node.get("collapsed", False)
        has_children = bool(children)
        is_selected = current_cat == node_id
        icon = "📁" if has_children else "📂"

        col1, col2, col3 = st.columns([0.14, 0.65, 0.21])

        with col1:
            if has_children and depth < max_depth:
                label = "▶" if collapsed else "▼"
                if st.button(label, key=f"toggle_{node_id}", help="Plier/déplier"):
                    ds.toggle_collapsed(username, node_id)
                    st.rerun()

        with col2:
            prefix = "   " * depth
            display = f"{prefix}{icon} {name}"
            if st.button(display, key=f"cat_{node_id}",
                         type="primary" if is_selected else "secondary",
                         use_container_width=True):
                set_current_category(node_id)
                st.rerun()

        with col3:
            with st.popover("⋯"):
                # Ajouter une sous-catégorie
                sub = st.text_input("Sous-cat.", key=f"sub_{node_id}",
                                    placeholder="Nom...", label_visibility="collapsed")
                if st.button("➕ Ajouter", key=f"addsub_{node_id}"):
                    if sub.strip():
                        ds.add_category(username, sub.strip(), parent_id=node_id)
                        st.rerun()

                # Renommer
                new_name = st.text_input("Renommer", key=f"ren_{node_id}",
                                         value=name, label_visibility="collapsed")
                if st.button("✏️ Renommer", key=f"doRen_{node_id}"):
                    if new_name.strip() and new_name.strip() != name:
                        ds.rename_category(username, node_id, new_name.strip())
                        st.rerun()

                # Déplacer
                all_cats = ds.get_all_categories_flat(username)
                opts = {c["name"]: c["id"] for c in all_cats if c["id"] != node_id}
                opts["(Racine)"] = None
                target = st.selectbox("Déplacer vers", list(opts.keys()),
                                      key=f"mv_{node_id}")
                if st.button("↗️ Déplacer", key=f"doMv_{node_id}"):
                    ds.move_category(username, node_id, opts[target])
                    st.rerun()

                st.divider()
                if st.button("🗑️ Supprimer", key=f"del_{node_id}", type="primary"):
                    ds.delete_category(username, node_id)
                    if current_cat == node_id:
                        set_current_category(None)
                    st.rerun()

        # Enfants (max 2 niveaux visibles)
        if has_children and not collapsed and depth < max_depth:
            _render_nodes(ds, username, children, depth + 1, max_depth)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 6 — VUE TOUTES LES SESSIONS
# ══════════════════════════════════════════════════════════════════════════════

def render_all_sessions():
    ds = get_ds()
    users = ds.get_users()

    st.markdown('<div class="page-title">🌐 Toutes les sessions</div>',
                unsafe_allow_html=True)

    if not users:
        st.info("Aucune session créée. Ajoutez des utilisateurs depuis la barre latérale.")
        return

    for user in users:
        st.markdown(f'<div class="user-section-header">👤 {user}</div>',
                    unsafe_allow_html=True)
        tree = ds.get_category_tree(user)
        if not tree:
            st.markdown('<p style="color:#55556a;font-size:0.8rem;padding-left:6px;">Aucune catégorie</p>',
                        unsafe_allow_html=True)
        else:
            _render_all_sessions_categories(ds, user, tree, depth=0)

        if st.button(f"→ Ouvrir session {user}", key=f"open_all_{user}"):
            set_current_user(user)
            st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)


def _render_all_sessions_categories(ds: DataStore, user: str, nodes: list, depth: int):
    for node in nodes:
        items_count = len(node.get("items", []))
        child_count = len(node.get("children", []))
        prefix = "　" * depth
        col_n, col_m, col_o = st.columns([0.55, 0.25, 0.2])
        with col_n:
            icon = "📁" if child_count else "📂"
            st.markdown(
                f'<span style="font-size:0.83rem;color:#8888aa;">{prefix}{icon} <b>{node["name"]}</b></span>',
                unsafe_allow_html=True,
            )
        with col_m:
            if items_count or child_count:
                st.markdown(
                    f'<span style="font-size:0.7rem;color:#6c63ff;">'
                    f'{items_count} fich. · {child_count} ss-cat.</span>',
                    unsafe_allow_html=True,
                )
        with col_o:
            if st.button("Ouvrir", key=f"opencat_{user}_{node['id']}"):
                set_current_user(user)
                set_current_category(node["id"])
                st.rerun()

        if depth == 0 and node.get("children"):
            _render_all_sessions_categories(ds, user, node["children"], depth=1)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 7 — VUE DOSSIER
# ══════════════════════════════════════════════════════════════════════════════

def render_folder():
    ds = get_ds()
    user = st.session_state.current_user
    cat_id = st.session_state.get("current_cat_id")

    # Breadcrumb
    _render_breadcrumb(ds, user, cat_id)

    if cat_id is None:
        st.markdown('<div class="page-title">🏠 Accueil</div>', unsafe_allow_html=True)
        st.info("Sélectionnez ou créez une catégorie dans la barre latérale.")
        return

    node = _tree_find(ds.get_category_tree(user), cat_id)
    if node is None:
        st.warning("Catégorie introuvable.")
        set_current_category(None)
        st.rerun()
        return

    st.markdown(f'<div class="page-title">📁 {node["name"]}</div>', unsafe_allow_html=True)

    # Barre d'outils
    c1, c2, c3, _ = st.columns([1, 1, 1, 3])
    with c1:
        if st.button("➕ Dossier", use_container_width=True):
            st.session_state._show_new = "folder"
    with c2:
        if st.button("📊 Table", use_container_width=True):
            st.session_state._show_new = "table"
    with c3:
        if st.button("🧠 Map", use_container_width=True):
            st.session_state._show_new = "map"

    # Dialogs de création
    show = st.session_state.get("_show_new")
    if show == "folder":
        with st.container(border=True):
            st.markdown("**➕ Nouveau sous-dossier**")
            fname = st.text_input("Nom", key="new_folder_name", placeholder="Nom...")
            ca, cb = st.columns(2)
            with ca:
                if st.button("Créer", key="confirm_folder", use_container_width=True):
                    if fname.strip():
                        ds.add_category(user, fname.strip(), parent_id=cat_id)
                        st.session_state._show_new = None
                        st.rerun()
            with cb:
                if st.button("Annuler", key="cancel_folder", use_container_width=True):
                    st.session_state._show_new = None
                    st.rerun()

    elif show == "table":
        with st.container(border=True):
            st.markdown("**📊 Nouvelle table**")
            tname = st.text_input("Nom", key="new_tbl_name", placeholder="Nom...")
            tc, tr = st.columns(2)
            with tc:
                tcols = st.number_input("Colonnes", 1, 50, 10, key="new_tbl_cols")
            with tr:
                trows = st.number_input("Lignes", 1, 500, 20, key="new_tbl_rows")
            ca, cb = st.columns(2)
            with ca:
                if st.button("Créer", key="confirm_tbl", use_container_width=True):
                    if tname.strip():
                        _create_table(ds, user, cat_id, tname.strip(), int(tcols), int(trows))
                        st.session_state._show_new = None
                        st.rerun()
            with cb:
                if st.button("Annuler", key="cancel_tbl", use_container_width=True):
                    st.session_state._show_new = None
                    st.rerun()

    elif show == "map":
        with st.container(border=True):
            st.markdown("**🧠 Nouvelle map**")
            mname = st.text_input("Nom", key="new_map_name", placeholder="Nom...")
            ca, cb = st.columns(2)
            with ca:
                if st.button("Créer", key="confirm_map", use_container_width=True):
                    if mname.strip():
                        _create_map(ds, user, cat_id, mname.strip())
                        st.session_state._show_new = None
                        st.rerun()
            with cb:
                if st.button("Annuler", key="cancel_map", use_container_width=True):
                    st.session_state._show_new = None
                    st.rerun()

    st.markdown("---")

    # Sous-dossiers
    children = node.get("children", [])
    if children:
        st.markdown('<span style="font-size:0.72rem;color:#6c63ff;text-transform:uppercase;'
                    'letter-spacing:0.1em;">📁 Sous-dossiers</span>', unsafe_allow_html=True)
        cols = st.columns(min(len(children), 4))
        for i, child in enumerate(children):
            with cols[i % 4]:
                ni = len(child.get("items", []))
                nc = len(child.get("children", []))
                with st.container(border=True):
                    st.markdown(f"**📁 {child['name']}**")
                    st.markdown(
                        f'<span style="font-size:0.7rem;color:#8888aa;">'
                        f'{ni} fichiers · {nc} sous-doss.</span>',
                        unsafe_allow_html=True,
                    )
                    if st.button("Ouvrir", key=f"open_child_{child['id']}", use_container_width=True):
                        set_current_category(child["id"])
                        st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)

    # Fichiers (tables + maps)
    items = node.get("items", [])
    if items:
        st.markdown('<span style="font-size:0.72rem;color:#6c63ff;text-transform:uppercase;'
                    'letter-spacing:0.1em;">📄 Fichiers</span>', unsafe_allow_html=True)
        cols = st.columns(min(len(items), 4))
        for i, item in enumerate(items):
            with cols[i % 4]:
                icon = "📊" if item["type"] == "table" else "🧠"
                badge = "TABLE" if item["type"] == "table" else "MAP"
                with st.container(border=True):
                    st.markdown(f'**{icon} {item["name"]}**')
                    st.markdown(
                        f'<span style="font-size:0.67rem;color:#6c63ff;">{badge}</span>',
                        unsafe_allow_html=True,
                    )
                    ca, cb = st.columns(2)
                    with ca:
                        if st.button("Ouvrir", key=f"open_item_{item['id']}", use_container_width=True):
                            open_item(item)
                            st.rerun()
                    with cb:
                        if st.button("🗑️", key=f"del_item_{item['id']}", use_container_width=True):
                            _delete_item(ds, user, cat_id, item)
                            st.rerun()

    if not children and not items:
        st.markdown(
            '<p style="color:#55556a;font-size:0.85rem;margin-top:18px;">'
            "Dossier vide. Utilisez la barre d'outils pour créer des éléments.</p>",
            unsafe_allow_html=True,
        )


def _render_breadcrumb(ds: DataStore, user: str, cat_id: str | None):
    path = ds.get_category_path(user, cat_id) if cat_id else []
    parts = ["🏠 Accueil"] + path
    html = '<div class="breadcrumb">'
    for i, p in enumerate(parts):
        cls = "bc-item current" if i == len(parts) - 1 else "bc-item"
        html += f'<span class="{cls}">{p}</span>'
        if i < len(parts) - 1:
            html += '<span class="bc-sep">›</span>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)

    if path:
        if st.button("🏠", key="bc_home", help="Retour à l'accueil"):
            set_current_category(None)
            st.rerun()


def _create_table(ds: DataStore, user: str, cat_id: str, name: str, cols: int, rows: int):
    bc = [user] + ds.get_category_path(user, cat_id)
    file_id = ds.generate_unique_file_id("tbl")
    ds.create_table(file_id, bc, rows=rows, cols=cols)
    ds.add_item_to_category(user, cat_id, {"id": file_id, "name": name, "type": "table", "file_id": file_id})


def _create_map(ds: DataStore, user: str, cat_id: str, name: str):
    bc = [user] + ds.get_category_path(user, cat_id)
    file_id = ds.generate_unique_file_id("map")
    ds.create_map(file_id, bc)
    ds.add_item_to_category(user, cat_id, {"id": file_id, "name": name, "type": "map", "file_id": file_id})


def _delete_item(ds: DataStore, user: str, cat_id: str, item: dict):
    ds.remove_item_from_category(user, cat_id, item["id"])
    if item["type"] == "table":
        ds.delete_table(item["file_id"])
    else:
        ds.delete_map(item["file_id"])


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 8 — VUE TABLEUR
# ══════════════════════════════════════════════════════════════════════════════

def render_table():
    ds = get_ds()
    item = st.session_state.get("current_item")
    if not item:
        go_back_to_folder()
        st.rerun()
        return

    file_id = item["file_id"]

    # Chargement initial dans le state
    if st.session_state.table_df is None:
        df = ds.load_table(file_id)
        if df is None or df.empty:
            st.error("Table introuvable dans R2.")
            go_back_to_folder()
            st.rerun()
            return
        st.session_state.table_df = df
        st.session_state.undo_stack = []
        st.session_state.redo_stack = []

    df: pd.DataFrame = st.session_state.table_df
    visible_cols = [c for c in df.columns if c != "_location_"]
    visible_df = df[visible_cols].copy()

    # En-tête
    cb, ct, _ = st.columns([0.14, 0.6, 0.26])
    with cb:
        if st.button("← Retour", key="tbl_back", use_container_width=True):
            ds.save_table(file_id, st.session_state.table_df)
            go_back_to_folder()
            st.rerun()
    with ct:
        st.markdown(f'<div class="page-title">📊 {item["name"]}</div>', unsafe_allow_html=True)

    # Barre d'outils
    can_undo = bool(st.session_state.undo_stack)
    can_redo = bool(st.session_state.redo_stack)
    cu, cr, cs, car, cac, _ = st.columns([0.12, 0.12, 0.11, 0.13, 0.14, 0.38])

    with cu:
        if st.button("↩ Annuler", key="tbl_undo", disabled=not can_undo,
                     help=f"Annuler ({len(st.session_state.undo_stack)})",
                     use_container_width=True):
            _tbl_undo(ds, file_id)
            st.rerun()
    with cr:
        if st.button("↪ Rétablir", key="tbl_redo", disabled=not can_redo,
                     help=f"Rétablir ({len(st.session_state.redo_stack)})",
                     use_container_width=True):
            _tbl_redo(ds, file_id)
            st.rerun()
    with cs:
        if st.button("💾 Sauv.", key="tbl_save", use_container_width=True):
            ds.save_table(file_id, st.session_state.table_df)
            st.toast("Sauvegardé dans R2 ✓", icon="✅")
    with car:
        if st.button("+ Ligne", key="tbl_addrow", use_container_width=True):
            _tbl_push_undo()
            new_row = pd.DataFrame({c: [""] for c in df.columns})
            st.session_state.table_df = pd.concat(
                [st.session_state.table_df, new_row], ignore_index=True)
            st.session_state.redo_stack = []
            st.rerun()
    with cac:
        if st.button("+ Colonne", key="tbl_addcol", use_container_width=True):
            _tbl_push_undo()
            cur = st.session_state.table_df
            idx = len([c for c in cur.columns if c != "_location_"])
            cname = f"Col_{chr(65 + idx % 26)}{'_' + str(idx // 26) if idx >= 26 else ''}"
            st.session_state.table_df[cname] = ""
            st.session_state.redo_stack = []
            st.rerun()

    st.markdown("---")
    st.markdown(
        '<span style="font-size:0.7rem;color:#55556a;">'
        '💡 Colonne <code>_location_</code> masquée — contient le chemin R2 de ce fichier.</span>',
        unsafe_allow_html=True,
    )

    edited = st.data_editor(
        visible_df,
        use_container_width=True,
        num_rows="dynamic",
        key=f"de_{file_id}",
        column_config={c: st.column_config.TextColumn(c, width="medium") for c in visible_cols},
        hide_index=False,
    )

    # Détection de changements
    if not edited.equals(visible_df):
        _tbl_push_undo()
        loc = st.session_state.table_df["_location_"].copy()
        st.session_state.table_df = edited.copy()
        diff = len(edited) - len(loc)
        if diff > 0:
            loc = pd.concat([loc, pd.Series([""] * diff)], ignore_index=True)
        elif diff < 0:
            loc = loc.iloc[:len(edited)].reset_index(drop=True)
        st.session_state.table_df.insert(0, "_location_", loc.values)
        st.session_state.redo_stack = []
        ds.save_table(file_id, st.session_state.table_df)

    nrows, ncols = visible_df.shape
    st.markdown(
        f'<div style="font-size:0.7rem;color:#55556a;margin-top:6px;">'
        f'📐 {nrows} lignes × {ncols} colonnes &nbsp;|&nbsp; '
        f'☁️ tables/{file_id}.parquet</div>',
        unsafe_allow_html=True,
    )


def _tbl_push_undo():
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    if len(st.session_state.undo_stack) > 50:
        st.session_state.undo_stack.pop(0)


def _tbl_undo(ds: DataStore, file_id: str):
    if not st.session_state.undo_stack:
        return
    st.session_state.redo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df = st.session_state.undo_stack.pop()
    ds.save_table(file_id, st.session_state.table_df)


def _tbl_redo(ds: DataStore, file_id: str):
    if not st.session_state.redo_stack:
        return
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df = st.session_state.redo_stack.pop()
    ds.save_table(file_id, st.session_state.table_df)


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 9 — VUE MAP BRAINSTORMING (canvas HTML5)
# ══════════════════════════════════════════════════════════════════════════════

def _build_canvas_html(objects_json: str, file_id: str) -> str:
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8">
<style>
  *{{box-sizing:border-box;margin:0;padding:0;}}
  body{{background:#16161d;font-family:'JetBrains Mono',monospace;color:#e8e8f0;user-select:none;overflow:hidden;}}
  #tb{{display:flex;align-items:center;gap:5px;padding:7px 12px;background:#1c1c26;border-bottom:1px solid #2a2a3a;flex-wrap:wrap;}}
  .tb{{padding:5px 11px;border-radius:5px;border:1px solid #2a2a3a;background:#22222e;color:#8888aa;font-size:.74rem;cursor:pointer;font-family:inherit;transition:all .15s;white-space:nowrap;}}
  .tb:hover{{border-color:#6c63ff;color:#8b85ff;background:rgba(108,99,255,.1);}}
  .tb.on{{border-color:rgba(108,99,255,.7);background:rgba(108,99,255,.2);color:#8b85ff;}}
  .sep{{width:1px;height:20px;background:#2a2a3a;margin:0 2px;flex-shrink:0;}}
  #zd{{font-size:.7rem;color:#55556a;min-width:42px;text-align:center;}}
  #hint{{font-size:.68rem;color:#55556a;margin-left:4px;flex:1;}}
  #st{{font-size:.68rem;color:#4ade80;}}
  #cw{{position:relative;overflow:hidden;width:100%;height:calc(100vh - 50px);
       background:#0f0f13;background-image:radial-gradient(circle,#1e1e28 1px,transparent 1px);background-size:28px 28px;}}
  #cv{{display:block;}}
  #te{{position:absolute;display:none;border:2px solid #6c63ff;background:rgba(22,22,32,.97);
       color:#e8e8f0;font-family:'JetBrains Mono',monospace;font-size:13px;resize:none;
       outline:none;padding:7px;border-radius:4px;overflow:hidden;text-align:center;line-height:1.4;}}
  #ah{{position:absolute;top:7px;right:12px;font-size:.7rem;color:#8b85ff;
       background:rgba(108,99,255,.15);padding:3px 9px;border-radius:4px;display:none;pointer-events:none;}}
</style></head>
<body>
<div id="tb">
  <button class="tb on" id="bs" onclick="setTool('select')">&#128432; Selection</button>
  <button class="tb" id="br" onclick="setTool('rect')">&#9645; Rectangle</button>
  <button class="tb" id="ba" onclick="setTool('arrow')">&#8594; Fleche</button>
  <div class="sep"></div>
  <button class="tb" onclick="zoom(.15)">&#128269;+</button>
  <div id="zd">100%</div>
  <button class="tb" onclick="zoom(-.15)">&#128269;-</button>
  <button class="tb" onclick="resetView()">&#8855;</button>
  <div class="sep"></div>
  <button class="tb" onclick="delSel()">&#128465;</button>
  <div class="sep"></div>
  <button class="tb" onclick="exportJSON()" style="border-color:rgba(108,99,255,.5);color:#8b85ff;">&#128190; Export JSON</button>
  <span id="hint">Alt+drag: panoramique</span>
  <span id="st"></span>
</div>
<div id="cw">
  <canvas id="cv"></canvas>
  <textarea id="te" maxlength="500"></textarea>
  <div id="ah">Source &#8594; cliquer la cible</div>
</div>
<script>
const cv=document.getElementById('cv'),ctx=cv.getContext('2d'),
      cw=document.getElementById('cw'),te=document.getElementById('te'),
      ah=document.getElementById('ah'),stEl=document.getElementById('st'),
      hintEl=document.getElementById('hint');
let objs={objects_json},tool='select',sc=1,ox=50,oy=50;
let selId=null,drag=false,dsx=0,dsy=0,dox=0,doy=0;
let draw=false,ds={{x:0,y:0}},dc={{x:0,y:0}};
let rsz=false,rh=null,rs=null,pan=false,px=0,py=0,pox=0,poy=0;
let eid=null,asrc=null,idc=1;
objs.forEach(o=>{{const n=parseInt(String(o.id).replace(/[^0-9]/g,''))||0;if(n>=idc)idc=n+1;}});

function resizeCv(){{cv.width=cw.clientWidth;cv.height=cw.clientHeight;render();}}
window.addEventListener('resize',resizeCv);resizeCv();

function tw(cx,cy){{return{{x:(cx-ox)/sc,y:(cy-oy)/sc}};}}
function gp(e){{const r=cv.getBoundingClientRect();return{{x:e.clientX-r.left,y:e.clientY-r.top}};}}
function hRect(o,wx,wy){{return o.type==='rectangle'&&wx>=o.x&&wx<=o.x+o.w&&wy>=o.y&&wy<=o.y+o.h;}}
function hTest(wx,wy){{for(let i=objs.length-1;i>=0;i--){{if(hRect(objs[i],wx,wy))return objs[i];}}return null;}}
function gHandles(o){{
  if(o.type!=='rectangle')return[];
  return[{{id:'se',x:o.x+o.w,y:o.y+o.h}},{{id:'e',x:o.x+o.w,y:o.y+o.h/2}},
         {{id:'s',x:o.x+o.w/2,y:o.y+o.h}},{{id:'n',x:o.x+o.w/2,y:o.y}},
         {{id:'nw',x:o.x,y:o.y}},{{id:'w',x:o.x,y:o.y+o.h/2}}];
}}
function hHandle(o,wx,wy){{const t=7/sc;for(const h of gHandles(o)){{if(Math.abs(wx-h.x)<t&&Math.abs(wy-h.y)<t)return h.id;}}return null;}}

function render(){{
  ctx.clearRect(0,0,cv.width,cv.height);
  ctx.save();ctx.translate(ox,oy);ctx.scale(sc,sc);
  objs.filter(o=>o.type==='arrow').forEach(drawArrow);
  objs.filter(o=>o.type==='rectangle').forEach(drawRect);
  if(draw&&tool==='rect'){{
    const w=dc.x-ds.x,h=dc.y-ds.y;
    ctx.save();ctx.strokeStyle='#6c63ff';ctx.fillStyle='rgba(108,99,255,.07)';
    ctx.lineWidth=1.5/sc;ctx.setLineDash([6/sc,3/sc]);
    rr(ctx,ds.x,ds.y,w,h,6/sc);ctx.fill();ctx.stroke();ctx.restore();
  }}
  ctx.restore();
  document.getElementById('zd').textContent=Math.round(sc*100)+'%';
}}

function drawRect(o){{
  const sel=o.id===selId;
  ctx.save();
  if(sel){{ctx.shadowColor='rgba(108,99,255,.55)';ctx.shadowBlur=12/sc;}}
  ctx.fillStyle=o.fill||'#1c1c36';
  ctx.strokeStyle=sel?'#6c63ff':'#3a3a5a';ctx.lineWidth=(sel?2:1)/sc;
  ctx.beginPath();rr(ctx,o.x,o.y,o.w,o.h,8/sc);ctx.fill();ctx.stroke();
  ctx.restore();
  if(o.label&&o.id!==eid){{
    ctx.save();
    const fs=Math.max(9,Math.min(14,o.w/14));
    ctx.font=fs+'px JetBrains Mono,monospace';
    ctx.fillStyle='#ddddf0';ctx.textAlign='center';ctx.textBaseline='middle';
    const pad=o.w*.1,lines=wrapText(ctx,o.label,o.w-pad*2),lh=fs*1.45,th=lines.length*lh;
    const sy=o.y+o.h/2-th/2+lh/2;
    ctx.save();ctx.beginPath();ctx.rect(o.x+4/sc,o.y+4/sc,o.w-8/sc,o.h-8/sc);ctx.clip();
    lines.forEach((l,i)=>ctx.fillText(l,o.x+o.w/2,sy+i*lh));
    ctx.restore();ctx.restore();
  }}
  if(sel){{
    ctx.save();ctx.fillStyle='#6c63ff';ctx.strokeStyle='#fff';ctx.lineWidth=.8/sc;
    gHandles(o).forEach(h=>{{ctx.beginPath();ctx.arc(h.x,h.y,5/sc,0,Math.PI*2);ctx.fill();ctx.stroke();}});
    ctx.restore();
  }}
}}
function drawArrow(o){{
  const sel=o.id===selId;
  ctx.save();ctx.strokeStyle=sel?'#9b94ff':'#6c63ff';ctx.lineWidth=2/sc;
  ctx.beginPath();ctx.moveTo(o.x1,o.y1);ctx.lineTo(o.x2,o.y2);ctx.stroke();
  const ang=Math.atan2(o.y2-o.y1,o.x2-o.x1),ah=14/sc;
  ctx.fillStyle=sel?'#9b94ff':'#6c63ff';
  ctx.beginPath();ctx.moveTo(o.x2,o.y2);
  ctx.lineTo(o.x2-ah*Math.cos(ang-.45),o.y2-ah*Math.sin(ang-.45));
  ctx.lineTo(o.x2-ah*Math.cos(ang+.45),o.y2-ah*Math.sin(ang+.45));
  ctx.closePath();ctx.fill();ctx.restore();
}}
function rr(ctx,x,y,w,h,r){{
  if(w<0){{x+=w;w=-w;}}if(h<0){{y+=h;h=-h;}}r=Math.min(r,w/2,h/2);
  ctx.beginPath();ctx.moveTo(x+r,y);ctx.lineTo(x+w-r,y);ctx.quadraticCurveTo(x+w,y,x+w,y+r);
  ctx.lineTo(x+w,y+h-r);ctx.quadraticCurveTo(x+w,y+h,x+w-r,y+h);
  ctx.lineTo(x+r,y+h);ctx.quadraticCurveTo(x,y+h,x,y+h-r);
  ctx.lineTo(x,y+r);ctx.quadraticCurveTo(x,y,x+r,y);ctx.closePath();
}}
function wrapText(ctx,text,maxW){{
  if(!text)return[''];
  const words=text.split(' ');const lines=[];let line='';
  for(const w of words){{const t=line?line+' '+w:w;if(ctx.measureText(t).width>maxW&&line){{lines.push(line);line=w;}}else{{line=t;}}}}
  if(line)lines.push(line);return lines.length?lines:[''];
}}
function openEditor(o){{
  eid=o.id;const cx=o.x*sc+ox,cy=o.y*sc+oy;
  te.style.cssText='display:block;position:absolute;left:'+cx+'px;top:'+cy+'px;width:'+o.w*sc+'px;height:'+o.h*sc+'px;font-size:'+Math.max(10,Math.min(14,o.w/14))+'px;';
  te.value=o.label||'';te.focus();render();
}}
function closeEditor(){{
  if(eid!==null){{const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;eid=null;te.style.display='none';render();}}
}}
te.addEventListener('input',()=>{{const o=objs.find(x=>x.id===eid);if(o){{o.label=te.value;render();}}}});
te.addEventListener('blur',closeEditor);
te.addEventListener('keydown',e=>{{if(e.key==='Escape')closeEditor();e.stopPropagation();}});

function setTool(t){{
  tool=t;asrc=null;ah.style.display='none';
  document.querySelectorAll('.tb[id^="b"]').forEach(b=>b.classList.remove('on'));
  const btn=document.getElementById('b'+t[0]);if(btn)btn.classList.add('on');
  closeEditor();
  const cursors={{select:'default',rect:'crosshair',arrow:'crosshair'}};
  cv.style.cursor=cursors[t]||'default';
  const hints={{select:'Clic: sel | Dbl-clic: editer | Drag: deplacer',rect:'Glisser pour creer un rectangle',arrow:'Cliquer source puis cible'}};
  hintEl.textContent=hints[t]||'';
}}

cv.addEventListener('mousedown',e=>{{
  e.preventDefault();const cp=gp(e),wp=tw(cp.x,cp.y);
  if(e.button===1||(e.button===0&&e.altKey)){{pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';return;}}
  if(e.button!==0)return;closeEditor();
  if(tool==='select'){{
    const sel=selId?objs.find(o=>o.id===selId):null;
    if(sel&&sel.type==='rectangle'){{const h=hHandle(sel,wp.x,wp.y);if(h){{rsz=true;rh=h;dsx=wp.x;dsy=wp.y;rs={{x:sel.x,y:sel.y,w:sel.w,h:sel.h}};return;}}}}
    const hit=hTest(wp.x,wp.y);
    if(hit){{selId=hit.id;if(hit.type==='rectangle'){{drag=true;dsx=wp.x;dsy=wp.y;dox=hit.x;doy=hit.y;}}}}
    else{{selId=null;pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';}}
    render();
  }}else if(tool==='rect'){{draw=true;ds={{x:wp.x,y:wp.y}};dc={{x:wp.x,y:wp.y}};
  }}else if(tool==='arrow'){{
    const hit=hTest(wp.x,wp.y);if(!hit){{asrc=null;ah.style.display='none';return;}}
    if(!asrc){{asrc=hit.id;ah.style.display='block';}}
    else if(asrc!==hit.id){{
      const src=objs.find(o=>o.id===asrc),dst=hit;
      objs.push({{id:'a'+(idc++),type:'arrow',x1:src.x+src.w/2,y1:src.y+src.h/2,x2:dst.x+dst.w/2,y2:dst.y+dst.h/2,label:'',srcId:asrc,dstId:hit.id}});
      asrc=null;ah.style.display='none';render();
    }}
  }}
}});
cv.addEventListener('mousemove',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  if(pan){{ox=pox+(cp.x-px);oy=poy+(cp.y-py);render();return;}}
  if(drag&&selId){{const o=objs.find(x=>x.id===selId);if(o&&o.type==='rectangle'){{o.x=dox+(wp.x-dsx);o.y=doy+(wp.y-dsy);syncArrows(o);render();}}}}
  if(rsz&&selId){{
    const o=objs.find(x=>x.id===selId);if(o){{
      const dx=wp.x-dsx,dy=wp.y-dsy;
      if(rh.includes('e'))o.w=Math.max(60,rs.w+dx);
      if(rh.includes('s'))o.h=Math.max(36,rs.h+dy);
      if(rh.includes('w')){{o.x=rs.x+dx;o.w=Math.max(60,rs.w-dx);}}
      if(rh.includes('n')){{o.y=rs.y+dy;o.h=Math.max(36,rs.h-dy);}}
      syncArrows(o);if(eid===o.id)openEditor(o);render();
    }}
  }}
  if(draw){{dc={{x:wp.x,y:wp.y}};render();}}
}});
cv.addEventListener('mouseup',e=>{{
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};cv.style.cursor=curs[tool]||'default';
  if(pan){{pan=false;return;}}
  if(draw){{
    draw=false;const w=dc.x-ds.x,h=dc.y-ds.y;
    if(Math.abs(w)>25&&Math.abs(h)>20){{
      const o={{id:'r'+(idc++),type:'rectangle',x:w>0?ds.x:ds.x+w,y:h>0?ds.y:ds.y+h,w:Math.abs(w),h:Math.abs(h),label:'',fill:'#1c1c36'}};
      objs.push(o);selId=o.id;
    }}render();
  }}
  if(drag||rsz){{drag=false;rsz=false;}}
}});
cv.addEventListener('dblclick',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  if(tool==='select'){{const hit=hTest(wp.x,wp.y);if(hit&&hit.type==='rectangle'){{selId=hit.id;openEditor(hit);}}}}
}});
cv.addEventListener('wheel',e=>{{
  e.preventDefault();const cp=gp(e),d=e.deltaY<0?.12:-.12;
  const ns=Math.max(.08,Math.min(6,sc+d));
  ox=cp.x-(cp.x-ox)*(ns/sc);oy=cp.y-(cp.y-oy)*(ns/sc);sc=ns;render();
}},{{passive:false}});
function zoom(d){{const cx=cv.width/2,cy=cv.height/2;const ns=Math.max(.08,Math.min(6,sc+d));ox=cx-(cx-ox)*(ns/sc);oy=cy-(cy-oy)*(ns/sc);sc=ns;render();}}
function resetView(){{sc=1;ox=50;oy=50;render();}}
function delSel(){{
  if(!selId)return;
  objs=objs.filter(o=>o.id!==selId&&o.srcId!==selId&&o.dstId!==selId);
  selId=null;render();
}}
document.addEventListener('keydown',e=>{{if(e.target===document.body&&(e.key==='Delete'||e.key==='Backspace'))delSel();}});
function syncArrows(rect){{
  objs.filter(o=>o.type==='arrow').forEach(a=>{{
    const src=objs.find(x=>x.id===a.srcId),dst=objs.find(x=>x.id===a.dstId);
    if(src){{a.x1=src.x+src.w/2;a.y1=src.y+src.h/2;}}
    if(dst){{a.x2=dst.x+dst.w/2;a.y2=dst.y+dst.h/2;}}
  }});
}}
function exportJSON(){{
  const data=objs.map(o=>{{
    let c={{}};
    if(o.type==='rectangle')c={{x:o.x,y:o.y,w:o.w,h:o.h}};
    else if(o.type==='arrow')c={{x1:o.x1,y1:o.y1,x2:o.x2,y2:o.y2,srcId:o.srcId,dstId:o.dstId}};
    return{{id:o.id,type:o.type,label:o.label||'',coords:JSON.stringify(c)}};
  }});
  const j=JSON.stringify(data);
  if(navigator.clipboard)navigator.clipboard.writeText(j).then(()=>{{stEl.textContent='Copie!';setTimeout(()=>stEl.textContent='',3000);}});
  try{{window.parent.postMessage({{type:'mapSave',file_id:'{file_id}',data:j}},'*');}}catch(e){{}}
  stEl.textContent='JSON copie — coller ci-dessous';setTimeout(()=>stEl.textContent='',4000);
}}
render();
</script></body></html>"""


def render_map():
    ds = get_ds()
    item = st.session_state.get("current_item")
    if not item:
        go_back_to_folder()
        st.rerun()
        return

    file_id = item["file_id"]
    df = ds.load_map(file_id)
    if df is None or df.empty:
        st.error("Map introuvable dans R2.")
        go_back_to_folder()
        st.rerun()
        return

    # Construire la liste d'objets JS
    objects_data: list[dict] = []
    for _, row in df.iterrows():
        if str(row.get("type", "")) == "meta":
            continue
        coords: dict = {}
        try:
            coords = json.loads(str(row.get("coords", "{}")))
        except Exception:
            pass
        obj: dict = {
            "id": str(row["object_id"]),
            "type": str(row["type"]),
            "label": str(row.get("label", "")),
        }
        if row["type"] == "rectangle":
            obj.update({"x": float(coords.get("x", 100)), "y": float(coords.get("y", 100)),
                         "w": float(coords.get("w", 180)), "h": float(coords.get("h", 100)),
                         "fill": "#1c1c36"})
        elif row["type"] == "arrow":
            obj.update({"x1": float(coords.get("x1", 0)), "y1": float(coords.get("y1", 0)),
                         "x2": float(coords.get("x2", 100)), "y2": float(coords.get("y2", 100)),
                         "srcId": coords.get("srcId"), "dstId": coords.get("dstId")})
        objects_data.append(obj)

    objects_json = json.dumps(objects_data, ensure_ascii=False)

    # En-tête
    cb, ct = st.columns([0.14, 0.86])
    with cb:
        if st.button("← Retour", key="map_back", use_container_width=True):
            go_back_to_folder()
            st.rerun()
    with ct:
        st.markdown(f'<div class="page-title">🧠 {item["name"]}</div>', unsafe_allow_html=True)

    # Panneau de sauvegarde manuelle
    with st.expander("💾 Sauvegarder la map (coller le JSON exporté)"):
        st.markdown(
            '<span style="font-size:0.77rem;color:#8888aa;">'
            "1. Cliquer <b>Export JSON</b> dans le canvas (copie dans le presse-papier)<br>"
            "2. Coller ici → Sauvegarder dans R2</span>",
            unsafe_allow_html=True,
        )
        raw = st.text_area("JSON", key=f"map_json_{file_id}", height=68,
                           placeholder='[{"id":"r1","type":"rectangle",...}]',
                           label_visibility="collapsed")
        if st.button("💾 Sauvegarder dans R2", key=f"save_map_{file_id}", use_container_width=True):
            if raw and raw.strip():
                try:
                    _save_map_objects(ds, file_id, df, json.loads(raw.strip()))
                    st.success("Map sauvegardée dans R2 !")
                    st.rerun()
                except Exception as ex:
                    st.error(f"JSON invalide : {ex}")

    components.html(_build_canvas_html(objects_json, file_id), height=640, scrolling=False)

    st.markdown(
        '<div style="font-size:0.7rem;color:#55556a;margin-top:5px;">'
        "💡 <b>Alt+Drag</b> pour panoramique &nbsp;|&nbsp; <b>Molette</b> pour zoomer &nbsp;|&nbsp; "
        "<b>Dbl-clic</b> pour éditer le texte &nbsp;|&nbsp; <b>Suppr.</b> pour effacer</div>",
        unsafe_allow_html=True,
    )


def _save_map_objects(ds: DataStore, file_id: str, original_df: pd.DataFrame, objects: list):
    location = ""
    meta = original_df[original_df["type"] == "meta"]
    if not meta.empty:
        location = str(meta.iloc[0].get("_location_", ""))

    rows = [{"_location_": location, "object_id": "_meta_", "type": "meta",
             "label": "map", "coords": "{}"}]
    for obj in objects:
        t = obj.get("type", "")
        coords: dict = {}
        if t == "rectangle":
            coords = {k: obj.get(k, 0) for k in ("x", "y", "w", "h")}
        elif t == "arrow":
            coords = {k: obj.get(k) for k in ("x1", "y1", "x2", "y2", "srcId", "dstId")}
        rows.append({"_location_": "", "object_id": str(obj.get("id", "")),
                     "type": t, "label": str(obj.get("label", "")),
                     "coords": json.dumps(coords)})
    ds.save_map(file_id, pd.DataFrame(rows))


# ══════════════════════════════════════════════════════════════════════════════
#  SECTION 10 — POINT D'ENTRÉE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="WorkSpace Manager",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)
    init_state()

    with st.sidebar:
        render_sidebar()

    current_user = st.session_state.get("current_user")
    view = st.session_state.get("view", "folder")

    if current_user is None:
        render_all_sessions()
    elif view == "table":
        render_table()
    elif view == "map":
        render_map()
    else:
        render_folder()


if __name__ == "__main__":
    main()
