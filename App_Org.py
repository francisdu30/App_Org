# =============================================================================
#  WorkSpace Manager — Streamlit Community Cloud Edition  v2
#  Stockage : Cloudflare R2 (boto3 / S3-compatible)
# =============================================================================
#  Secrets requis dans .streamlit/secrets.toml :
#    R2_ACCOUNT_ID = "..."   R2_ACCESS_KEY = "..."
#    R2_SECRET_KEY = "..."   R2_BUCKET     = "..."
# =============================================================================
#  Structure R2 :
#    sessions.parquet          → {key, value} : users + arbos JSON
#    tables/<id>.parquet       → tableurs (col _location_ cachée)
#    maps/<id>.parquet         → maps brainstorming
# =============================================================================

from __future__ import annotations
import io, json, uuid, hashlib
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import boto3
from botocore.exceptions import ClientError


# ══════════════════════════════════════════════════════════════════════════════
#  §1 — COUCHE R2
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_r2():
    return boto3.client("s3",
        endpoint_url=f"https://{st.secrets['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=st.secrets["R2_ACCESS_KEY"],
        aws_secret_access_key=st.secrets["R2_SECRET_KEY"],
        region_name="auto")

def _bkt(): return st.secrets["R2_BUCKET"]

def r2_load(key: str, cols: list[str]) -> pd.DataFrame:
    try:
        obj = get_r2().get_object(Bucket=_bkt(), Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey","404"):
            return pd.DataFrame(columns=cols)
        raise
    except Exception:
        return pd.DataFrame(columns=cols)

def r2_save(df: pd.DataFrame, key: str):
    buf = io.BytesIO(); df.to_parquet(buf, index=False); buf.seek(0)
    get_r2().put_object(Bucket=_bkt(), Key=key, Body=buf.getvalue())

def r2_delete(key: str):
    try: get_r2().delete_object(Bucket=_bkt(), Key=key)
    except Exception: pass

def r2_list(prefix: str) -> list[str]:
    try:
        pag = get_r2().get_paginator("list_objects_v2")
        return [o["Key"] for p in pag.paginate(Bucket=_bkt(),Prefix=prefix)
                for o in p.get("Contents",[])]
    except Exception: return []


# ══════════════════════════════════════════════════════════════════════════════
#  §2 — CACHE SESSION (évite les aller-retours R2 répétés par render)
# ══════════════════════════════════════════════════════════════════════════════
#
#  Principe : on charge sessions.parquet une seule fois par "version"
#  (hash du contenu). Toute écriture invalide le cache local.
#  Résultat : la sidebar ne fait plus 3-4 requêtes R2 par rerun.

SESSIONS_KEY = "sessions.parquet"
SESSIONS_COLS = ["key","value"]

def _sessions_cache_load() -> pd.DataFrame:
    """Charge depuis R2 ET met en cache session_state."""
    df = r2_load(SESSIONS_KEY, SESSIONS_COLS)
    st.session_state._sess_cache = df
    st.session_state._sess_dirty = False
    return df

def _sessions_get() -> pd.DataFrame:
    """Retourne le cache ou charge depuis R2."""
    if "_sess_cache" not in st.session_state:
        return _sessions_cache_load()
    return st.session_state._sess_cache

def _sessions_save(df: pd.DataFrame):
    """Sauvegarde dans R2 et met à jour le cache."""
    r2_save(df, SESSIONS_KEY)
    st.session_state._sess_cache = df

def _sess_get_row(df: pd.DataFrame, key: str) -> str | None:
    rows = df[df["key"]==key]
    return rows.iloc[0]["value"] if not rows.empty else None

def _sess_upsert(df: pd.DataFrame, key: str, value: str) -> pd.DataFrame:
    if key in df["key"].values:
        df = df.copy(); df.loc[df["key"]==key,"value"] = value
    else:
        df = pd.concat([df, pd.DataFrame([{"key":key,"value":value}])], ignore_index=True)
    return df

def _sess_delete(df: pd.DataFrame, key: str) -> pd.DataFrame:
    return df[df["key"]!=key].reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  §3 — DATASTORE
# ══════════════════════════════════════════════════════════════════════════════

class DataStore:

    def __init__(self):
        try:
            df = _sessions_get()
            if df.empty or "_users_meta_" not in df["key"].values:
                self._bootstrap()
        except Exception: self._bootstrap()

    def _bootstrap(self):
        df = pd.DataFrame([{"key":"_users_meta_","value":"{}"}])
        try: _sessions_save(df)
        except Exception: pass

    # ── Utilisateurs ──────────────────────────────────────────────────────────

    def get_users(self) -> list[str]:
        raw = _sess_get_row(_sessions_get(), "_users_meta_")
        try: return list(json.loads(raw or "{}").keys())
        except: return []

    def add_user(self, username: str) -> bool:
        df = _sessions_get()
        users = json.loads(_sess_get_row(df,"_users_meta_") or "{}")
        if username in users: return False
        users[username] = {"created_at": str(pd.Timestamp.now())}
        df = _sess_upsert(df,"_users_meta_",json.dumps(users))
        df = _sess_upsert(df, username, json.dumps([]))
        _sessions_save(df); return True

    def remove_user(self, username: str):
        df = _sessions_get()
        users = json.loads(_sess_get_row(df,"_users_meta_") or "{}")
        users.pop(username,None)
        df = _sess_upsert(df,"_users_meta_",json.dumps(users))
        df = _sess_delete(df, username)
        _sessions_save(df)

    # ── Arborescence ──────────────────────────────────────────────────────────

    def get_tree(self, username: str) -> list:
        raw = _sess_get_row(_sessions_get(), username)
        try: return json.loads(raw or "[]")
        except: return []

    def _save_tree(self, username: str, tree: list):
        df = _sessions_get()
        df = _sess_upsert(df, username, json.dumps(tree))
        _sessions_save(df)

    def add_category(self, username: str, name: str, parent_id: str|None=None) -> dict:
        tree = self.get_tree(username)
        node = {"id":uuid.uuid4().hex[:8],"name":name,"children":[],"items":[],"collapsed":False}
        if parent_id is None: tree.append(node)
        else: _t_insert(tree, parent_id, node)
        self._save_tree(username, tree); return node

    def delete_category(self, username: str, cat_id: str):
        tree = _t_remove(self.get_tree(username), cat_id)
        self._save_tree(username, tree)

    def rename_category(self, username: str, cat_id: str, name: str):
        tree = self.get_tree(username)
        _t_rename(tree, cat_id, name); self._save_tree(username, tree)

    def toggle_collapsed(self, username: str, cat_id: str):
        tree = self.get_tree(username)
        _t_toggle(tree, cat_id); self._save_tree(username, tree)

    def move_category(self, username: str, node_id: str, new_parent: str|None):
        tree = self.get_tree(username)
        node = _t_find(tree, node_id)
        if not node: return
        if new_parent and _t_is_desc(node, new_parent): return
        tree = _t_remove(tree, node_id)
        if new_parent is None: tree.append(node)
        else: _t_insert(tree, new_parent, node)
        self._save_tree(username, tree)

    def add_item(self, username: str, cat_id: str, item: dict):
        tree = self.get_tree(username)
        _t_add_item(tree, cat_id, item); self._save_tree(username, tree)

    def remove_item(self, username: str, cat_id: str, item_id: str):
        tree = self.get_tree(username)
        _t_del_item(tree, cat_id, item_id); self._save_tree(username, tree)

    def get_path(self, username: str, cat_id: str) -> list[str]:
        tree = self.get_tree(username); path=[]
        _t_path_names(tree, cat_id, path); return path

    def get_flat(self, username: str) -> list[dict]:
        tree = self.get_tree(username); res=[]
        _t_flatten(tree,[],res,username); return res

    # ── ID unique (sans list_r2_keys — utilise uuid v4 avec vérif légère) ──

    def gen_id(self, prefix: str="tbl") -> str:
        # uuid4 hex 16 chars → collision quasi-impossible, pas besoin de lister R2
        return f"{prefix}_{uuid.uuid4().hex[:16]}"

    # ── Tables ────────────────────────────────────────────────────────────────

    def create_table(self, fid: str, bc: list[str], rows:int=20, cols:int=10) -> pd.DataFrame:
        cnames = [f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}" for i in range(cols)]
        df = pd.DataFrame({c:[""]*rows for c in cnames})
        df.insert(0,"_location_",[" > ".join(bc)]+[""]*(rows-1))
        r2_save(df, f"tables/{fid}.parquet"); return df

    def resize_table(self, fid: str, df: pd.DataFrame, new_rows: int, new_cols: int) -> pd.DataFrame:
        """Redimensionne la table (ajoute/supprime lignes et colonnes)."""
        vis = [c for c in df.columns if c!="_location_"]
        loc = df["_location_"].copy()
        data = df[vis].copy()
        # Colonnes
        cur_cols = len(vis)
        if new_cols > cur_cols:
            for i in range(cur_cols, new_cols):
                cn = f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}"
                data[cn] = ""
        elif new_cols < cur_cols:
            data = data.iloc[:, :new_cols]
        # Lignes
        cur_rows = len(data)
        if new_rows > cur_rows:
            empty = pd.DataFrame({"" if c=="_location_" else c:[""] for c in data.columns},
                                  index=range(new_rows-cur_rows))
            empty.columns = data.columns
            data = pd.concat([data, empty], ignore_index=True)
        elif new_rows < cur_rows:
            data = data.iloc[:new_rows]
        # Reconstruire avec _location_
        loc_ext = loc.reindex(range(new_rows)).fillna("")
        data.insert(0,"_location_",loc_ext.values)
        return data

    def load_table(self, fid: str) -> pd.DataFrame|None:
        df = r2_load(f"tables/{fid}.parquet",[])
        return None if (df.empty and "_location_" not in df.columns) else df

    def save_table(self, fid: str, df: pd.DataFrame):
        r2_save(df, f"tables/{fid}.parquet")

    def del_table(self, fid: str): r2_delete(f"tables/{fid}.parquet")

    # ── Maps ──────────────────────────────────────────────────────────────────

    def create_map(self, fid: str, bc: list[str]) -> pd.DataFrame:
        df = pd.DataFrame([{"_location_":" > ".join(bc),"object_id":"_meta_",
                            "type":"meta","label":"map","coords":"{}","writable":"false"}])
        r2_save(df, f"maps/{fid}.parquet"); return df

    def load_map(self, fid: str) -> pd.DataFrame|None:
        cols=["_location_","object_id","type","label","coords","writable"]
        df = r2_load(f"maps/{fid}.parquet", cols)
        # Migration: ajouter colonne writable si absente
        if "writable" not in df.columns: df["writable"]="true"
        return None if (df.empty and "object_id" not in df.columns) else df

    def save_map(self, fid: str, df: pd.DataFrame): r2_save(df, f"maps/{fid}.parquet")
    def del_map(self, fid: str): r2_delete(f"maps/{fid}.parquet")


# ── Helpers arbre ─────────────────────────────────────────────────────────────

def _t_insert(ns,pid,node):
    for n in ns:
        if n["id"]==pid: n.setdefault("children",[]).append(node); return True
        if _t_insert(n.get("children",[]),pid,node): return True
    return False

def _t_remove(ns,nid):
    r=[]
    for n in ns:
        if n["id"]==nid: continue
        n["children"]=_t_remove(n.get("children",[]),nid); r.append(n)
    return r

def _t_rename(ns,nid,name):
    for n in ns:
        if n["id"]==nid: n["name"]=name; return True
        if _t_rename(n.get("children",[]),nid,name): return True
    return False

def _t_toggle(ns,nid):
    for n in ns:
        if n["id"]==nid: n["collapsed"]=not n.get("collapsed",False); return True
        if _t_toggle(n.get("children",[]),nid): return True
    return False

def _t_find(ns,nid):
    for n in ns:
        if n["id"]==nid: return n
        f=_t_find(n.get("children",[]),nid)
        if f: return f
    return None

def _t_is_desc(node,tid):
    for c in node.get("children",[]):
        if c["id"]==tid or _t_is_desc(c,tid): return True
    return False

def _t_add_item(ns,cid,item):
    for n in ns:
        if n["id"]==cid: n.setdefault("items",[]).append(item); return True
        if _t_add_item(n.get("children",[]),cid,item): return True
    return False

def _t_del_item(ns,cid,iid):
    for n in ns:
        if n["id"]==cid: n["items"]=[i for i in n.get("items",[]) if i["id"]!=iid]; return True
        if _t_del_item(n.get("children",[]),cid,iid): return True
    return False

def _t_path_names(ns,tid,path):
    for n in ns:
        path.append(n["name"])
        if n["id"]==tid: return True
        if _t_path_names(n.get("children",[]),tid,path): return True
        path.pop()
    return False

def _t_flatten(ns,path,res,user):
    for n in ns:
        cp=path+[n["name"]]
        res.append({"id":n["id"],"name":n["name"],"path":cp,
                    "items":n.get("items",[]),"username":user})
        _t_flatten(n.get("children",[]),cp,res,user)


# ══════════════════════════════════════════════════════════════════════════════
#  §4 — STATE STREAMLIT
# ══════════════════════════════════════════════════════════════════════════════

def get_ds() -> DataStore:
    if "_ds" not in st.session_state:
        st.session_state._ds = DataStore()
    return st.session_state._ds

def init_state():
    defs = {
        "current_user":None, "current_cat_id":None, "current_item":None,
        "view":"folder",
        # Table
        "table_df":None, "table_hash":None,
        "undo_stack":[], "redo_stack":[],
        "tbl_autosave":True,
        "tbl_resize_rows":None, "tbl_resize_cols":None,
        # Map
        "map_autosave":True,
    }
    for k,v in defs.items():
        if k not in st.session_state: st.session_state[k]=v
    get_ds()

def set_user(u): st.session_state.update(current_user=u,current_cat_id=None,current_item=None,view="folder")
def set_cat(c): st.session_state.update(current_cat_id=c,current_item=None,view="folder")
def open_item(item):
    st.session_state.update(current_item=item,view=item["type"],
                             undo_stack=[],redo_stack=[],table_df=None,table_hash=None)
def go_back():
    st.session_state.update(current_item=None,view="folder",table_df=None,table_hash=None)


# ══════════════════════════════════════════════════════════════════════════════
#  §5 — CSS
# ══════════════════════════════════════════════════════════════════════════════

CSS = """
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Syne:wght@400;600;800&display=swap');
:root{--bg:#0f0f13;--bg2:#16161d;--bg3:#1c1c26;--bg4:#22222e;
  --ac:#6c63ff;--acl:#8b85ff;--acd:rgba(108,99,255,.15);
  --t1:#e8e8f0;--t2:#8888aa;--t3:#55556a;--bd:#2a2a3a;--bda:rgba(108,99,255,.4);}
.stApp{background:var(--bg)!important;font-family:'JetBrains Mono',monospace!important;color:var(--t1)!important;}
#MainMenu,footer,header{visibility:hidden;}.stDeployButton{display:none;}
section[data-testid="stSidebar"]{background:var(--bg2)!important;border-right:1px solid var(--bd)!important;}
.sh{font-family:'Syne',sans-serif;font-weight:800;font-size:1.05rem;color:var(--acl);padding:14px 10px 8px;border-bottom:1px solid var(--bd);letter-spacing:.05em;text-transform:uppercase;}
.pt{font-family:'Syne',sans-serif;font-size:1.5rem;font-weight:800;color:var(--t1);margin-bottom:16px;}
.bc{display:flex;align-items:center;gap:5px;font-size:.78rem;color:var(--t3);margin-bottom:12px;}
.bci{cursor:pointer;color:var(--t2);padding:2px 5px;border-radius:3px;}.bci:hover{color:var(--acl);}
.bci.cur{color:var(--acl);cursor:default;}.bcs{color:var(--t3);}
.ush{font-family:'Syne',sans-serif;font-size:.85rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.1em;padding:8px 0 4px;border-bottom:1px solid var(--bd);margin:14px 0 8px;}
.stButton>button{font-family:'JetBrains Mono',monospace!important;background:var(--bg3)!important;color:var(--t2)!important;border:1px solid var(--bd)!important;border-radius:6px!important;transition:all .15s!important;font-size:.8rem!important;}
.stButton>button:hover{border-color:var(--bda)!important;color:var(--acl)!important;background:var(--acd)!important;}
.stButton>button[kind="primary"]{background:var(--acd)!important;color:var(--acl)!important;border-color:var(--bda)!important;}
.stTextInput>div>div>input{background:var(--bg3)!important;color:var(--t1)!important;border:1px solid var(--bd)!important;border-radius:6px!important;font-family:'JetBrains Mono',monospace!important;font-size:.83rem!important;}
.stTextInput>div>div>input:focus{border-color:var(--ac)!important;box-shadow:0 0 0 2px var(--acd)!important;}
.stNumberInput>div>div>input{background:var(--bg3)!important;color:var(--t1)!important;border:1px solid var(--bd)!important;border-radius:6px!important;font-family:'JetBrains Mono',monospace!important;font-size:.83rem!important;}
.stSelectbox>div>div{background:var(--bg3)!important;color:var(--t1)!important;border:1px solid var(--bd)!important;border-radius:6px!important;}
.stTextArea>div>div>textarea{background:var(--bg3)!important;color:var(--t1)!important;border:1px solid var(--bd)!important;border-radius:6px!important;font-family:'JetBrains Mono',monospace!important;font-size:.8rem!important;}
.stMarkdown p{font-family:'JetBrains Mono',monospace!important;font-size:.83rem!important;color:var(--t2)!important;}
div[data-testid="stHorizontalBlock"]{gap:5px!important;}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:var(--bg);}::-webkit-scrollbar-thumb{background:var(--bd);border-radius:3px;}
.toggle-as{display:flex;align-items:center;gap:8px;font-size:.75rem;color:var(--t2);padding:4px 0;}
"""


# ══════════════════════════════════════════════════════════════════════════════
#  §6 — SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar():
    ds = get_ds()
    # Une seule lecture R2 (cache session_state)
    users = ds.get_users()
    cu = st.session_state.get("current_user")

    st.markdown('<div class="sh">⬡ Sessions</div>', unsafe_allow_html=True)

    if st.button("🌐 Toutes les sessions", key="btn_all",
                 type="primary" if cu is None else "secondary", use_container_width=True):
        set_user(None); st.rerun()

    for user in users:
        if st.button(f"👤 {user}", key=f"bu_{user}",
                     type="primary" if cu==user else "secondary", use_container_width=True):
            set_user(user); st.rerun()

    st.divider()
    with st.expander("➕ Nouvelle session"):
        nu = st.text_input("Nom", key="nu_input", label_visibility="collapsed", placeholder="Nom d'utilisateur...")
        if st.button("Créer", key="btn_nu", use_container_width=True):
            if nu.strip():
                if ds.add_user(nu.strip()): set_user(nu.strip()); st.rerun()
                else: st.error("Nom déjà utilisé.")

    if cu:
        st.markdown('<div style="padding:8px 0 4px;font-size:.72rem;color:#6c63ff;text-transform:uppercase;letter-spacing:.1em;">📁 Arborescence</div>', unsafe_allow_html=True)
        # Lecture arbre depuis cache (pas de R2 supplémentaire)
        _render_nodes(ds, cu, ds.get_tree(cu), 0)
        st.divider()
        with st.expander("➕ Catégorie racine"):
            cn = st.text_input("Nom", key="new_cat", label_visibility="collapsed", placeholder="Nom de catégorie...")
            if st.button("Créer", key="btn_nc", use_container_width=True):
                if cn.strip(): ds.add_category(cu, cn.strip()); st.rerun()
        st.divider()
        with st.expander("⚠️ Supprimer session"):
            st.warning(f"Supprimer « {cu} » ?")
            if st.button("Confirmer", key="btn_du", type="primary"):
                ds.remove_user(cu); set_user(None); st.rerun()


def _render_nodes(ds, user, nodes, depth, max_depth=1):
    cc = st.session_state.get("current_cat_id")
    for n in nodes:
        nid,name = n["id"],n["name"]
        kids = n.get("children",[])
        collapsed = n.get("collapsed",False)
        sel = cc==nid
        icon = "📁" if kids else "📂"
        c1,c2,c3 = st.columns([.13,.66,.21])
        with c1:
            if kids and depth<max_depth:
                lbl = "▶" if collapsed else "▼"
                if st.button(lbl, key=f"tgl_{nid}", help="Plier/déplier"):
                    ds.toggle_collapsed(user,nid); st.rerun()
        with c2:
            pref = "   "*depth
            if st.button(f"{pref}{icon} {name}", key=f"cat_{nid}",
                         type="primary" if sel else "secondary", use_container_width=True):
                set_cat(nid); st.rerun()
        with c3:
            with st.popover("⋯"):
                sub=st.text_input("Sous-cat.",key=f"sub_{nid}",placeholder="Nom...",label_visibility="collapsed")
                if st.button("➕",key=f"addsub_{nid}"):
                    if sub.strip(): ds.add_category(user,sub.strip(),parent_id=nid); st.rerun()
                nn=st.text_input("Renommer",key=f"ren_{nid}",value=name,label_visibility="collapsed")
                if st.button("✏️",key=f"doRen_{nid}"):
                    if nn.strip() and nn!=name: ds.rename_category(user,nid,nn.strip()); st.rerun()
                flat=ds.get_flat(user)
                opts={c["name"]:c["id"] for c in flat if c["id"]!=nid}; opts["(Racine)"]=None
                tgt=st.selectbox("Déplacer vers",list(opts.keys()),key=f"mv_{nid}")
                if st.button("↗️",key=f"doMv_{nid}"):
                    ds.move_category(user,nid,opts[tgt]); st.rerun()
                st.divider()
                if st.button("🗑️ Supprimer",key=f"del_{nid}",type="primary"):
                    ds.delete_category(user,nid)
                    if cc==nid: set_cat(None)
                    st.rerun()
        if kids and not collapsed and depth<max_depth:
            _render_nodes(ds,user,kids,depth+1,max_depth)


# ══════════════════════════════════════════════════════════════════════════════
#  §7 — VUE TOUTES SESSIONS
# ══════════════════════════════════════════════════════════════════════════════

def render_all_sessions():
    ds=get_ds(); users=ds.get_users()
    st.markdown('<div class="pt">🌐 Toutes les sessions</div>', unsafe_allow_html=True)
    if not users: st.info("Aucune session. Créez-en une depuis la barre latérale."); return
    for user in users:
        st.markdown(f'<div class="ush">👤 {user}</div>', unsafe_allow_html=True)
        tree=ds.get_tree(user)
        if not tree:
            st.markdown('<p style="color:#55556a;font-size:.8rem;padding-left:6px;">Aucune catégorie</p>', unsafe_allow_html=True)
        else: _render_all_cats(ds,user,tree,0)
        if st.button(f"→ Ouvrir session {user}",key=f"oas_{user}"):
            set_user(user); st.rerun()
        st.markdown("<br>",unsafe_allow_html=True)

def _render_all_cats(ds,user,nodes,depth):
    for n in nodes:
        ni=len(n.get("items",[])); nc=len(n.get("children",[]))
        pre="　"*depth; icon="📁" if nc else "📂"
        ca,cb,cc=st.columns([.55,.25,.2])
        with ca: st.markdown(f'<span style="font-size:.83rem;color:#8888aa;">{pre}{icon} <b>{n["name"]}</b></span>',unsafe_allow_html=True)
        with cb:
            if ni or nc: st.markdown(f'<span style="font-size:.7rem;color:#6c63ff;">{ni} fich.·{nc} ss-cat.</span>',unsafe_allow_html=True)
        with cc:
            if st.button("Ouvrir",key=f"oac_{user}_{n['id']}"):
                set_user(user); set_cat(n["id"]); st.rerun()
        if depth==0 and n.get("children"): _render_all_cats(ds,user,n["children"],1)


# ══════════════════════════════════════════════════════════════════════════════
#  §8 — VUE DOSSIER
# ══════════════════════════════════════════════════════════════════════════════

def render_folder():
    ds=get_ds(); user=st.session_state.current_user
    cat_id=st.session_state.get("current_cat_id")
    _render_bc(ds,user,cat_id)
    if cat_id is None:
        st.markdown('<div class="pt">🏠 Accueil</div>',unsafe_allow_html=True)
        st.info("Sélectionnez ou créez une catégorie dans la barre latérale."); return
    node=_t_find(ds.get_tree(user),cat_id)
    if node is None: st.warning("Catégorie introuvable."); set_cat(None); st.rerun(); return
    st.markdown(f'<div class="pt">📁 {node["name"]}</div>',unsafe_allow_html=True)

    c1,c2,c3,_=st.columns([1,1,1,3])
    with c1:
        if st.button("➕ Dossier",use_container_width=True): st.session_state._show_new="folder"
    with c2:
        if st.button("📊 Table",use_container_width=True): st.session_state._show_new="table"
    with c3:
        if st.button("🧠 Map",use_container_width=True): st.session_state._show_new="map"

    show=st.session_state.get("_show_new")
    if show=="folder":
        with st.container(border=True):
            st.markdown("**➕ Nouveau sous-dossier**")
            fn=st.text_input("Nom",key="nfn",placeholder="Nom...")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="cfn",use_container_width=True):
                    if fn.strip(): ds.add_category(user,fn.strip(),parent_id=cat_id); st.session_state._show_new=None; st.rerun()
            with b:
                if st.button("Annuler",key="xfn",use_container_width=True): st.session_state._show_new=None; st.rerun()
    elif show=="table":
        with st.container(border=True):
            st.markdown("**📊 Nouvelle table**")
            tn=st.text_input("Nom",key="ntn",placeholder="Nom...")
            tc,tr=st.columns(2)
            with tc: tcols=st.number_input("Colonnes",1,100,10,key="ntc")
            with tr: trows=st.number_input("Lignes",1,1000,20,key="ntr")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="ctn",use_container_width=True):
                    if tn.strip(): _do_create_table(ds,user,cat_id,tn.strip(),int(tcols),int(trows)); st.session_state._show_new=None; st.rerun()
            with b:
                if st.button("Annuler",key="xtn",use_container_width=True): st.session_state._show_new=None; st.rerun()
    elif show=="map":
        with st.container(border=True):
            st.markdown("**🧠 Nouvelle map**")
            mn=st.text_input("Nom",key="nmn",placeholder="Nom...")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="cmn",use_container_width=True):
                    if mn.strip(): _do_create_map(ds,user,cat_id,mn.strip()); st.session_state._show_new=None; st.rerun()
            with b:
                if st.button("Annuler",key="xmn",use_container_width=True): st.session_state._show_new=None; st.rerun()

    st.markdown("---")
    kids=node.get("children",[])
    if kids:
        st.markdown('<span style="font-size:.72rem;color:#6c63ff;text-transform:uppercase;letter-spacing:.1em;">📁 Sous-dossiers</span>',unsafe_allow_html=True)
        cols=st.columns(min(len(kids),4))
        for i,ch in enumerate(kids):
            with cols[i%4]:
                with st.container(border=True):
                    st.markdown(f"**📁 {ch['name']}**")
                    st.markdown(f'<span style="font-size:.7rem;color:#8888aa;">{len(ch.get("items",[]))} fich.·{len(ch.get("children",[]))} ss.</span>',unsafe_allow_html=True)
                    if st.button("Ouvrir",key=f"okid_{ch['id']}",use_container_width=True):
                        set_cat(ch["id"]); st.rerun()
        st.markdown("<br>",unsafe_allow_html=True)

    items=node.get("items",[])
    if items:
        st.markdown('<span style="font-size:.72rem;color:#6c63ff;text-transform:uppercase;letter-spacing:.1em;">📄 Fichiers</span>',unsafe_allow_html=True)
        cols=st.columns(min(len(items),4))
        for i,it in enumerate(items):
            with cols[i%4]:
                icon="📊" if it["type"]=="table" else "🧠"
                with st.container(border=True):
                    st.markdown(f'**{icon} {it["name"]}**')
                    st.markdown(f'<span style="font-size:.67rem;color:#6c63ff;">{"TABLE" if it["type"]=="table" else "MAP"}</span>',unsafe_allow_html=True)
                    a,b=st.columns(2)
                    with a:
                        if st.button("Ouvrir",key=f"oit_{it['id']}",use_container_width=True):
                            open_item(it); st.rerun()
                    with b:
                        if st.button("🗑️",key=f"dit_{it['id']}",use_container_width=True):
                            ds.remove_item(user,cat_id,it["id"])
                            (ds.del_table if it["type"]=="table" else ds.del_map)(it["file_id"])
                            st.rerun()

    if not kids and not items:
        st.markdown('<p style="color:#55556a;font-size:.85rem;margin-top:16px;">Dossier vide.</p>',unsafe_allow_html=True)


def _render_bc(ds,user,cat_id):
    path=ds.get_path(user,cat_id) if cat_id else []
    parts=["🏠 Accueil"]+path
    html='<div class="bc">'
    for i,p in enumerate(parts):
        cls="bci cur" if i==len(parts)-1 else "bci"
        html+=f'<span class="{cls}">{p}</span>'
        if i<len(parts)-1: html+='<span class="bcs">›</span>'
    html+="</div>"; st.markdown(html,unsafe_allow_html=True)
    if path:
        if st.button("🏠",key="bc_home",help="Accueil"): set_cat(None); st.rerun()

def _do_create_table(ds,user,cat_id,name,cols,rows):
    bc=[user]+ds.get_path(user,cat_id); fid=ds.gen_id("tbl")
    ds.create_table(fid,bc,rows,cols)
    ds.add_item(user,cat_id,{"id":fid,"name":name,"type":"table","file_id":fid})

def _do_create_map(ds,user,cat_id,name):
    bc=[user]+ds.get_path(user,cat_id); fid=ds.gen_id("map")
    ds.create_map(fid,bc)
    ds.add_item(user,cat_id,{"id":fid,"name":name,"type":"map","file_id":fid})


# ══════════════════════════════════════════════════════════════════════════════
#  §9 — VUE TABLEUR  (fix effacement, resize, autosave)
# ══════════════════════════════════════════════════════════════════════════════
#
#  CORRECTIF EFFACEMENT :
#  Le bug venait du fait que `data_editor` retourne les données AVANT que
#  st.session_state.table_df soit mis à jour, causant une boucle.
#  Solution : on compare avec un hash du DataFrame courant. Si le hash
#  ne change pas entre deux reruns, on n'écrase pas.
#  On ne sauvegarde dans R2 que si autosave=True OU bouton manuel.

def render_table():
    ds=get_ds()
    item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]

    # Chargement initial (une seule fois par ouverture)
    if st.session_state.table_df is None:
        df=ds.load_table(fid)
        if df is None or df.empty: st.error("Table introuvable."); go_back(); st.rerun(); return
        st.session_state.table_df=df
        st.session_state.table_hash=_df_hash(df)
        st.session_state.undo_stack=[]; st.session_state.redo_stack=[]

    df: pd.DataFrame = st.session_state.table_df
    vis=[c for c in df.columns if c!="_location_"]

    # ── En-tête ───────────────────────────────────────────────────────────────
    cb,ct,_=st.columns([.13,.6,.27])
    with cb:
        if st.button("← Retour",key="tb_back",use_container_width=True):
            ds.save_table(fid,st.session_state.table_df); go_back(); st.rerun()
    with ct:
        st.markdown(f'<div class="pt">📊 {item["name"]}</div>',unsafe_allow_html=True)

    # ── Barre d'outils ────────────────────────────────────────────────────────
    can_undo=bool(st.session_state.undo_stack); can_redo=bool(st.session_state.redo_stack)
    cu,cr,cs,cas,_=st.columns([.1,.1,.1,.3,.4])
    with cu:
        if st.button("↩",key="tu",disabled=not can_undo,
                     help=f"Annuler ({len(st.session_state.undo_stack)})",use_container_width=True):
            _tundo(ds,fid); st.rerun()
    with cr:
        if st.button("↪",key="tr",disabled=not can_redo,
                     help=f"Rétablir ({len(st.session_state.redo_stack)})",use_container_width=True):
            _tredo(ds,fid); st.rerun()
    with cs:
        if st.button("💾",key="ts",help="Sauvegarder maintenant",use_container_width=True):
            ds.save_table(fid,st.session_state.table_df); st.toast("Sauvegardé ✓",icon="✅")
    with cas:
        autosave=st.toggle("Sauvegarde auto",value=st.session_state.tbl_autosave,key="tgl_as_tbl")
        st.session_state.tbl_autosave=autosave

    # ── Redimensionnement ─────────────────────────────────────────────────────
    with st.expander("⚙️ Redimensionner la table"):
        cr2,cc2,cb2=st.columns([.35,.35,.3])
        with cr2: new_rows=st.number_input("Lignes",1,2000,len(df),key="rsz_rows")
        with cc2: new_cols=st.number_input("Colonnes",1,200,len(vis),key="rsz_cols")
        with cb2:
            st.markdown("<br>",unsafe_allow_html=True)
            if st.button("Appliquer",key="rsz_apply",use_container_width=True):
                _tpush_undo()
                new_df=ds.resize_table(fid,st.session_state.table_df,int(new_rows),int(new_cols))
                st.session_state.table_df=new_df
                st.session_state.table_hash=_df_hash(new_df)
                st.session_state.redo_stack=[]
                if st.session_state.tbl_autosave: ds.save_table(fid,new_df)
                st.rerun()

    st.markdown("---")
    st.markdown('<span style="font-size:.7rem;color:#55556a;">💡 Colonne <code>_location_</code> masquée (chemin R2).</span>',unsafe_allow_html=True)

    # ── Éditeur ───────────────────────────────────────────────────────────────
    visible_df=df[vis].copy()

    # Clé stable = ne change pas entre reruns sauf si on ouvre une autre table
    editor_key=f"de_{fid}"

    edited=st.data_editor(
        visible_df,
        use_container_width=True,
        num_rows="fixed",        # ← FIXED : on gère les lignes via resize
        key=editor_key,
        column_config={c: st.column_config.TextColumn(c,width="medium") for c in vis},
        hide_index=False,
    )

    # CORRECTIF EFFACEMENT : comparaison par hash, pas equals()
    edited_hash=_df_hash(edited)
    if edited_hash != st.session_state.table_hash:
        _tpush_undo()
        loc=st.session_state.table_df["_location_"].copy()
        new_df=edited.copy()
        new_df.insert(0,"_location_",loc.values[:len(edited)])
        st.session_state.table_df=new_df
        st.session_state.table_hash=edited_hash
        st.session_state.redo_stack=[]
        if st.session_state.tbl_autosave:
            ds.save_table(fid,new_df)

    nr,nc=visible_df.shape
    st.markdown(
        f'<div style="font-size:.7rem;color:#55556a;margin-top:5px;">'
        f'📐 {nr} lignes × {nc} colonnes &nbsp;|&nbsp; ☁️ tables/{fid}.parquet'
        f'{"&nbsp;|&nbsp;🔄 Autosave ON" if st.session_state.tbl_autosave else "&nbsp;|&nbsp;⏸ Autosave OFF"}'
        f'</div>', unsafe_allow_html=True)


def _df_hash(df: pd.DataFrame) -> str:
    """Hash rapide d'un DataFrame pour détecter les changements sans equals()."""
    try: return hashlib.md5(pd.util.hash_pandas_object(df,index=True).values.tobytes()).hexdigest()
    except: return str(df.shape)

def _tpush_undo():
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    if len(st.session_state.undo_stack)>50: st.session_state.undo_stack.pop(0)

def _tundo(ds,fid):
    if not st.session_state.undo_stack: return
    st.session_state.redo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df=st.session_state.undo_stack.pop()
    st.session_state.table_hash=_df_hash(st.session_state.table_df)
    ds.save_table(fid,st.session_state.table_df)

def _tredo(ds,fid):
    if not st.session_state.redo_stack: return
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df=st.session_state.redo_stack.pop()
    st.session_state.table_hash=_df_hash(st.session_state.table_df)
    ds.save_table(fid,st.session_state.table_df)


# ══════════════════════════════════════════════════════════════════════════════
#  §10 — VUE MAP BRAINSTORMING
#  Nouveautés : undo/redo JS, sélection auto en mode création,
#  colonne "writable", zone d'écriture adaptée, min-size contrôlée,
#  frappe clavier directe sur objet sélectionné, autosave toggle.
# ══════════════════════════════════════════════════════════════════════════════

def _build_canvas_html(objs_json: str, fid: str) -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
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
#cw{{position:relative;overflow:hidden;width:100%;height:calc(100vh - 52px);
     background:#0f0f13;background-image:radial-gradient(circle,#1e1e28 1px,transparent 1px);background-size:28px 28px;}}
#cv{{display:block;}}
#te{{position:absolute;display:none;border:2px solid #6c63ff;background:rgba(22,22,32,.97);
     color:#e8e8f0;font-family:'JetBrains Mono',monospace;font-size:13px;resize:none;
     outline:none;padding:7px;border-radius:4px;overflow:hidden;text-align:center;line-height:1.4;}}
#ah{{position:absolute;top:7px;right:12px;font-size:.7rem;color:#8b85ff;
     background:rgba(108,99,255,.15);padding:3px 9px;border-radius:4px;display:none;pointer-events:none;}}
</style></head><body>
<div id="tb">
  <button class="tb on" id="bs" onclick="setTool('select')">&#128432; Sélection</button>
  <button class="tb" id="br" onclick="setTool('rect')">&#9645; Rectangle</button>
  <button class="tb" id="ba" onclick="setTool('arrow')">&#8594; Flèche</button>
  <div class="sep"></div>
  <button class="tb" onclick="zoom(.15)">&#128269;+</button>
  <div id="zd">100%</div>
  <button class="tb" onclick="zoom(-.15)">&#128269;-</button>
  <button class="tb" onclick="resetView()">&#8855;</button>
  <div class="sep"></div>
  <button class="tb" onclick="mapUndo()">&#8617; Annuler</button>
  <button class="tb" onclick="mapRedo()">&#8618; Rétablir</button>
  <div class="sep"></div>
  <button class="tb" onclick="toggleWritable()" id="btn_wr">&#9998; Éditable: OUI</button>
  <div class="sep"></div>
  <button class="tb" onclick="delSel()">&#128465; Suppr.</button>
  <div class="sep"></div>
  <button class="tb" onclick="exportJSON()" style="border-color:rgba(108,99,255,.5);color:#8b85ff;">&#128190; Export JSON</button>
  <span id="hint">Alt+drag: panoramique</span>
  <span id="st"></span>
</div>
<div id="cw">
  <canvas id="cv"></canvas>
  <textarea id="te" maxlength="2000"></textarea>
  <div id="ah">Source &#8594; cliquer la cible</div>
</div>
<script>
const cv=document.getElementById('cv'),ctx=cv.getContext('2d'),
      cw=document.getElementById('cw'),te=document.getElementById('te'),
      ah=document.getElementById('ah'),stEl=document.getElementById('st'),
      hintEl=document.getElementById('hint'),btnWr=document.getElementById('btn_wr');

// ── State ─────────────────────────────────────────────────────────────────
let objs={objs_json};
let tool='select',sc=1,ox=50,oy=50;
let selId=null,drag=false,dsx=0,dsy=0,dox=0,doy=0;
let draw=false,ds={{x:0,y:0}},dc={{x:0,y:0}};
let rsz=false,rh=null,rs=null,pan=false,px=0,py=0,pox=0,poy=0;
let eid=null,asrc=null,idc=1;
// Undo/redo maps
let undoStack=[],redoStack=[];
objs.forEach(o=>{{const n=parseInt(String(o.id).replace(/[^0-9]/g,''))||0;if(n>=idc)idc=n+1;}});

// ── Canvas resize ─────────────────────────────────────────────────────────
function resizeCv(){{cv.width=cw.clientWidth;cv.height=cw.clientHeight;render();}}
window.addEventListener('resize',resizeCv);resizeCv();

// ── Coords ────────────────────────────────────────────────────────────────
function tw(cx,cy){{return{{x:(cx-ox)/sc,y:(cy-oy)/sc}};}}
function gp(e){{const r=cv.getBoundingClientRect();return{{x:e.clientX-r.left,y:e.clientY-r.top}};}}

// ── Hit testing ───────────────────────────────────────────────────────────
function hRect(o,wx,wy){{return o.type==='rectangle'&&wx>=o.x&&wx<=o.x+o.w&&wy>=o.y&&wy<=o.y+o.h;}}
function hTest(wx,wy){{for(let i=objs.length-1;i>=0;i--){{if(hRect(objs[i],wx,wy))return objs[i];}}return null;}}
function gHandles(o){{
  if(o.type!=='rectangle')return[];
  return[{{id:'se',x:o.x+o.w,y:o.y+o.h}},{{id:'e',x:o.x+o.w,y:o.y+o.h/2}},
         {{id:'s',x:o.x+o.w/2,y:o.y+o.h}},{{id:'n',x:o.x+o.w/2,y:o.y}},
         {{id:'nw',x:o.x,y:o.y}},{{id:'w',x:o.x,y:o.y+o.h/2}}];
}}
function hHandle(o,wx,wy){{const t=7/sc;for(const h of gHandles(o)){{if(Math.abs(wx-h.x)<t&&Math.abs(wy-h.y)<t)return h.id;}}return null;}}

// ── Min size based on text content ────────────────────────────────────────
function minSizeForText(o){{
  if(o.type!=='rectangle'||!o.label)return{{w:60,h:36}};
  const fs=14; ctx.font=fs+'px JetBrains Mono,monospace';
  const words=o.label.split(' ');let maxWord=0;
  words.forEach(w=>{{const m=ctx.measureText(w).width;if(m>maxWord)maxWord=m;}});
  const minW=Math.max(60,maxWord+28);
  const linesEst=Math.ceil(o.label.length/Math.max(1,Math.floor(minW/8)));
  const minH=Math.max(36,linesEst*fs*1.5+20);
  return{{w:minW,h:minH}};
}}

// ── Undo/Redo ─────────────────────────────────────────────────────────────
function pushUndo(){{
  undoStack.push(JSON.stringify(objs));
  if(undoStack.length>40)undoStack.shift();
  redoStack=[];
}}
function mapUndo(){{
  if(!undoStack.length)return;
  redoStack.push(JSON.stringify(objs));
  objs=JSON.parse(undoStack.pop());
  selId=null;render();
}}
function mapRedo(){{
  if(!redoStack.length)return;
  undoStack.push(JSON.stringify(objs));
  objs=JSON.parse(redoStack.pop());
  selId=null;render();
}}

// ── Writable toggle ───────────────────────────────────────────────────────
function toggleWritable(){{
  const o=objs.find(x=>x.id===selId);
  if(!o||o.type!=='rectangle')return;
  o.writable=(o.writable!==false)?false:true;
  btnWr.textContent='✎ Éditable: '+(o.writable!==false?'OUI':'NON');
  render();
}}
function updateWritableBtn(){{
  const o=objs.find(x=>x.id===selId);
  if(!o||o.type!=='rectangle'){{btnWr.textContent='✎ Éditable: —';return;}}
  btnWr.textContent='✎ Éditable: '+(o.writable!==false?'OUI':'NON');
}}

// ── Render ────────────────────────────────────────────────────────────────
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
  ctx.strokeStyle=sel?'#6c63ff':(o.writable===false?'#4a4a5a':'#3a3a5a');
  ctx.lineWidth=(sel?2:1)/sc;
  if(o.writable===false)ctx.setLineDash([4/sc,2/sc]);
  ctx.beginPath();rr(ctx,o.x,o.y,o.w,o.h,8/sc);ctx.fill();ctx.stroke();
  ctx.setLineDash([]);
  ctx.restore();
  // Badge non-editable
  if(o.writable===false){{
    ctx.save();ctx.fillStyle='rgba(85,85,106,.7)';ctx.font=`${{9/sc}}px JetBrains Mono,monospace`;
    ctx.textAlign='right';ctx.textBaseline='top';
    ctx.fillText('🔒',o.x+o.w-4/sc,o.y+4/sc);ctx.restore();
  }}
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

// ── Text editor ───────────────────────────────────────────────────────────
function openEditor(o){{
  if(o.writable===false)return;
  eid=o.id;
  const cx=o.x*sc+ox,cy=o.y*sc+oy;
  te.style.cssText=`display:block;position:absolute;left:${{cx}}px;top:${{cy}}px;width:${{o.w*sc}}px;height:${{o.h*sc}}px;font-size:${{Math.max(10,Math.min(14,o.w/14))}}px;`;
  te.value=o.label||'';te.focus();
  // Move cursor to end
  const len=te.value.length;te.setSelectionRange(len,len);
  render();
}}
function closeEditor(){{
  if(eid!==null){{const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;eid=null;te.style.display='none';render();}}
}}
te.addEventListener('input',()=>{{const o=objs.find(x=>x.id===eid);if(o){{o.label=te.value;render();}}}});
te.addEventListener('blur',closeEditor);
te.addEventListener('keydown',e=>{{if(e.key==='Escape')closeEditor();e.stopPropagation();}});

// ── Tool switching ────────────────────────────────────────────────────────
function setTool(t){{
  tool=t;asrc=null;ah.style.display='none';
  ['bs','br','ba'].forEach(id=>document.getElementById(id).classList.remove('on'));
  const btn=document.getElementById('b'+t[0]);if(btn)btn.classList.add('on');
  closeEditor();
  const cursors={{select:'default',rect:'crosshair',arrow:'crosshair'}};
  cv.style.cursor=cursors[t]||'default';
  const hints={{select:'Clic: sél | Dbl-clic ou frappe: éditer | Drag: déplacer',
                rect:'Glisser pour créer | Clic sur existant: sélectionner',
                arrow:'Cliquer source puis cible | Clic sur existant: sélectionner'}};
  hintEl.textContent=hints[t]||'';
}}

// ── Mouse ─────────────────────────────────────────────────────────────────
cv.addEventListener('mousedown',e=>{{
  e.preventDefault();const cp=gp(e),wp=tw(cp.x,cp.y);
  if(e.button===1||(e.button===0&&e.altKey)){{pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';return;}}
  if(e.button!==0)return;closeEditor();
  if(tool==='select'){{
    const sel=selId?objs.find(o=>o.id===selId):null;
    if(sel&&sel.type==='rectangle'){{
      const h=hHandle(sel,wp.x,wp.y);
      if(h){{pushUndo();rsz=true;rh=h;dsx=wp.x;dsy=wp.y;rs={{x:sel.x,y:sel.y,w:sel.w,h:sel.h}};return;}}
    }}
    const hit=hTest(wp.x,wp.y);
    if(hit){{
      selId=hit.id;updateWritableBtn();
      if(hit.type==='rectangle'){{pushUndo();drag=true;dsx=wp.x;dsy=wp.y;dox=hit.x;doy=hit.y;}}
    }}else{{selId=null;updateWritableBtn();pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';}}
    render();
  }} else if(tool==='rect'){{
    // Clic sur objet existant → sélectionner
    const hit=hTest(wp.x,wp.y);
    if(hit){{selId=hit.id;updateWritableBtn();setTool('select');render();return;}}
    draw=true;ds={{x:wp.x,y:wp.y}};dc={{x:wp.x,y:wp.y}};
  }} else if(tool==='arrow'){{
    // Clic sur objet existant toujours sélectionnable
    const hit=hTest(wp.x,wp.y);
    if(!hit){{asrc=null;ah.style.display='none';return;}}
    if(!asrc){{asrc=hit.id;selId=hit.id;updateWritableBtn();ah.style.display='block';render();}}
    else if(asrc!==hit.id){{
      pushUndo();
      const src=objs.find(o=>o.id===asrc),dst=hit;
      objs.push({{id:'a'+(idc++),type:'arrow',x1:src.x+src.w/2,y1:src.y+src.h/2,
                  x2:dst.x+dst.w/2,y2:dst.y+dst.h/2,label:'',srcId:asrc,dstId:hit.id,writable:true}});
      asrc=null;ah.style.display='none';render();
    }}
  }}
}});

cv.addEventListener('mousemove',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  if(pan){{ox=pox+(cp.x-px);oy=poy+(cp.y-py);render();return;}}
  if(drag&&selId){{const o=objs.find(x=>x.id===selId);if(o&&o.type==='rectangle'){{o.x=dox+(wp.x-dsx);o.y=doy+(wp.y-dsy);syncArrows(o);render();}}}}
  if(rsz&&selId){{
    const o=objs.find(x=>x.id===selId);
    if(o){{
      const ms=minSizeForText(o);
      const dx=wp.x-dsx,dy=wp.y-dsy;
      if(rh.includes('e'))o.w=Math.max(ms.w,rs.w+dx);
      if(rh.includes('s'))o.h=Math.max(ms.h,rs.h+dy);
      if(rh.includes('w')){{const nw=Math.max(ms.w,rs.w-dx);o.x=rs.x+(rs.w-nw);o.w=nw;}}
      if(rh.includes('n')){{const nh=Math.max(ms.h,rs.h-dy);o.y=rs.y+(rs.h-nh);o.h=nh;}}
      syncArrows(o);if(eid===o.id)openEditor(o);render();
    }}
  }}
  if(draw){{dc={{x:wp.x,y:wp.y}};render();}}
}});

cv.addEventListener('mouseup',e=>{{
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};
  cv.style.cursor=curs[tool]||'default';
  if(pan){{pan=false;return;}}
  if(draw){{
    draw=false;const w=dc.x-ds.x,h=dc.y-ds.y;
    if(Math.abs(w)>20&&Math.abs(h)>16){{
      pushUndo();
      const o={{id:'r'+(idc++),type:'rectangle',x:w>0?ds.x:ds.x+w,y:h>0?ds.y:ds.y+h,
               w:Math.abs(w),h:Math.abs(h),label:'',fill:'#1c1c36',writable:true}};
      objs.push(o);selId=o.id;updateWritableBtn();
    }}render();
  }}
  if(drag||rsz){{drag=false;rsz=false;}}
}});

cv.addEventListener('dblclick',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  const hit=hTest(wp.x,wp.y);
  if(hit&&hit.type==='rectangle'){{selId=hit.id;updateWritableBtn();openEditor(hit);}}
}});

// Frappe clavier directe sur l'objet sélectionné
document.addEventListener('keydown',e=>{{
  if(e.target!==document.body)return;
  if(e.key==='Delete'||e.key==='Backspace'){{delSel();return;}}
  if(e.key==='Escape'){{selId=null;render();return;}}
  // Si un rectangle est sélectionné et éditable → ouvrir l'éditeur et ajouter le caractère
  if(selId&&e.key.length===1&&!e.ctrlKey&&!e.metaKey){{
    const o=objs.find(x=>x.id===selId);
    if(o&&o.type==='rectangle'&&o.writable!==false){{
      openEditor(o);
      // Ajouter le caractère tapé à la suite
      te.value=(o.label||'')+e.key;
      o.label=te.value;
      const len=te.value.length;te.setSelectionRange(len,len);
      render();
    }}
  }}
  // Ctrl+Z / Ctrl+Y
  if(e.key==='z'&&e.ctrlKey){{mapUndo();}}
  if((e.key==='y'&&e.ctrlKey)||(e.key==='Z'&&e.ctrlKey&&e.shiftKey)){{mapRedo();}}
}});

cv.addEventListener('wheel',e=>{{
  e.preventDefault();const cp=gp(e),d=e.deltaY<0?.12:-.12;
  const ns=Math.max(.08,Math.min(6,sc+d));
  ox=cp.x-(cp.x-ox)*(ns/sc);oy=cp.y-(cp.y-oy)*(ns/sc);sc=ns;render();
}},{{passive:false}});
function zoom(d){{const cx=cv.width/2,cy=cv.height/2;const ns=Math.max(.08,Math.min(6,sc+d));ox=cx-(cx-ox)*(ns/sc);oy=cy-(cy-oy)*(ns/sc);sc=ns;render();}}
function resetView(){{sc=1;ox=50;oy=50;render();}}

function delSel(){{
  if(!selId)return;pushUndo();
  objs=objs.filter(o=>o.id!==selId&&o.srcId!==selId&&o.dstId!==selId);
  selId=null;updateWritableBtn();render();
}}

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
    return{{id:o.id,type:o.type,label:o.label||'',coords:JSON.stringify(c),writable:o.writable!==false}};
  }});
  const j=JSON.stringify(data);
  if(navigator.clipboard)navigator.clipboard.writeText(j).then(()=>{{stEl.textContent='Copié!';setTimeout(()=>stEl.textContent='',3000);}});
  stEl.textContent='JSON copié — coller dans le champ ci-dessous';setTimeout(()=>stEl.textContent='',4000);
}}
render();
</script></body></html>"""


def render_map():
    ds=get_ds()
    item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]
    df=ds.load_map(fid)
    if df is None or df.empty: st.error("Map introuvable."); go_back(); st.rerun(); return

    # Construire liste objets JS
    objs=[]
    for _,row in df.iterrows():
        if str(row.get("type",""))=="meta": continue
        coords={}
        try: coords=json.loads(str(row.get("coords","{}")))
        except: pass
        o={"id":str(row["object_id"]),"type":str(row["type"]),
           "label":str(row.get("label","")),"writable":str(row.get("writable","true")).lower()!="false"}
        if row["type"]=="rectangle":
            o.update({"x":float(coords.get("x",100)),"y":float(coords.get("y",100)),
                      "w":float(coords.get("w",180)),"h":float(coords.get("h",100)),"fill":"#1c1c36"})
        elif row["type"]=="arrow":
            o.update({"x1":float(coords.get("x1",0)),"y1":float(coords.get("y1",0)),
                      "x2":float(coords.get("x2",100)),"y2":float(coords.get("y2",100)),
                      "srcId":coords.get("srcId"),"dstId":coords.get("dstId")})
        objs.append(o)
    objs_json=json.dumps(objs,ensure_ascii=False)

    # En-tête
    cb,ct=st.columns([.13,.87])
    with cb:
        if st.button("← Retour",key="map_back",use_container_width=True):
            go_back(); st.rerun()
    with ct:
        st.markdown(f'<div class="pt">🧠 {item["name"]}</div>',unsafe_allow_html=True)

    # Autosave toggle
    c1,c2=st.columns([.3,.7])
    with c1:
        autosave=st.toggle("Sauvegarde auto map",value=st.session_state.map_autosave,key="tgl_as_map")
        st.session_state.map_autosave=autosave

    # Panneau import/export
    with st.expander("💾 Sauvegarder la map (coller le JSON exporté)"):
        st.markdown('<span style="font-size:.77rem;color:#8888aa;">1. Clic <b>Export JSON</b> dans le canvas<br>2. Coller ici → Sauvegarder</span>',unsafe_allow_html=True)
        raw=st.text_area("JSON",key=f"mj_{fid}",height=68,
                          placeholder='[{"id":"r1","type":"rectangle",...}]',
                          label_visibility="collapsed")
        if st.button("💾 Sauvegarder dans R2",key=f"smr_{fid}",use_container_width=True):
            if raw and raw.strip():
                try:
                    _map_save_objects(ds,fid,df,json.loads(raw.strip()))
                    st.success("Map sauvegardée !"); st.rerun()
                except Exception as ex: st.error(f"JSON invalide : {ex}")

    components.html(_build_canvas_html(objs_json,fid),height=640,scrolling=False)
    st.markdown('<div style="font-size:.7rem;color:#55556a;margin-top:5px;">💡 <b>Alt+Drag</b>: pan &nbsp;|&nbsp; <b>Molette</b>: zoom &nbsp;|&nbsp; <b>Dbl-clic</b>: éditer &nbsp;|&nbsp; <b>Frappe</b> sur objet sélectionné: écrire &nbsp;|&nbsp; <b>Ctrl+Z/Y</b>: undo/redo</div>',unsafe_allow_html=True)


def _map_save_objects(ds,fid,orig_df,objects):
    loc=""
    meta=orig_df[orig_df["type"]=="meta"]
    if not meta.empty: loc=str(meta.iloc[0].get("_location_",""))
    rows=[{"_location_":loc,"object_id":"_meta_","type":"meta","label":"map","coords":"{}","writable":"false"}]
    for o in objects:
        t=o.get("type",""); coords={}
        if t=="rectangle": coords={k:o.get(k,0) for k in("x","y","w","h")}
        elif t=="arrow": coords={k:o.get(k) for k in("x1","y1","x2","y2","srcId","dstId")}
        rows.append({"_location_":"","object_id":str(o.get("id","")),"type":t,
                     "label":str(o.get("label","")),"coords":json.dumps(coords),
                     "writable":str(o.get("writable",True)).lower()})
    ds.save_map(fid,pd.DataFrame(rows))


# ══════════════════════════════════════════════════════════════════════════════
#  §11 — POINT D'ENTRÉE
# ══════════════════════════════════════════════════════════════════════════════

def main():
    st.set_page_config(page_title="WorkSpace Manager",layout="wide",initial_sidebar_state="expanded")
    st.markdown(f"<style>{CSS}</style>",unsafe_allow_html=True)
    init_state()
    with st.sidebar: render_sidebar()
    user=st.session_state.get("current_user")
    view=st.session_state.get("view","folder")
    if user is None: render_all_sessions()
    elif view=="table": render_table()
    elif view=="map": render_map()
    else: render_folder()

if __name__=="__main__":
    main()
