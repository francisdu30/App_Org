# =============================================================================
#  WorkSpace Manager — Streamlit Community Cloud  v7  (Matcha Edition)
#  Stockage : Cloudflare R2 (boto3)
# =============================================================================
#  secrets.toml : R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET
# =============================================================================
from __future__ import annotations
import io, json, uuid
from datetime import date as dt_date
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# ══════════════════════════════════════════════════════════════════════════════
#  §1 — R2
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
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return pd.DataFrame(columns=cols)
        raise
    except Exception:
        return pd.DataFrame(columns=cols)

def r2_save(df: pd.DataFrame, key: str):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    get_r2().put_object(Bucket=_bkt(), Key=key, Body=buf.getvalue())

def r2_del(key: str):
    try: get_r2().delete_object(Bucket=_bkt(), Key=key)
    except: pass

def r2_list(prefix: str) -> list[str]:
    try:
        pag = get_r2().get_paginator("list_objects_v2")
        return [o["Key"] for p in pag.paginate(Bucket=_bkt(), Prefix=prefix)
                for o in p.get("Contents", [])]
    except: return []

def r2_copy(src: str, dst: str):
    try: get_r2().copy_object(Bucket=_bkt(),
                              CopySource={"Bucket": _bkt(), "Key": src}, Key=dst)
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  §2 — SESSIONS CACHE
# ══════════════════════════════════════════════════════════════════════════════
SKEY = "sessions.parquet"
SCOLS = ["key", "value"]

def _sget() -> pd.DataFrame:
    if "_sc" not in st.session_state:
        st.session_state._sc = r2_load(SKEY, SCOLS)
    return st.session_state._sc

def _ssave(df: pd.DataFrame):
    r2_save(df, SKEY)
    st.session_state._sc = df

def _srow(df, k):
    r = df[df["key"] == k]
    return r.iloc[0]["value"] if not r.empty else None

def _sup(df, k, v):
    if k in df["key"].values:
        df = df.copy(); df.loc[df["key"] == k, "value"] = v
    else:
        df = pd.concat([df, pd.DataFrame([{"key": k, "value": v}])], ignore_index=True)
    return df

def _sdel(df, k):
    return df[df["key"] != k].reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════════════════
#  §3 — DATASTORE
# ══════════════════════════════════════════════════════════════════════════════
MAX_VERSIONS = 30

class DataStore:
    def __init__(self):
        try:
            df = _sget()
            if df.empty or "_users_meta_" not in df["key"].values:
                self._boot()
        except: self._boot()

    def _boot(self):
        try: _ssave(pd.DataFrame([{"key": "_users_meta_", "value": "{}"}]))
        except: pass

    # Users
    def get_users(self) -> list[str]:
        try: return list(json.loads(_srow(_sget(), "_users_meta_") or "{}").keys())
        except: return []

    def add_user(self, name: str) -> bool:
        df = _sget()
        us = json.loads(_srow(df, "_users_meta_") or "{}")
        if name in us: return False
        us[name] = {"created_at": str(pd.Timestamp.now())}
        df = _sup(df, "_users_meta_", json.dumps(us))
        df = _sup(df, name, json.dumps([]))
        _ssave(df); return True

    def rm_user(self, name: str, to_backup: bool = True):
        if to_backup:
            tree = self.get_tree(name)
            all_items: list[dict] = []
            _tfl(tree, [], all_items, name)
            for cat in all_items:
                for it in cat["items"]: self._backup_item(name, it)
        df = _sget()
        us = json.loads(_srow(df, "_users_meta_") or "{}")
        us.pop(name, None)
        df = _sup(df, "_users_meta_", json.dumps(us))
        df = _sdel(df, name)
        _ssave(df)

    def _backup_item(self, user: str, item: dict):
        fid = item.get("file_id", ""); t = item.get("type", "table")
        src = f"{'tables' if t == 'table' else 'maps'}/{fid}.parquet"
        dst = f"backup/{user}/{t}/{fid}.parquet"
        r2_copy(src, dst)
        meta = self._get_backup_meta()
        meta.append({"user": user, "type": t, "fid": fid,
                     "name": item.get("name", "?"),
                     "deleted_at": str(pd.Timestamp.now())})
        self._save_backup_meta(meta)

    def _get_backup_meta(self) -> list:
        try:
            df = r2_load("backup/_meta.parquet",
                         ["user", "type", "fid", "name", "deleted_at"])
            return df.to_dict("records")
        except: return []

    def _save_backup_meta(self, meta: list):
        df = (pd.DataFrame(meta) if meta
              else pd.DataFrame(columns=["user", "type", "fid", "name", "deleted_at"]))
        r2_save(df, "backup/_meta.parquet")

    # Tree
    def get_tree(self, u: str) -> list:
        try: return json.loads(_srow(_sget(), u) or "[]")
        except: return []

    def _wtree(self, u: str, t: list):
        df = _sget(); df = _sup(df, u, json.dumps(t)); _ssave(df)

    def add_cat(self, u: str, name: str, pid: str | None = None) -> dict:
        t = self.get_tree(u)
        n = {"id": uuid.uuid4().hex[:8], "name": name,
             "children": [], "items": [], "collapsed": False}
        if pid is None: t.append(n)
        else: _ti(t, pid, n)
        self._wtree(u, t); return n

    def del_cat(self, u: str, cid: str):
        tree = self.get_tree(u)
        node = _tf(tree, cid)
        if node:
            all_items: list[dict] = []
            _tfl([node], [], all_items, u)
            for cat in all_items:
                for it in cat["items"]: self._backup_item(u, it)
        self._wtree(u, _tr(tree, cid))

    def ren_cat(self, u, cid, nm):
        t = self.get_tree(u); _trn(t, cid, nm); self._wtree(u, t)

    def tog_col(self, u, cid):
        t = self.get_tree(u); _ttog(t, cid); self._wtree(u, t)

    def set_all_collapsed(self, u: str, collapsed: bool):
        t = self.get_tree(u); _tset_col(t, collapsed); self._wtree(u, t)

    def mv_cat(self, u, nid, newp: str | None):
        t = self.get_tree(u); nd = _tf(t, nid)
        if not nd: return
        if newp and _tdesc(nd, newp): return
        t = _tr(t, nid)
        if newp is None: t.append(nd)
        else: _ti(t, newp, nd)
        self._wtree(u, t)

    def add_item(self, u, cid, item):
        t = self.get_tree(u); _tai(t, cid, item); self._wtree(u, t)

    def rm_item(self, u, cid, iid, to_backup: bool = True):
        t = self.get_tree(u)
        if to_backup:
            node = _tf(t, cid)
            if node:
                for it in node.get("items", []):
                    if it["id"] == iid: self._backup_item(u, it); break
        _tri(t, cid, iid); self._wtree(u, t)

    def get_path(self, u, cid) -> list[str]:
        t = self.get_tree(u); p: list[str] = []
        _tpn(t, cid, p); return p

    def get_flat(self, u) -> list[dict]:
        t = self.get_tree(u); r: list[dict] = []
        _tfl(t, [], r, u); return r

    def gen_id(self, pfx: str = "tbl") -> str:
        return f"{pfx}_{uuid.uuid4().hex[:16]}"

    # Versions
    def save_version(self, fid: str, src_key: str):
        today = str(dt_date.today())
        vkey = f"versions/{fid}/{today}.parquet"
        existing = r2_list(f"versions/{fid}/")
        if vkey in existing: return
        r2_copy(src_key, vkey)
        sorted_v = sorted(existing)
        while len(sorted_v) >= MAX_VERSIONS:
            r2_del(sorted_v.pop(0))

    def get_versions(self, fid: str) -> list[str]:
        keys = r2_list(f"versions/{fid}/")
        return sorted([k.split("/")[-1].replace(".parquet", "") for k in keys], reverse=True)

    def load_version(self, fid: str, date_str: str, cols: list[str]) -> pd.DataFrame:
        return r2_load(f"versions/{fid}/{date_str}.parquet", cols)

    # Tables
    def mk_table(self, fid, bc, rows=20, cols=10) -> pd.DataFrame:
        cns = [f"Col_{chr(65 + i % 26)}{'_' + str(i // 26) if i >= 26 else ''}"
               for i in range(cols)]
        df = pd.DataFrame({c: [""] * rows for c in cns})
        df.insert(0, "_location_", [" > ".join(bc)] + [""] * (rows - 1))
        r2_save(df, f"tables/{fid}.parquet")
        self.save_version(fid, f"tables/{fid}.parquet")
        return df

    def resize_table(self, fid, df, nr, nc) -> pd.DataFrame:
        vis = [c for c in df.columns if c != "_location_"]
        loc = df["_location_"].copy(); data = df[vis].copy()
        cur_c = len(vis)
        if nc > cur_c:
            for i in range(cur_c, nc):
                cn = f"Col_{chr(65 + i % 26)}{'_' + str(i // 26) if i >= 26 else ''}"
                data[cn] = ""
        elif nc < cur_c: data = data.iloc[:, :nc]
        cur_r = len(data)
        if nr > cur_r:
            ex = pd.DataFrame({c: [""] * (nr - cur_r) for c in data.columns})
            data = pd.concat([data, ex], ignore_index=True)
        elif nr < cur_r: data = data.iloc[:nr]
        loc = loc.reindex(range(nr)).fillna("")
        data.insert(0, "_location_", loc.values); return data

    def ld_table(self, fid) -> pd.DataFrame | None:
        df = r2_load(f"tables/{fid}.parquet", [])
        return None if (df.empty and "_location_" not in df.columns) else df

    def sv_table(self, fid, df):
        r2_save(df, f"tables/{fid}.parquet")
        self.save_version(fid, f"tables/{fid}.parquet")

    def dl_table(self, fid): r2_del(f"tables/{fid}.parquet")

    # Maps
    def mk_map(self, fid, bc) -> pd.DataFrame:
        df = pd.DataFrame([{"_location_": " > ".join(bc), "object_id": "_meta_",
                            "type": "meta", "label": "map", "coords": "{}",
                            "writable": "false"}])
        r2_save(df, f"maps/{fid}.parquet")
        self.save_version(fid, f"maps/{fid}.parquet")
        return df

    def ld_map(self, fid) -> pd.DataFrame | None:
        cols = ["_location_", "object_id", "type", "label", "coords", "writable"]
        df = r2_load(f"maps/{fid}.parquet", cols)
        if "writable" not in df.columns: df["writable"] = "true"
        return None if (df.empty and "object_id" not in df.columns) else df

    def sv_map(self, fid, df):
        r2_save(df, f"maps/{fid}.parquet")
        self.save_version(fid, f"maps/{fid}.parquet")

    def dl_map(self, fid): r2_del(f"maps/{fid}.parquet")

    def get_backup_list(self) -> list[dict]:
        return self._get_backup_meta()

# Tree helpers
def _ti(ns, pid, nd):
    for n in ns:
        if n["id"] == pid: n.setdefault("children", []).append(nd); return True
        if _ti(n.get("children", []), pid, nd): return True
    return False

def _tr(ns, nid):
    r = []
    for n in ns:
        if n["id"] == nid: continue
        n["children"] = _tr(n.get("children", []), nid); r.append(n)
    return r

def _trn(ns, nid, nm):
    for n in ns:
        if n["id"] == nid: n["name"] = nm; return True
        if _trn(n.get("children", []), nid, nm): return True
    return False

def _ttog(ns, nid):
    for n in ns:
        if n["id"] == nid: n["collapsed"] = not n.get("collapsed", False); return True
        if _ttog(n.get("children", []), nid): return True
    return False

def _tset_col(ns, col):
    for n in ns: n["collapsed"] = col; _tset_col(n.get("children", []), col)

def _tf(ns, nid):
    for n in ns:
        if n["id"] == nid: return n
        f = _tf(n.get("children", []), nid)
        if f: return f
    return None

def _tdesc(nd, tid):
    for c in nd.get("children", []):
        if c["id"] == tid or _tdesc(c, tid): return True
    return False

def _tai(ns, cid, item):
    for n in ns:
        if n["id"] == cid: n.setdefault("items", []).append(item); return True
        if _tai(n.get("children", []), cid, item): return True
    return False

def _tri(ns, cid, iid):
    for n in ns:
        if n["id"] == cid:
            n["items"] = [i for i in n.get("items", []) if i["id"] != iid]; return True
        if _tri(n.get("children", []), cid, iid): return True
    return False

def _tpn(ns, tid, p):
    for n in ns:
        p.append(n["name"])
        if n["id"] == tid: return True
        if _tpn(n.get("children", []), tid, p): return True
        p.pop()
    return False

def _tfl(ns, path, res, u):
    for n in ns:
        cp = path + [n["name"]]
        res.append({"id": n["id"], "name": n["name"], "path": cp,
                    "items": n.get("items", []), "username": u})
        _tfl(n.get("children", []), cp, res, u)

# ══════════════════════════════════════════════════════════════════════════════
#  §4 — STATE
# ══════════════════════════════════════════════════════════════════════════════
def get_ds() -> DataStore:
    if "_ds" not in st.session_state:
        st.session_state._ds = DataStore()
    return st.session_state._ds

def init_state():
    defs = {
        "current_user": None, "current_cat_id": None,
        "current_item": None, "view": "folder",
        "table_df": None,          # source de vérité complète (avec _location_)
        "table_vis_df": None,      # df visible stable passé à data_editor
        "table_fid": None,         # fid de la table courante
        "_tbl_sig": b"",           # hash du dernier df vu
        "undo_stack": [], "redo_stack": [],
        "map_pending_save": None,
    }
    for k, v in defs.items():
        if k not in st.session_state: st.session_state[k] = v
    get_ds()

def set_user(u):
    st.session_state.update(current_user=u, current_cat_id=None,
                            current_item=None, view="folder")

def set_cat(c):
    st.session_state.update(current_cat_id=c, current_item=None, view="folder")

def open_item(it):
    st.session_state.update(current_item=it, view=it["type"],
                            undo_stack=[], redo_stack=[],
                            table_df=None, table_vis_df=None,
                            table_fid=None, _tbl_sig=b"")

def go_back():
    st.session_state.update(current_item=None, view="folder",
                            table_df=None, table_vis_df=None,
                            table_fid=None, _tbl_sig=b"")

# ══════════════════════════════════════════════════════════════════════════════
#  §5 — CSS
# ══════════════════════════════════════════════════════════════════════════════
CSS = """
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Fraunces:opsz,wght@9..144,600;9..144,800&display=swap');
:root{
  --bg:#f4f7f2; --bg2:#eef3ea; --card:#ffffff;
  --ac:#4a7c59; --acl:#5e9b6e; --acd:rgba(74,124,89,.13); --acc:#3a6246;
  --t1:#1e2e22; --t2:#4a6657; --t3:#7a9e88;
  --bd:#c8dbc0; --bda:rgba(74,124,89,.35);
  --shadow:0 2px 12px rgba(74,124,89,.10);
  --shadow2:0 4px 24px rgba(74,124,89,.15);
}
*{font-family:'DM Mono',monospace;}
.stApp{background:var(--bg)!important;color:var(--t1)!important;}
#MainMenu,footer,header{visibility:hidden;}.stDeployButton{display:none;}
section[data-testid="stSidebar"]{
  background:var(--bg2)!important;
  border-right:2px solid var(--bd)!important;
  box-shadow:var(--shadow);
}
/* Page title */
.pt{font-family:'Fraunces',serif;font-size:1.6rem;font-weight:800;
  color:var(--t1);margin-bottom:14px;letter-spacing:-.01em;}
/* Breadcrumb */
.bc{display:flex;align-items:center;gap:4px;font-size:.75rem;
  color:var(--t3);margin-bottom:12px;}
.bci{cursor:pointer;color:var(--t2);padding:2px 6px;border-radius:5px;transition:all .15s;}
.bci:hover{background:var(--acd);color:var(--ac);}
.bci.cur{color:var(--ac);cursor:default;font-weight:500;}
.bcs{color:var(--t3);}
/* Section headers */
.ush{font-family:'Fraunces',serif;font-size:.88rem;font-weight:700;
  color:var(--t3);text-transform:uppercase;letter-spacing:.12em;
  padding:10px 0 5px;border-bottom:1px solid var(--bd);margin:16px 0 10px;}
/* GLOBAL buttons */
.stButton>button{
  font-family:'DM Mono',monospace!important;
  background:var(--card)!important;color:var(--t2)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  transition:all .18s!important;font-size:.78rem!important;
  box-shadow:0 1px 4px rgba(74,124,89,.07)!important;
}
.stButton>button:hover{
  border-color:var(--bda)!important;color:var(--ac)!important;
  background:var(--acd)!important;box-shadow:var(--shadow)!important;
}
.stButton>button[kind="primary"]{
  background:var(--acd)!important;color:var(--acc)!important;
  border-color:var(--bda)!important;font-weight:500!important;
}
/* SIDEBAR buttons — flat, no border */
section[data-testid="stSidebar"] .stButton>button{
  background:transparent!important;border:none!important;
  box-shadow:none!important;border-radius:5px!important;
  text-align:left!important;padding:2px 6px!important;
  font-size:.79rem!important;color:var(--t2)!important;
  white-space:nowrap!important;overflow:hidden!important;text-overflow:ellipsis!important;
}
section[data-testid="stSidebar"] .stButton>button:hover{
  background:var(--acd)!important;color:var(--ac)!important;
  border:none!important;box-shadow:none!important;
}
section[data-testid="stSidebar"] .stButton>button[kind="primary"]{
  background:rgba(74,124,89,.18)!important;color:var(--acc)!important;
  border:none!important;box-shadow:none!important;font-weight:600!important;
}
/* Inputs */
.stTextInput>div>div>input,.stNumberInput>div>div>input{
  background:var(--card)!important;color:var(--t1)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  font-family:'DM Mono',monospace!important;font-size:.82rem!important;
}
.stTextInput>div>div>input:focus,.stNumberInput>div>div>input:focus{
  border-color:var(--ac)!important;box-shadow:0 0 0 3px var(--acd)!important;
}
.stSelectbox>div>div{
  background:var(--card)!important;color:var(--t1)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
}
.stTextArea>div>div>textarea{
  background:var(--card)!important;color:var(--t1)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  font-family:'DM Mono',monospace!important;font-size:.8rem!important;
}
.stMarkdown p{font-family:'DM Mono',monospace!important;
  font-size:.82rem!important;color:var(--t2)!important;}
div[data-testid="stHorizontalBlock"]{gap:5px!important;}
/* Sidebar label */
.sb-lbl{font-size:.62rem;color:#7a9e88;text-transform:uppercase;
  letter-spacing:.1em;padding:10px 4px 2px;display:block;}
/* Folder items */
.fc{background:var(--card);border:1.5px solid var(--bd);border-radius:10px;
  padding:14px 12px;transition:all .18s;box-shadow:var(--shadow);}
.fc:hover{border-color:var(--bda);box-shadow:var(--shadow2);transform:translateY(-2px);}
.fc-icon{font-size:1.8rem;margin-bottom:6px;}
.fc-name{font-size:.82rem;color:var(--t1);font-weight:500;word-break:break-word;}
.fc-meta{font-size:.63rem;color:var(--t3);margin-top:3px;}
.fc-badge{font-size:.58rem;padding:2px 6px;border-radius:4px;
  background:var(--acd);color:var(--ac);font-weight:500;display:inline-block;margin-top:4px;}
/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg2);}
::-webkit-scrollbar-thumb{background:var(--bd);border-radius:4px;}
"""

# ══════════════════════════════════════════════════════════════════════════════
#  §6 — SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
def render_sidebar():
    ds = get_ds()
    users = ds.get_users()
    cu = st.session_state.get("current_user")

    st.markdown(
        '<div style="font-family:Fraunces,serif;font-size:1.15rem;font-weight:800;'
        'color:#4a7c59;padding:14px 10px 8px;border-bottom:1px solid #c8dbc0;">'
        '🍵 WorkSpace</div>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🌐 Toutes les sessions", key="btn_all",
                 type="primary" if cu is None else "secondary",
                 use_container_width=True):
        set_user(None); st.rerun()

    for u in users:
        if st.button(f"👤 {u}", key=f"bu_{u}",
                     type="primary" if cu == u else "secondary",
                     use_container_width=True):
            set_user(u); st.rerun()

    st.markdown("---")
    with st.expander("➕ Nouvelle session"):
        nu = st.text_input("Nom", key="nu_i", label_visibility="collapsed",
                           placeholder="Nom d'utilisateur…")
        if st.button("Créer", key="btn_nu", use_container_width=True):
            if nu.strip():
                if ds.add_user(nu.strip()): set_user(nu.strip()); st.rerun()
                else: st.error("Nom déjà utilisé.")

    if cu:
        st.markdown('<span class="sb-lbl">📁 Arborescence</span>',
                    unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            if st.button("▶ Plier", key="col_all", use_container_width=True):
                ds.set_all_collapsed(cu, True); st.rerun()
        with c2:
            if st.button("▼ Déplier", key="exp_all", use_container_width=True):
                ds.set_all_collapsed(cu, False); st.rerun()

        tree = ds.get_tree(cu)
        _render_tree(ds, cu, tree, depth=0, parent_last=[])

        st.markdown("---")
        with st.expander("➕ Catégorie racine"):
            cn = st.text_input("Nom", key="new_cat", label_visibility="collapsed",
                               placeholder="Nom de catégorie…")
            if st.button("Créer", key="btn_nc", use_container_width=True):
                if cn.strip(): ds.add_cat(cu, cn.strip()); st.rerun()

        st.markdown("---")
        with st.expander("⚠️ Supprimer session"):
            st.warning(f"Supprimer « {cu} » ?")
            conf = st.text_input("", key="del_sess_conf",
                                 label_visibility="collapsed", placeholder="Taper DELETE…")
            if st.button("Supprimer", key="btn_du", type="primary",
                         use_container_width=True):
                if conf.strip() == "DELETE":
                    ds.rm_user(cu, to_backup=True); set_user(None); st.rerun()
                else: st.error("Taper exactement DELETE")


def _trunc(s: str, n: int = 16) -> str:
    return s[:n] + "…" if len(s) > n else s


def _render_tree(ds, user, nodes, depth, parent_last, max_depth=2):
    cc = st.session_state.get("current_cat_id")
    n = len(nodes)
    for idx, node in enumerate(nodes):
        nid = node["id"]; name = node["name"]
        kids = node.get("children", [])
        collapsed = node.get("collapsed", False)
        sel = cc == nid; is_last = (idx == n - 1)
        has_kids = bool(kids) and depth < max_depth
        # Icone et style selon profondeur
        if depth == 0:
            icon = "◆" if kids else "◇"
        elif depth == 1:
            icon = "▸" if kids else "·"
        else:
            icon = "·"
        # Préfixe branche unicode — affiché hors bouton
        if depth > 0:
            pre = ""
            for pl in parent_last:
                pre += "    " if pl else "│   "
            br = "└── " if is_last else "├── "
            branch_div = (
                '<div style="line-height:1.2;padding:0 0 0 4px;margin-bottom:-4px;'
                'font-family:DM Mono,monospace;font-size:.70rem;color:#c8dbc0;'
                'letter-spacing:0;white-space:pre;">' + pre + br + '</div>'
            )
            st.markdown(branch_div, unsafe_allow_html=True)
        label = f"{icon} {_trunc(name)}"
        if has_kids:
            c_tog, c_nom, c_pop = st.columns([0.10, 0.75, 0.15])
            with c_tog:
                lbl = "▶" if collapsed else "▼"
                if st.button(lbl, key=f"tgl_{nid}", use_container_width=True):
                    ds.tog_col(user, nid); st.rerun()
        else:
            c_nom, c_pop = st.columns([0.85, 0.15])
        with c_nom:
            if st.button(label, key=f"cat_{nid}",
                         type="primary" if sel else "secondary",
                         use_container_width=True):
                set_cat(nid); st.rerun()
        with c_pop:
            _cat_menu(ds, user, nid, name, cc)
        if has_kids and not collapsed:
            _render_tree(ds, user, kids, depth + 1, parent_last + [is_last])

def _cat_menu(ds, user, nid, name, current_cat):
    with st.popover("⋯"):
        sub = st.text_input("", key=f"sub_{nid}", placeholder="Sous-cat…",
                            label_visibility="collapsed")
        if st.button("➕ Ajouter sous-cat.", key=f"addsub_{nid}",
                     use_container_width=True):
            if sub.strip(): ds.add_cat(user, sub.strip(), pid=nid); st.rerun()

        nn = st.text_input("", key=f"ren_{nid}", value=name,
                           label_visibility="collapsed")
        if st.button("✏️ Renommer", key=f"doRen_{nid}", use_container_width=True):
            if nn.strip() and nn != name:
                ds.ren_cat(user, nid, nn.strip()); st.rerun()

        flat = ds.get_flat(user)
        opts = {c["name"]: c["id"] for c in flat if c["id"] != nid}
        opts["(Racine)"] = None
        tgt = st.selectbox("Déplacer vers", list(opts.keys()), key=f"mv_{nid}")
        if st.button("↗️ Déplacer", key=f"doMv_{nid}", use_container_width=True):
            ds.mv_cat(user, nid, opts[tgt]); st.rerun()

        st.divider()
        conf = st.text_input("", key=f"dc_{nid}", placeholder="DELETE",
                             label_visibility="collapsed")
        if st.button("🗑️ Supprimer", key=f"del_{nid}", type="primary",
                     use_container_width=True):
            if conf.strip() == "DELETE":
                ds.del_cat(user, nid)
                if current_cat == nid: set_cat(None)
                st.rerun()
            else: st.error("Taper DELETE")

# ══════════════════════════════════════════════════════════════════════════════
#  §7 — ALL SESSIONS
# ══════════════════════════════════════════════════════════════════════════════
def render_all_sessions():
    ds = get_ds(); users = ds.get_users()
    st.markdown('<div class="pt">🌐 Toutes les sessions</div>',
                unsafe_allow_html=True)
    if not users:
        st.info("Aucune session. Créez-en une depuis la barre latérale."); return
    for user in users:
        st.markdown(f'<div class="ush">👤 {user}</div>', unsafe_allow_html=True)
        tree = ds.get_tree(user)
        if not tree:
            st.markdown('<p style="color:var(--t3);font-size:.8rem;">Aucune catégorie</p>',
                        unsafe_allow_html=True)
        else: _all_cats(ds, user, tree, 0)
        if st.button(f"→ Ouvrir {user}", key=f"oas_{user}"):
            set_user(user); st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div class="pt" style="font-size:1.1rem;">🗄️ Corbeille</div>',
                unsafe_allow_html=True)
    _render_backup(ds)


def _all_cats(ds, user, nodes, depth):
    for n in nodes:
        ni = len(n.get("items", [])); nc = len(n.get("children", []))
        pre = "　" * depth; icon = "📁" if nc else "📂"
        ca, cb, cc = st.columns([.55, .25, .2])
        with ca:
            st.markdown(
                f'<span style="font-size:.82rem;color:var(--t2);">'
                f'{pre}{icon} <b>{n["name"]}</b></span>',
                unsafe_allow_html=True)
        with cb:
            if ni or nc:
                st.markdown(
                    f'<span style="font-size:.68rem;color:var(--ac);">'
                    f'{ni} fich.·{nc} ss</span>', unsafe_allow_html=True)
        with cc:
            if st.button("→", key=f"oac_{user}_{n['id']}"):
                set_user(user); set_cat(n["id"]); st.rerun()
        if depth == 0 and n.get("children"):
            _all_cats(ds, user, n["children"], 1)


def _render_backup(ds: DataStore):
    items = ds.get_backup_list()
    if not items: st.info("Aucun élément supprimé."); return
    for i, it in enumerate(items):
        c1, c2, c3, c4 = st.columns([.3, .2, .2, .3])
        with c1: st.markdown(f'**{it.get("name","?")}** ({it.get("type","?")})')
        with c2:
            st.markdown(f'<span style="font-size:.7rem;color:var(--t3);">'
                        f'{it.get("user","?")}</span>', unsafe_allow_html=True)
        with c3:
            st.markdown(
                f'<span style="font-size:.68rem;color:var(--t3);">'
                f'{str(it.get("deleted_at",""))[:10]}</span>',
                unsafe_allow_html=True)
        with c4:
            fid = it.get("fid", ""); tp = it.get("type", "table")
            vkey = f"backup/{it['user']}/{tp}/{fid}.parquet"
            if st.button(f"⬇ Restaurer", key=f"rst_{fid}_{i}",
                         use_container_width=True):
                dst = f"{'tables' if tp == 'table' else 'maps'}/{fid}.parquet"
                r2_copy(vkey, dst)
                st.success(f"Fichier {fid} restauré.")

# ══════════════════════════════════════════════════════════════════════════════
#  §8 — FOLDER VIEW
# ══════════════════════════════════════════════════════════════════════════════
def render_folder():
    ds = get_ds()
    user = st.session_state.current_user
    cat_id = st.session_state.get("current_cat_id")
    _render_bc(ds, user, cat_id)

    if cat_id is None:
        st.markdown('<div class="pt">🏠 Accueil</div>', unsafe_allow_html=True)
        st.info("Sélectionnez ou créez une catégorie dans la barre latérale.")
        return

    node = _tf(ds.get_tree(user), cat_id)
    if node is None:
        st.warning("Catégorie introuvable."); set_cat(None); st.rerun(); return

    st.markdown(f'<div class="pt">📁 {node["name"]}</div>', unsafe_allow_html=True)

    c1, c2, c3, _ = st.columns([1, 1, 1, 4])
    with c1:
        if st.button("➕ Dossier", use_container_width=True):
            st.session_state._sn = "folder"
    with c2:
        if st.button("📊 Table", use_container_width=True):
            st.session_state._sn = "table"
    with c3:
        if st.button("🧠 Map", use_container_width=True):
            st.session_state._sn = "map"

    sn = st.session_state.get("_sn")
    if sn == "folder":
        with st.container(border=True):
            st.markdown("**➕ Nouveau sous-dossier**")
            fn = st.text_input("Nom", key="nfn", placeholder="Nom…")
            a, b = st.columns(2)
            with a:
                if st.button("Créer", key="cfn", use_container_width=True):
                    if fn.strip():
                        ds.add_cat(user, fn.strip(), pid=cat_id)
                        st.session_state._sn = None; st.rerun()
            with b:
                if st.button("Annuler", key="xfn", use_container_width=True):
                    st.session_state._sn = None; st.rerun()
    elif sn == "table":
        with st.container(border=True):
            st.markdown("**📊 Nouvelle table**")
            tn = st.text_input("Nom", key="ntn", placeholder="Nom…")
            tc2, tr2 = st.columns(2)
            with tc2: tcols = st.number_input("Colonnes", 1, 200, 10, key="ntc")
            with tr2: trows = st.number_input("Lignes", 1, 2000, 20, key="ntr")
            a, b = st.columns(2)
            with a:
                if st.button("Créer", key="ctn", use_container_width=True):
                    if tn.strip():
                        _mk_table(ds, user, cat_id, tn.strip(),
                                  int(tcols), int(trows))
                        st.session_state._sn = None; st.rerun()
            with b:
                if st.button("Annuler", key="xtn", use_container_width=True):
                    st.session_state._sn = None; st.rerun()
    elif sn == "map":
        with st.container(border=True):
            st.markdown("**🧠 Nouvelle map**")
            mn = st.text_input("Nom", key="nmn", placeholder="Nom…")
            a, b = st.columns(2)
            with a:
                if st.button("Créer", key="cmn", use_container_width=True):
                    if mn.strip():
                        _mk_map(ds, user, cat_id, mn.strip())
                        st.session_state._sn = None; st.rerun()
            with b:
                if st.button("Annuler", key="xmn", use_container_width=True):
                    st.session_state._sn = None; st.rerun()

    kids = node.get("children", []); items = node.get("items", [])

    if kids:
        st.markdown(
            '<div style="font-size:.68rem;color:var(--t3);text-transform:uppercase;'
            'letter-spacing:.1em;margin:18px 0 6px;">📁 Sous-dossiers</div>',
            unsafe_allow_html=True)
        for idx, ch in enumerate(kids):
            ni = len(ch.get("items", [])); nc = len(ch.get("children", []))
            is_last = (idx == len(kids) - 1)
            bot_pct = "50%" if is_last else "100%"
            svg = (f'<svg width="30" height="52" style="display:block;">'
                   f'<line x1="10" y1="0" x2="10" y2="{bot_pct}" '
                   f'stroke="#c8dbc0" stroke-width="2"/>'
                   f'<line x1="10" y1="26" x2="26" y2="26" '
                   f'stroke="#c8dbc0" stroke-width="2"/>'
                   f'<circle cx="26" cy="26" r="4" fill="#4a7c59" opacity=".75"/>'
                   f'</svg>')
            cs, ci, cb2 = st.columns([0.05, 0.72, 0.23])
            with cs: st.markdown(svg, unsafe_allow_html=True)
            with ci:
                st.markdown(
                    f'<div style="padding:6px 0;line-height:1.3;">'
                    f'<span style="font-size:.85rem;color:#1e2e22;font-weight:500;">'
                    f'📁 {ch["name"]}</span>'
                    f'<span style="font-size:.62rem;color:#7a9e88;margin-left:8px;">'
                    f'{ni} fich. · {nc} ss-doss.</span></div>',
                    unsafe_allow_html=True)
            with cb2:
                if st.button("Ouvrir →", key=f"okid_{ch['id']}",
                             use_container_width=True):
                    set_cat(ch["id"]); st.rerun()
        st.markdown("<br>", unsafe_allow_html=True)

    if items:
        st.markdown(
            '<div style="font-size:.68rem;color:var(--t3);text-transform:uppercase;'
            'letter-spacing:.1em;margin:8px 0 10px;">📄 Fichiers</div>',
            unsafe_allow_html=True)
        cols = st.columns(min(len(items), 4))
        for i, it in enumerate(items):
            with cols[i % 4]:
                icon = "📊" if it["type"] == "table" else "🧠"
                badge = "TABLE" if it["type"] == "table" else "MAP"
                st.markdown(
                    f'<div class="fc"><div class="fc-icon">{icon}</div>'
                    f'<div class="fc-name">{it["name"]}</div>'
                    f'<span class="fc-badge">{badge}</span></div>',
                    unsafe_allow_html=True)
                a, b = st.columns(2)
                with a:
                    if st.button("Ouvrir", key=f"oit_{it['id']}",
                                 use_container_width=True):
                        open_item(it); st.rerun()
                with b:
                    if st.button("🗑️", key=f"dit_{it['id']}",
                                 use_container_width=True):
                        st.session_state[f"dc_{it['id']}"] = True

                if st.session_state.get(f"dc_{it['id']}"):
                    conf = st.text_input("", key=f"dcc_{it['id']}",
                                        placeholder="DELETE",
                                        label_visibility="collapsed")
                    if st.button("Confirmer", key=f"dco_{it['id']}",
                                 type="primary", use_container_width=True):
                        if conf.strip() == "DELETE":
                            ds.rm_item(user, cat_id, it["id"], to_backup=True)
                            (ds.dl_table if it["type"] == "table"
                             else ds.dl_map)(it["file_id"])
                            st.session_state.pop(f"dc_{it['id']}", None)
                            st.rerun()
                        else: st.error("Taper DELETE")

    if not kids and not items:
        st.markdown(
            '<p style="color:var(--t3);font-size:.85rem;margin-top:20px;'
            'text-align:center;">Dossier vide — utilisez la barre d\'outils.</p>',
            unsafe_allow_html=True)


def _render_bc(ds, user, cat_id):
    path = ds.get_path(user, cat_id) if cat_id else []
    parts = ["🏠 Accueil"] + path
    html = '<div class="bc">'
    for i, p in enumerate(parts):
        cls = "bci cur" if i == len(parts) - 1 else "bci"
        html += f'<span class="{cls}">{p}</span>'
        if i < len(parts) - 1: html += '<span class="bcs"> › </span>'
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)
    if path:
        if st.button("🏠", key="bc_home", help="Accueil"):
            set_cat(None); st.rerun()


def _mk_table(ds, user, cat_id, name, cols, rows):
    bc = [user] + ds.get_path(user, cat_id)
    fid = ds.gen_id("tbl")
    ds.mk_table(fid, bc, rows, cols)
    ds.add_item(user, cat_id, {"id": fid, "name": name,
                               "type": "table", "file_id": fid})


def _mk_map(ds, user, cat_id, name):
    bc = [user] + ds.get_path(user, cat_id)
    fid = ds.gen_id("map")
    ds.mk_map(fid, bc)
    ds.add_item(user, cat_id, {"id": fid, "name": name,
                               "type": "map", "file_id": fid})

# ══════════════════════════════════════════════════════════════════════════════
#  §9 — TABLE VIEW
#
#  Architecture définitive :
#  - on_change callback est la SEULE façon fiable de détecter une vraie édition
#  - Le callback copie le DataFrame retourné par data_editor dans session_state
#    SOUS UNE CLÉ SÉPARÉE (pas table_df) pour éviter les conflits de rerun
#  - Au render suivant, on fusionne et on sauvegarde
# ══════════════════════════════════════════════════════════════════════════════
def render_table():
    ds = get_ds()
    item = st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid = item["file_id"]

    # ── Chargement initial ────────────────────────────────────────────────────
    # On charge UNE SEULE FOIS par ouverture de table.
    # table_vis_df est le df stable passé à data_editor — il ne change PAS
    # lors des reruns normaux, seulement lors d'actions explicites.
    if st.session_state.table_df is None or st.session_state.table_fid != fid:
        df_loaded = ds.ld_table(fid)
        if df_loaded is None or df_loaded.empty:
            st.error("Table introuvable."); go_back(); st.rerun(); return
        st.session_state.table_df = df_loaded
        st.session_state.table_fid = fid
        vis0 = [c for c in df_loaded.columns if c != "_location_"]
        st.session_state.table_vis_df = df_loaded[vis0].copy().reset_index(drop=True)
        st.session_state.undo_stack = []
        st.session_state.redo_stack = []

    df: pd.DataFrame = st.session_state.table_df
    vis = [c for c in df.columns if c != "_location_"]

    # ── Header ────────────────────────────────────────────────────────────────
    cb, ct = st.columns([.12, .88])
    with cb:
        if st.button("← Retour", key="tb_back", use_container_width=True):
            ds.sv_table(fid, st.session_state.table_df)
            go_back(); st.rerun()
    with ct:
        st.markdown(f'<div class="pt">📊 {item["name"]}</div>',
                    unsafe_allow_html=True)

    # ── Toolbar ───────────────────────────────────────────────────────────────
    can_u = bool(st.session_state.undo_stack)
    can_r = bool(st.session_state.redo_stack)
    c_u, c_r, c_s, c_v, _ = st.columns([.09, .09, .09, .3, .43])
    with c_u:
        if st.button("↩", key="tu", disabled=not can_u,
                     help=f"Annuler ({len(st.session_state.undo_stack)})",
                     use_container_width=True):
            _tundo(ds, fid); st.rerun()
    with c_r:
        if st.button("↪", key="tr", disabled=not can_r,
                     help=f"Rétablir ({len(st.session_state.redo_stack)})",
                     use_container_width=True):
            _tredo(ds, fid); st.rerun()
    with c_s:
        if st.button("💾", key="ts", help="Sauvegarder maintenant",
                     use_container_width=True):
            ds.sv_table(fid, st.session_state.table_df)
            st.toast("Sauvegardé ✓", icon="✅")
    with c_v:
        _render_version_picker(ds, fid, "table")

    # ── Resize ────────────────────────────────────────────────────────────────
    with st.expander("⚙️ Redimensionner"):
        rr2, rc2, rb2 = st.columns([.35, .35, .3])
        with rr2: nr = st.number_input("Lignes", 1, 5000, len(df), key="rsz_r")
        with rc2: nc = st.number_input("Colonnes", 1, 500, len(vis), key="rsz_c")
        with rb2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Appliquer", key="rsz_ok", use_container_width=True):
                _tpush_undo()
                ndf = ds.resize_table(fid, st.session_state.table_df,
                                      int(nr), int(nc))
                st.session_state.table_df = ndf
                vis2 = [c for c in ndf.columns if c != "_location_"]
                st.session_state.table_vis_df = ndf[vis2].copy().reset_index(drop=True)
                st.session_state.redo_stack = []
                ds.sv_table(fid, ndf); st.rerun()

    st.markdown("---")

    # ── Data editor sans on_change (évite le rerun/flou) ───────────────────────
    # Architecture :
    # 1. data_editor reçoit table_vis_df (stable, ne change pas entre reruns).
    # 2. Il retourne edited (le df avec les modifications de l'utilisateur).
    # 3. On compare edited et table_vis_df via hash numpy (déterministe).
    #    Si différent → vraie modification → on sauvegarde sans rerun.
    # 4. Pas de on_change = pas de rerun supplémentaire = pas de flou.
    ekey = f"tbl_{fid}"

    edited = st.data_editor(
        st.session_state.table_vis_df,
        use_container_width=True,
        num_rows="fixed",
        key=ekey,
        column_config={c: st.column_config.TextColumn(c, width="medium")
                       for c in vis},
        hide_index=False,
    )

    # Détecter les modifications via hash numpy (fiable, déterministe)
    def _df_sig(df):
        return df.fillna("").astype(str).to_numpy().tobytes()

    cur_sig = _df_sig(edited)
    prev_sig = st.session_state.get("_tbl_sig", b"")

    if cur_sig != prev_sig and prev_sig != b"":
        # Vraie modification utilisateur
        _tpush_undo()
        loc = st.session_state.table_df["_location_"].reset_index(drop=True)
        new_df = edited.reset_index(drop=True).copy()
        if len(loc) != len(new_df):
            loc = loc.reindex(range(len(new_df))).fillna("")
        new_df.insert(0, "_location_", loc.values)
        st.session_state.table_df = new_df
        st.session_state.redo_stack = []
        ds.sv_table(fid, new_df)

    # Mettre à jour la signature pour le prochain render
    st.session_state["_tbl_sig"] = cur_sig

    nr2, nc2 = st.session_state.table_vis_df.shape
    st.markdown(
        f'<div style="font-size:.68rem;color:var(--t3);margin-top:5px;">'
        f'📐 {nr2}×{nc2} &nbsp;|&nbsp; ☁️ tables/{fid}.parquet</div>',
        unsafe_allow_html=True)


def _tpush_undo():
    st.session_state.undo_stack.append(
        st.session_state.table_df.copy(deep=True))
    if len(st.session_state.undo_stack) > 50:
        st.session_state.undo_stack.pop(0)


def _tundo(ds, fid):
    if not st.session_state.undo_stack: return
    st.session_state.redo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df = st.session_state.undo_stack.pop()
    # Sync vis_df
    vis = [c for c in st.session_state.table_df.columns if c != "_location_"]
    st.session_state.table_vis_df = st.session_state.table_df[vis].copy().reset_index(drop=True)
    ds.sv_table(fid, st.session_state.table_df)


def _tredo(ds, fid):
    if not st.session_state.redo_stack: return
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df = st.session_state.redo_stack.pop()
    vis = [c for c in st.session_state.table_df.columns if c != "_location_"]
    st.session_state.table_vis_df = st.session_state.table_df[vis].copy().reset_index(drop=True)
    ds.sv_table(fid, st.session_state.table_df)


def _render_version_picker(ds: DataStore, fid: str, kind: str):
    versions = ds.get_versions(fid)
    if not versions: return
    with st.popover(f"🕐 Versions ({len(versions)})"):
        sel_v = st.selectbox("Version", versions, key=f"vsel_{fid}")
        if st.button("⬇ Restaurer", key=f"vrst_{fid}", type="primary",
                     use_container_width=True):
            cols = ([] if kind == "table"
                    else ["_location_", "object_id", "type",
                          "label", "coords", "writable"])
            restored = ds.load_version(fid, sel_v, cols)
            if not restored.empty:
                if kind == "table":
                    st.session_state.table_df = restored
                    ds.sv_table(fid, restored)
                else:
                    ds.sv_map(fid, restored)
                st.success(f"Version {sel_v} restaurée !"); st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  §10 — MAP VIEW
#
#  Architecture complètement repensée :
#  Le canvas est une page HTML autonome qui stocke son état dans sessionStorage.
#  La sauvegarde vers R2 passe par un bouton Streamlit explicite qui lit
#  les données depuis un st.text_area que le canvas remplit.
#
#  Pour l'autosave : le canvas écrit dans un input Streamlit via un pont
#  components.html qui intercepte les messages et déclenche un st.rerun.
#  La clé est de ne PAS utiliser postMessage vers le parent (iframe restrictions)
#  mais d'utiliser un st.text_area comme canal de communication.
# ══════════════════════════════════════════════════════════════════════════════
MAX_CHARS = 500  # absolue max global; la limite réelle est calculée par forme

def _build_map_html(objs_json: str, fid: str, loc: str, autosave: bool = True) -> str:
    as_js = "true" if autosave else "false"
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
html,body{{width:100%;height:100%;overflow:hidden;background:#f4f7f2;
  font-family:'DM Mono',monospace;user-select:none;}}
#tb{{display:flex;align-items:center;gap:4px;padding:5px 8px;
  background:#eef3ea;border-bottom:1.5px solid #c8dbc0;flex-wrap:wrap;min-height:40px;}}
.tb{{padding:3px 9px;border-radius:6px;border:1.5px solid #c8dbc0;
  background:#fff;color:#4a6657;font-size:.70rem;cursor:pointer;
  font-family:inherit;transition:all .15s;white-space:nowrap;}}
.tb:hover{{border-color:#4a7c59;color:#4a7c59;background:#f0f8f2;}}
.tb.on{{border-color:#4a7c59;background:#dff0e2;color:#3a6246;font-weight:600;}}
.sep{{width:1px;height:14px;background:#c8dbc0;margin:0 2px;flex-shrink:0;}}
#zd{{font-size:.63rem;color:#7a9e88;min-width:32px;text-align:center;}}
#hint{{font-size:.60rem;color:#7a9e88;flex:1;margin-left:4px;overflow:hidden;white-space:nowrap;}}
#st{{font-size:.60rem;color:#4a7c59;font-weight:600;margin-left:4px;}}
#cw{{position:relative;overflow:hidden;background:#f8faf6;
  background-image:radial-gradient(circle,#d4e6cc 1px,transparent 1px);
  background-size:24px 24px;}}
canvas{{display:block;cursor:default;}}
#te{{position:absolute;display:none;background:rgba(255,255,255,.97);
  color:#1e2e22;font-family:'DM Mono',monospace;resize:none;outline:none;
  overflow:hidden;text-align:center;
  border:2px solid #4a7c59;border-radius:6px;
  box-shadow:0 3px 16px rgba(74,124,89,.18);}}
#cc{{position:absolute;display:none;font-size:.52rem;color:#7a9e88;
  pointer-events:none;text-align:center;}}
#ah{{position:absolute;top:5px;right:8px;font-size:.62rem;color:#4a7c59;
  background:rgba(74,124,89,.12);padding:2px 7px;border-radius:5px;
  display:none;pointer-events:none;}}
</style></head><body>
<div id="tb">
  <button class="tb on" id="bs" onclick="setTool('s')">&#8598; Sél.</button>
  <button class="tb" id="br" onclick="setTool('r')">&#9645; Rect.</button>
  <button class="tb" id="ba" onclick="setTool('a')">&#8594; Flèche</button>
  <div class="sep"></div>
  <button class="tb" onclick="zoom(.15)">&#43;</button>
  <div id="zd">100%</div>
  <button class="tb" onclick="zoom(-.15)">&#8722;</button>
  <button class="tb" onclick="resetView()">&#8855;</button>
  <div class="sep"></div>
  <button class="tb" onclick="doUndo()">&#8617;</button>
  <button class="tb" onclick="doRedo()">&#8618;</button>
  <div class="sep"></div>
  <button class="tb" onclick="delSel()">&#128465;</button>
  <div class="sep"></div>
  <button class="tb" id="btn_as" onclick="toggleAS()">&#128190; Auto</button>
  <button class="tb" onclick="saveNow(true)" style="border-color:#4a7c59;color:#3a6246;background:#edfaf0;">&#128190; Sauv.</button>
  <span id="hint">Alt+drag: pan | Ctrl+Z/Y: undo/redo | Suppr: effacer</span>
  <span id="st"></span>
</div>
<div id="cw">
  <canvas id="cv"></canvas>
  <textarea id="te"></textarea>
  <div id="cc"></div>
  <div id="ah">&#8594; cliquer la cible</div>
</div>
<script>
const cv=document.getElementById('cv'),ctx=cv.getContext('2d');
const cw=document.getElementById('cw'),te=document.getElementById('te');
const cc=document.getElementById('cc'),ah=document.getElementById('ah');
const stEl=document.getElementById('st'),hintEl=document.getElementById('hint');
const FID="{fid}",LOC="{loc}";
const PAD=20,LINE_H=20,MIN_W=60,MIN_H=40,CHAR_W=6.0;

let objs={objs_json};
let tool='s',sc=1,ox=60,oy=60;
let selId=null,eid=null,asrc=null,idc=1;
let drag=false,dsx=0,dsy=0,dox=0,doy=0;
let rsz=false,rh=null,rs=null;
let drawing=false,dSt={{x:0,y:0}},dEnd={{x:0,y:0}};
let pan=false,panX=0,panY=0,panOX=0,panOY=0;
let undoSt=[],redoSt=[];
let autoSave={as_js};
let _saveTimer=null;

objs.forEach(o=>{{const n=parseInt(String(o.id).replace(/[^0-9]/g,''))||0;if(n>=idc)idc=n+1;}});

// Measure canvas (always sc=1)
const mCtx=(()=>{{const c=document.createElement('canvas');c.width=2;c.height=2;
  const x=c.getContext('2d');x.font='13px DM Mono,monospace';return x;}})();
function mW(t){{mCtx.font='13px DM Mono,monospace';return mCtx.measureText(t).width;}}

function resize(){{
  const tbH=document.getElementById('tb').offsetHeight;
  cw.style.height=(window.innerHeight-tbH)+'px';
  cv.width=cw.clientWidth;cv.height=cw.clientHeight;render();
}}
window.addEventListener('resize',resize);resize();

function tw(cx,cy){{return{{x:(cx-ox)/sc,y:(cy-oy)/sc}};}}
function gp(e){{const r=cv.getBoundingClientRect();return{{x:e.clientX-r.left,y:e.clientY-r.top}};}}

// Hit testing
function inRect(o,wx,wy){{return o.type==='r'&&wx>=o.x&&wx<=o.x+o.w&&wy>=o.y&&wy<=o.y+o.h;}}
function inArrow(o,wx,wy){{
  if(o.type!=='a')return false;
  const dx=o.x2-o.x1,dy=o.y2-o.y1,L=Math.sqrt(dx*dx+dy*dy);
  if(L<1)return false;
  const t=((wx-o.x1)*dx+(wy-o.y1)*dy)/(L*L);
  if(t<0||t>1)return false;
  const px=o.x1+t*dx,py=o.y1+t*dy;
  return Math.sqrt((wx-px)**2+(wy-py)**2)<10/sc;
}}
function hitAny(wx,wy){{
  // Test les flèches en priorité, puis les rectangles
  for(let i=objs.length-1;i>=0;i--)if(inArrow(objs[i],wx,wy))return objs[i];
  for(let i=objs.length-1;i>=0;i--)if(inRect(objs[i],wx,wy))return objs[i];
  return null;
}}
function hitRect(wx,wy){{for(let i=objs.length-1;i>=0;i--)if(inRect(objs[i],wx,wy))return objs[i];return null;}}
function handles(o){{
  if(o.type!=='r')return[];
  return[{{id:'se',x:o.x+o.w,y:o.y+o.h}},{{id:'e',x:o.x+o.w,y:o.y+o.h/2}},
         {{id:'s',x:o.x+o.w/2,y:o.y+o.h}},{{id:'n',x:o.x+o.w/2,y:o.y}},
         {{id:'nw',x:o.x,y:o.y}},{{id:'w',x:o.x,y:o.y+o.h/2}}];
}}
function hitHandle(o,wx,wy){{
  const t=8/sc;
  for(const h of handles(o))if(Math.abs(wx-h.x)<t&&Math.abs(wy-h.y)<t)return h.id;
  return null;
}}

// Text
function wrapText(text,maxW){{
  if(!text)return[''];
  const words=text.split(' ');const lines=[];let line='';
  for(const w of words){{const t=line?line+' '+w:w;if(mW(t)>maxW&&line){{lines.push(line);line=w;}}else line=t;}}
  const all=line?[...lines,line]:lines;const result=[];
  for(const ln of all){{
    if(mW(ln)<=maxW){{result.push(ln);continue;}}
    let cur='';for(const ch of ln){{if(mW(cur+ch)>maxW&&cur){{result.push(cur);cur=ch;}}else cur+=ch;}}
    if(cur)result.push(cur);
  }}
  return result.length?result:[''];
}}
function maxChars(w,h){{return Math.max(4,Math.floor(Math.max(1,(w-PAD*2)/CHAR_W)*Math.max(1,(h-PAD*2)/LINE_H)));}}
function minHforW(label,w){{if(!label)return MIN_H;return Math.max(MIN_H,wrapText(label,Math.max(1,w-PAD*2)).length*LINE_H+PAD*2);}}

// Undo/Redo
function pushU(){{undoSt.push(JSON.stringify(objs));if(undoSt.length>50)undoSt.shift();redoSt=[];}}
function doUndo(){{if(!undoSt.length)return;redoSt.push(JSON.stringify(objs));objs=JSON.parse(undoSt.pop());selId=null;render();}}
function doRedo(){{if(!redoSt.length)return;undoSt.push(JSON.stringify(objs));objs=JSON.parse(redoSt.pop());selId=null;render();}}

// Autosave
function toggleAS(){{
  autoSave=!autoSave;
  document.getElementById('btn_as').classList.toggle('on',autoSave);
  stEl.textContent=autoSave?'Auto ON':'Auto OFF';setTimeout(()=>stEl.textContent='',2000);
}}
function serializeObjs(){{
  if(eid!==null){{const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;}}
  const rows=objs.map(o=>{{
    let coords={{}};
    if(o.type==='r')coords={{x:o.x,y:o.y,w:o.w,h:o.h}};
    else coords={{x1:o.x1,y1:o.y1,x2:o.x2,y2:o.y2,srcId:o.srcId,dstId:o.dstId}};
    return{{object_id:o.id,type:o.type==='r'?'rectangle':'arrow',
            label:o.label||'',coords:JSON.stringify(coords),writable:o.w2?'false':'true'}};
  }});
  return JSON.stringify({{fid:FID,loc:LOC,rows,ts:Date.now()}});
}}

function saveNow(force){{
  if(!force&&!autoSave)return;
  clearTimeout(_saveTimer);
  _saveTimer=setTimeout(()=>{{
    const payload=serializeObjs();
    // sessionStorage : accessible dans le même iframe, pas de cross-origin
    try{{sessionStorage.setItem('map_data_'+FID,payload);}}catch(_){{}}
    // Déclencher sauvegarde via query param : encoder en base64 et naviguer
    try{{
      const b64=btoa(unescape(encodeURIComponent(payload)));
      const url=new URL(window.parent.location.href);
      url.searchParams.set('mapdata',b64);
      window.parent.history.replaceState({{}},'',url.toString());
      // Forcer Streamlit à lire le query param en simulant une navigation
      window.parent.dispatchEvent(new Event('popstate'));
    }}catch(e){{
      // Si cross-origin bloqué, stocker pour sauvegarde manuelle
    }}
    stEl.textContent='\u2713';setTimeout(()=>stEl.textContent='',1500);
  }},400);
}}

// Sauvegarde forcée immédiate (ex: avant fermeture)
function saveImmediate(){{
  const payload=serializeObjs();
  try{{sessionStorage.setItem('map_data_'+FID,payload);}}catch(_){{}}
  try{{
    const b64=btoa(unescape(encodeURIComponent(payload)));
    const url=new URL(window.parent.location.href);
    url.searchParams.set('mapdata',b64);
    window.parent.history.replaceState({{}},'',url.toString());
  }}catch(e){{}}
}}
// Init autosave button state
if(autoSave)document.getElementById('btn_as').classList.add('on');

// Render
function render(){{
  ctx.clearRect(0,0,cv.width,cv.height);
  ctx.save();ctx.translate(ox,oy);ctx.scale(sc,sc);
  // Rects d'abord, flèches par-dessus (pointes toujours visibles)
  objs.filter(o=>o.type==='r').forEach(drawRect);
  objs.filter(o=>o.type==='a').forEach(drawArrow);
  if(drawing){{
    const w=dEnd.x-dSt.x,h=dEnd.y-dSt.y;
    ctx.save();ctx.strokeStyle='#4a7c59';ctx.fillStyle='rgba(74,124,89,.06)';
    ctx.lineWidth=1.5/sc;ctx.setLineDash([5/sc,3/sc]);
    rrP(dSt.x,dSt.y,w,h,6/sc);ctx.fill();ctx.stroke();ctx.restore();
  }}
  ctx.restore();
  document.getElementById('zd').textContent=Math.round(sc*100)+'%';
  if(eid!==null){{const o=objs.find(x=>x.id===eid);if(o)reposTE(o);}}
}}

function drawRect(o){{
  const sel=(o.id===selId),editing=(o.id===eid);
  ctx.save();
  if(sel&&!editing){{ctx.shadowColor='rgba(74,124,89,.35)';ctx.shadowBlur=12/sc;}}
  ctx.fillStyle=editing?'rgba(255,255,255,0)':'#fff';
  ctx.strokeStyle=sel||editing?'#4a7c59':(o.w2?'#a0b8a0':'#c8dbc0');
  ctx.lineWidth=(sel||editing?2:1.5)/sc;
  if(o.w2&&!sel)ctx.setLineDash([4/sc,2/sc]);
  ctx.beginPath();rrP(o.x,o.y,o.w,o.h,8/sc);ctx.fill();ctx.stroke();ctx.setLineDash([]);
  if(o.label&&!editing){{
    ctx.save();ctx.font=(13/sc)+'px DM Mono,monospace';
    ctx.fillStyle='#1e2e22';ctx.textAlign='center';ctx.textBaseline='middle';
    const tw2=o.w-PAD*2,th2=o.h-PAD*2;
    const lines=wrapText(o.label,tw2),lh=LINE_H,tot=lines.length*lh;
    const sy=o.y+PAD+(th2-tot)/2+lh/2;
    ctx.save();ctx.beginPath();ctx.rect(o.x+PAD,o.y+PAD,tw2,th2);ctx.clip();
    lines.forEach((l,i)=>ctx.fillText(l,o.x+o.w/2,sy+i*lh));
    ctx.restore();
    if(sel){{
      const cnt=o.label.length,cap=maxChars(o.w,o.h);
      ctx.font=(8/sc)+'px DM Mono,monospace';
      ctx.fillStyle=cnt>=cap?'#c0392b':'rgba(122,158,136,.75)';
      ctx.textAlign='center';ctx.textBaseline='top';
      ctx.fillText(cnt+'/'+cap,o.x+o.w/2,o.y+o.h+2/sc);
    }}
    ctx.restore();
  }}
  if(sel&&!editing){{
    ctx.save();
    handles(o).forEach(h=>{{
      ctx.fillStyle='#4a7c59';ctx.strokeStyle='#fff';ctx.lineWidth=1/sc;
      ctx.beginPath();ctx.arc(h.x,h.y,5/sc,0,Math.PI*2);ctx.fill();ctx.stroke();
    }});
    ctx.restore();
  }}
  ctx.restore();
}}

function drawArrow(o){{
  const sel=(o.id===selId);
  const ang=Math.atan2(o.y2-o.y1,o.x2-o.x1);
  const AH=22/sc; // taille pointe en pixels monde
  // Ligne raccourcie pour laisser la pointe visible
  const ex=o.x2-Math.cos(ang)*AH*0.75, ey=o.y2-Math.sin(ang)*AH*0.75;
  ctx.save();
  // Ligne
  ctx.strokeStyle=sel?'#2d4f38':'#4a7c59';
  ctx.lineWidth=(sel?3.5:2.5)/sc;ctx.lineCap='round';
  ctx.beginPath();ctx.moveTo(o.x1,o.y1);ctx.lineTo(ex,ey);ctx.stroke();
  // Cercle de sélection au milieu
  if(sel){{
    ctx.fillStyle='rgba(74,124,89,.18)';
    ctx.beginPath();ctx.arc((o.x1+o.x2)/2,(o.y1+o.y2)/2,10/sc,0,Math.PI*2);ctx.fill();
  }}
  // Pointe triangle — dessinée en dernier (au-dessus de la ligne)
  ctx.fillStyle=sel?'#2d4f38':'#3a6246';
  ctx.strokeStyle=sel?'#1e2e22':'#2d4f38';
  ctx.lineWidth=0.8/sc;
  ctx.beginPath();
  ctx.moveTo(o.x2,o.y2);
  ctx.lineTo(o.x2-AH*Math.cos(ang-0.45),o.y2-AH*Math.sin(ang-0.45));
  ctx.lineTo(o.x2-AH*0.5*Math.cos(ang),o.y2-AH*0.5*Math.sin(ang));
  ctx.lineTo(o.x2-AH*Math.cos(ang+0.45),o.y2-AH*Math.sin(ang+0.45));
  ctx.closePath();
  ctx.fill();ctx.stroke();
  ctx.restore();
}}

function rrP(x,y,w,h,r){{
  if(w<0){{x+=w;w=-w;}}if(h<0){{y+=h;h=-h;}}r=Math.min(r,w/2,h/2);
  ctx.beginPath();ctx.moveTo(x+r,y);ctx.lineTo(x+w-r,y);ctx.quadraticCurveTo(x+w,y,x+w,y+r);
  ctx.lineTo(x+w,y+h-r);ctx.quadraticCurveTo(x+w,y+h,x+w-r,y+h);
  ctx.lineTo(x+r,y+h);ctx.quadraticCurveTo(x,y+h,x,y+h-r);
  ctx.lineTo(x,y+r);ctx.quadraticCurveTo(x,y,x+r,y);ctx.closePath();
}}

// Textarea editor
function reposTE(o){{
  const cx=o.x*sc+ox,cy=o.y*sc+oy;
  te.style.left=cx+'px';te.style.top=cy+'px';
  te.style.width=(o.w*sc)+'px';te.style.height=(o.h*sc)+'px';
  te.style.fontSize=(13*sc)+'px';te.style.lineHeight=(LINE_H*sc)+'px';
  te.style.padding=(PAD*sc)+'px';te.style.boxSizing='border-box';
  te.style.wordBreak='break-word';te.style.whiteSpace='pre-wrap';
  te.style.overflowWrap='break-word';te.style.textAlign='center';te.style.overflow='hidden';
  te.maxLength=maxChars(o.w,o.h);
  const cnt=te.value.length,cap=maxChars(o.w,o.h);
  cc.style.left=cx+'px';cc.style.top=(cy+o.h*sc+2)+'px';cc.style.width=(o.w*sc)+'px';
  cc.style.color=cnt>=cap?'#c0392b':'#7a9e88';cc.textContent=cnt+'/'+cap;
}}
function openTE(o){{
  if(o.w2)return;eid=o.id;te.value=o.label||'';
  te.style.display='block';cc.style.display='block';
  reposTE(o);te.focus();const l=te.value.length;te.setSelectionRange(l,l);render();
}}
function closeTE(){{
  if(eid===null)return;const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;
  eid=null;te.style.display='none';cc.style.display='none';render();saveNow(false);
}}
te.addEventListener('input',()=>{{const o=objs.find(x=>x.id===eid);if(o){{o.label=te.value;reposTE(o);render();saveNow(false);}}}});
te.addEventListener('blur',()=>{{}});
te.addEventListener('keydown',e=>{{if(e.key==='Escape')closeTE();e.stopPropagation();}});

// Tools
function setTool(t){{
  tool=t;asrc=null;ah.style.display='none';drawing=false;
  ['bs','br','ba'].forEach(id=>document.getElementById(id)?.classList.remove('on'));
  const b=document.getElementById('b'+t);if(b)b.classList.add('on');
  cv.style.cursor=t==='s'?'default':'crosshair';
  const H={{s:'Clic: sélect | 2e clic: écrire | Drag: déplacer | Alt+drag: pan',
            r:'Glisser: créer | Clic existant: sélect',a:'Clic source puis cible'}};
  hintEl.textContent=H[t]||'';
}}

// Mouse
cv.addEventListener('mousedown',e=>{{
  e.preventDefault();const cp=gp(e),wp=tw(cp.x,cp.y);
  if(e.altKey||e.button===1){{
    if(eid!==null){{const eo=objs.find(x=>x.id===eid);if(!eo||!inRect(eo,wp.x,wp.y))closeTE();}}
    pan=true;panX=cp.x;panY=cp.y;panOX=ox;panOY=oy;cv.style.cursor='grabbing';return;
  }}
  if(e.button!==0)return;
  if(eid!==null){{const eo=objs.find(x=>x.id===eid);if(eo&&inRect(eo,wp.x,wp.y))return;closeTE();}}
  if(tool==='s'){{
    const sel=selId?objs.find(o=>o.id===selId):null;
    if(sel&&sel.type==='r'){{const h=hitHandle(sel,wp.x,wp.y);if(h){{pushU();rsz=true;rh=h;dsx=wp.x;dsy=wp.y;rs={{x:sel.x,y:sel.y,w:sel.w,h:sel.h}};return;}}}}
    const hit=hitAny(wp.x,wp.y);
    if(hit){{
      if(hit.type==='r'){{if(selId===hit.id){{openTE(hit);return;}}pushU();selId=hit.id;drag=true;dsx=wp.x;dsy=wp.y;dox=hit.x;doy=hit.y;}}
      else{{selId=hit.id;}}
    }}else{{selId=null;}}
    render();
  }}else if(tool==='r'){{
    const hit=hitAny(wp.x,wp.y);
    if(hit){{selId=hit.id;setTool('s');if(hit.type==='r')openTE(hit);render();return;}}
    drawing=true;dSt={{x:wp.x,y:wp.y}};dEnd={{x:wp.x,y:wp.y}};
  }}else if(tool==='a'){{
    const hitA=hitAny(wp.x,wp.y);
    if(hitA&&hitA.type==='a'){{selId=hitA.id;render();return;}}
    const hit=hitRect(wp.x,wp.y);
    if(!hit){{asrc=null;ah.style.display='none';return;}}
    if(!asrc){{asrc=hit.id;selId=hit.id;ah.style.display='block';render();}}
    else if(asrc!==hit.id){{
      pushU();const src=objs.find(o=>o.id===asrc);
      const dcx=hit.x+hit.w/2,dcy=hit.y+hit.h/2;
      const scx=src.x+src.w/2,scy=src.y+src.h/2;
      const sp=edgePt(src,dcx,dcy),dp=edgePt(hit,scx,scy);
      objs.push({{id:'a'+(idc++),type:'a',x1:sp.x,y1:sp.y,
        x2:dp.x,y2:dp.y,srcId:asrc,dstId:hit.id,label:'',w2:false}});
      asrc=null;ah.style.display='none';render();saveNow(false);
    }}
  }}
}});

cv.addEventListener('mousemove',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  if(pan){{ox=panOX+(cp.x-panX);oy=panOY+(cp.y-panY);render();return;}}
  if(drag&&selId){{const o=objs.find(x=>x.id===selId);if(o&&o.type==='r'){{o.x=dox+(wp.x-dsx);o.y=doy+(wp.y-dsy);syncArrows(o);render();}}}}
  if(rsz&&selId){{
    const o=objs.find(x=>x.id===selId);if(o){{
      const dx=wp.x-dsx,dy=wp.y-dsy;
      if(rh.includes('e')){{o.w=Math.max(MIN_W,rs.w+dx);const mh=minHforW(o.label||'',o.w);if(o.h<mh)o.h=mh;}}
      if(rh.includes('w')){{const nw=Math.max(MIN_W,rs.w-dx);o.x=rs.x+(rs.w-nw);o.w=nw;const mh=minHforW(o.label||'',o.w);if(o.h<mh)o.h=mh;}}
      if(rh.includes('s')){{o.h=Math.max(minHforW(o.label||'',o.w),rs.h+dy);}}
      if(rh.includes('n')){{const mh=minHforW(o.label||'',o.w);const nh=Math.max(mh,rs.h-dy);o.y=rs.y+(rs.h-nh);o.h=nh;}}
      syncArrows(o);if(eid===o.id)reposTE(o);render();
    }}
  }}
  if(drawing){{dEnd={{x:wp.x,y:wp.y}};render();}}
}});

cv.addEventListener('mouseup',e=>{{
  cv.style.cursor=tool==='s'?'default':'crosshair';
  if(pan){{pan=false;return;}}
  if(drawing){{
    drawing=false;const w=dEnd.x-dSt.x,h=dEnd.y-dSt.y;
    if(Math.abs(w)>15&&Math.abs(h)>12){{
      pushU();const o={{id:'r'+(idc++),type:'r',
        x:w>0?dSt.x:dSt.x+w,y:h>0?dSt.y:dSt.y+h,
        w:Math.abs(w),h:Math.abs(h),label:'',w2:false}};
      objs.push(o);selId=o.id;render();setTool('s');openTE(o);saveNow(false);
    }}else render();
  }}
  if(drag){{drag=false;saveNow(false);}}
  if(rsz){{rsz=false;saveNow(false);}}
}});

document.addEventListener('keydown',e=>{{
  if(e.target!==document.body)return;
  if(e.key==='Delete'||e.key==='Backspace'){{delSel();return;}}
  if(e.key==='Escape'){{if(eid!==null)closeTE();else{{selId=null;render();}};return;}}
  if(e.ctrlKey||e.metaKey){{if(e.key==='z'&&!e.shiftKey){{doUndo();return;}}if(e.key==='z'&&e.shiftKey||e.key==='y'){{doRedo();return;}}}}
  if(selId&&e.key.length===1&&!e.ctrlKey&&!e.metaKey){{
    const o=objs.find(x=>x.id===selId);
    if(o&&o.type==='r'&&!o.w2){{
      if((o.label||'').length>=maxChars(o.w,o.h))return;
      openTE(o);te.value=(o.label||'')+e.key;o.label=te.value;
      const l=te.value.length;te.setSelectionRange(l,l);reposTE(o);render();
    }}
  }}
}});

cv.addEventListener('wheel',e=>{{
  e.preventDefault();const cp=gp(e),d=e.deltaY<0?.12:-.12;
  const ns=Math.max(.08,Math.min(6,sc+d));
  ox=cp.x-(cp.x-ox)*(ns/sc);oy=cp.y-(cp.y-oy)*(ns/sc);sc=ns;render();
}},{{passive:false}});

function zoom(d){{const cx=cv.width/2,cy=cv.height/2;const ns=Math.max(.08,Math.min(6,sc+d));ox=cx-(cx-ox)*(ns/sc);oy=cy-(cy-oy)*(ns/sc);sc=ns;render();}}
function resetView(){{sc=1;ox=60;oy=60;render();}}
function delSel(){{if(!selId)return;if(eid!==null)closeTE();pushU();objs=objs.filter(o=>o.id!==selId&&o.srcId!==selId&&o.dstId!==selId);selId=null;render();saveNow(false);}}
// Calcule le point de sortie du bord d'un rectangle vers une cible (cx,cy)
function edgePt(r,tx,ty){{
  const cx=r.x+r.w/2,cy=r.y+r.h/2;
  const dx=tx-cx,dy=ty-cy;
  if(Math.abs(dx)<0.001&&Math.abs(dy)<0.001)return{{x:cx,y:cy}};
  // Intersection avec les 4 bords
  const hw=r.w/2,hh=r.h/2;
  let t=Infinity;
  if(Math.abs(dx)>0.001){{
    const tx1=(dx>0?hw:-hw)/dx; if(tx1>0)t=Math.min(t,tx1);
  }}
  if(Math.abs(dy)>0.001){{
    const ty1=(dy>0?hh:-hh)/dy; if(ty1>0)t=Math.min(t,ty1);
  }}
  return{{x:cx+dx*t,y:cy+dy*t}};
}}
function syncArrows(rect){{
  objs.filter(o=>o.type==='a').forEach(a=>{{
    const s=objs.find(x=>x.id===a.srcId),d=objs.find(x=>x.id===a.dstId);
    const scx=s?s.x+s.w/2:a.x1,scy=s?s.y+s.h/2:a.y1;
    const dcx=d?d.x+d.w/2:a.x2,dcy=d?d.y+d.h/2:a.y2;
    if(s){{const p=edgePt(s,dcx,dcy);a.x1=p.x;a.y1=p.y;}}
    if(d){{const p=edgePt(d,scx,scy);a.x2=p.x;a.y2=p.y;}}
  }});
}}

render();
</script></body></html>"""

def render_map():
    ds = get_ds()
    item = st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid = item["file_id"]

    df = ds.ld_map(fid)
    if df is None or df.empty:
        st.error("Map introuvable."); go_back(); st.rerun(); return

    loc = ""
    objs: list[dict] = []
    for _, row in df.iterrows():
        t = str(row.get("type", ""))
        if t == "meta": loc = str(row.get("_location_", "")); continue
        coords: dict = {}
        try: coords = json.loads(str(row.get("coords", "{}")))
        except: pass
        o: dict = {
            "id": str(row["object_id"]),
            "type": "r" if t == "rectangle" else "a",
            "label": str(row.get("label", "")),
            "w2": str(row.get("writable", "true")).lower() == "false",
        }
        if t == "rectangle":
            o.update({"x": float(coords.get("x", 100)),
                      "y": float(coords.get("y", 100)),
                      "w": float(coords.get("w", 180)),
                      "h": float(coords.get("h", 100))})
        elif t == "arrow":
            o.update({"x1": float(coords.get("x1", 0)),
                      "y1": float(coords.get("y1", 0)),
                      "x2": float(coords.get("x2", 100)),
                      "y2": float(coords.get("y2", 100)),
                      "srcId": coords.get("srcId"),
                      "dstId": coords.get("dstId")})
        objs.append(o)

    objs_json = json.dumps(objs, ensure_ascii=False)

    # Header
    cb, ct = st.columns([.12, .88])
    with cb:
        if st.button("← Retour", key="map_back", use_container_width=True):
            go_back(); st.rerun()
    with ct:
        st.markdown(f'<div class="pt">🧠 {item["name"]}</div>',
                    unsafe_allow_html=True)

    c_v, c_h = st.columns([.3, .7])
    with c_v: _render_version_picker(ds, fid, "map")
    with c_h:
        st.markdown(
            '<div style="font-size:.65rem;color:var(--t3);padding-top:8px;">'
            '💡 Clic: sél | 2e clic: écrire | Alt+drag: pan | '
            'Ctrl+Z/Y: undo/redo | 💾 Sauvegarder → coller ci-dessous</div>',
            unsafe_allow_html=True)

    # ── Sauvegarde via query_params ──────────────────────────────────────────
    # Le canvas stocke les données dans sessionStorage.
    # Quand l'utilisateur clique "Sauvegarder" ou "← Retour",
    # le JS encode les données en base64 et redirige vers l'URL courante
    # avec ?mapdata=<base64>. Streamlit détecte le query param et sauvegarde.
    
    # Lire les données de sauvegarde depuis query_params
    raw_qp = st.query_params.get("mapdata", "")
    if raw_qp:
        try:
            import base64 as _b64
            payload = json.loads(_b64.b64decode(raw_qp.encode()).decode())
            if payload.get("fid") == fid:
                _save_map_from_payload(ds, fid, loc, payload)
                st.toast("Map sauvegardée ✓", icon="✅")
        except: pass
        # Nettoyer le query param après traitement
        st.query_params.clear()

    # Canvas (avec sauvegarde intégrée)
    components.html(_build_map_html(objs_json, fid, loc), height=650, scrolling=False)

    st.markdown(
        '<div style="font-size:.65rem;color:var(--t3);margin-top:4px;">'
        '💡 <b>Alt+Drag</b>: panoramique &nbsp;|&nbsp; '
        '<b>Molette</b>: zoom &nbsp;|&nbsp; '
        '<b>Clic</b>: sélectionner &nbsp;|&nbsp; '
        '<b>2e clic / frappe</b>: écrire dans une forme &nbsp;|&nbsp; '
        '<b>Ctrl+Z/Y</b>: undo/redo &nbsp;|&nbsp; '
        '<b>Suppr.</b>: effacer</div>',
        unsafe_allow_html=True)


def _save_map_from_payload(ds: DataStore, fid: str, loc: str, payload: dict):
    rows_in = payload.get("rows", [])
    rows_out = [{
        "_location_": loc, "object_id": "_meta_",
        "type": "meta", "label": "map",
        "coords": "{}", "writable": "false",
    }]
    for r in rows_in:
        t = r.get("type", "")
        rows_out.append({
            "_location_": "",
            "object_id": str(r.get("object_id", "")),
            "type": t,
            "label": str(r.get("label", "")),
            "coords": str(r.get("coords", "{}")),
            "writable": str(r.get("writable", "true")),
        })
    ds.sv_map(fid, pd.DataFrame(rows_out))

# ══════════════════════════════════════════════════════════════════════════════
#  §11 — MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(
        page_title="🍵 WorkSpace", layout="wide",
        initial_sidebar_state="expanded")
    st.markdown(f"<style>{CSS}</style>", unsafe_allow_html=True)
    init_state()
    with st.sidebar: render_sidebar()
    user = st.session_state.get("current_user")
    view = st.session_state.get("view", "folder")
    if user is None: render_all_sessions()
    elif view == "table": render_table()
    elif view == "map": render_map()
    else: render_folder()

if __name__ == "__main__": main()
