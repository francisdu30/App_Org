# =============================================================================
#  WorkSpace Manager — Streamlit Community Cloud  v4  (Matcha Edition)
#  Stockage : Cloudflare R2 (boto3)
# =============================================================================
#  secrets.toml : R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET
# =============================================================================
from __future__ import annotations
import io, json, uuid, time
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

def r2_load(key:str, cols:list[str]) -> pd.DataFrame:
    try:
        obj=get_r2().get_object(Bucket=_bkt(),Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey","404"):
            return pd.DataFrame(columns=cols)
        raise
    except Exception: return pd.DataFrame(columns=cols)

def r2_save(df:pd.DataFrame, key:str):
    buf=io.BytesIO(); df.to_parquet(buf,index=False); buf.seek(0)
    get_r2().put_object(Bucket=_bkt(),Key=key,Body=buf.getvalue())

def r2_del(key:str):
    try: get_r2().delete_object(Bucket=_bkt(),Key=key)
    except: pass

def r2_list(prefix:str) -> list[str]:
    try:
        pag=get_r2().get_paginator("list_objects_v2")
        return [o["Key"] for p in pag.paginate(Bucket=_bkt(),Prefix=prefix) for o in p.get("Contents",[])]
    except: return []

def r2_copy(src:str, dst:str):
    try: get_r2().copy_object(Bucket=_bkt(),CopySource={"Bucket":_bkt(),"Key":src},Key=dst)
    except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  §2 — SESSIONS CACHE
# ══════════════════════════════════════════════════════════════════════════════
SKEY="sessions.parquet"
SCOLS=["key","value"]

def _sget() -> pd.DataFrame:
    if "_sc" not in st.session_state:
        st.session_state._sc=r2_load(SKEY,SCOLS)
    return st.session_state._sc

def _ssave(df:pd.DataFrame):
    r2_save(df,SKEY); st.session_state._sc=df

def _srow(df,k): r=df[df["key"]==k]; return r.iloc[0]["value"] if not r.empty else None
def _sup(df,k,v):
    if k in df["key"].values: df=df.copy(); df.loc[df["key"]==k,"value"]=v
    else: df=pd.concat([df,pd.DataFrame([{"key":k,"value":v}])],ignore_index=True)
    return df
def _sdel(df,k): return df[df["key"]!=k].reset_index(drop=True)

# ══════════════════════════════════════════════════════════════════════════════
#  §3 — DATASTORE
# ══════════════════════════════════════════════════════════════════════════════
MAX_VERSIONS=30

class DataStore:
    def __init__(self):
        try:
            df=_sget()
            if df.empty or "_users_meta_" not in df["key"].values: self._boot()
        except: self._boot()

    def _boot(self):
        try: _ssave(pd.DataFrame([{"key":"_users_meta_","value":"{}"}]))
        except: pass

    # ── Users ─────────────────────────────────────────────────────────────────
    def get_users(self) -> list[str]:
        try: return list(json.loads(_srow(_sget(),"_users_meta_") or "{}").keys())
        except: return []

    def add_user(self,name:str) -> bool:
        df=_sget(); us=json.loads(_srow(df,"_users_meta_") or "{}")
        if name in us: return False
        us[name]={"created_at":str(pd.Timestamp.now())}
        df=_sup(df,"_users_meta_",json.dumps(us)); df=_sup(df,name,json.dumps([]))
        _ssave(df); return True

    def rm_user(self,name:str, to_backup:bool=True):
        """Soft-delete: move all items to _backup_ before removing user."""
        if to_backup:
            # Move each item to backup
            tree=self.get_tree(name)
            all_items=[]
            _tfl(tree,[],all_items,name)
            for cat in all_items:
                for it in cat["items"]:
                    self._backup_item(name,it)
        df=_sget(); us=json.loads(_srow(df,"_users_meta_") or "{}")
        us.pop(name,None); df=_sup(df,"_users_meta_",json.dumps(us)); df=_sdel(df,name); _ssave(df)

    def _backup_item(self,user:str,item:dict):
        """Move a table/map file to backup/ prefix in R2."""
        fid=item.get("file_id",""); t=item.get("type","table")
        src=f"{'tables' if t=='table' else 'maps'}/{fid}.parquet"
        dst=f"backup/{user}/{t}/{fid}.parquet"
        r2_copy(src,dst)
        # Also copy all versions
        for vkey in r2_list(f"versions/{fid}/"):
            day=vkey.split("/")[-1]
            r2_copy(vkey,f"backup/{user}/{t}/versions/{fid}/{day}")
        # Store meta
        meta=self._get_backup_meta()
        meta.append({"user":user,"type":t,"fid":fid,"name":item.get("name","?"),"deleted_at":str(pd.Timestamp.now())})
        self._save_backup_meta(meta)

    def _get_backup_meta(self) -> list:
        try:
            df=r2_load("backup/_meta.parquet",["user","type","fid","name","deleted_at"])
            return df.to_dict("records")
        except: return []

    def _save_backup_meta(self,meta:list):
        df=pd.DataFrame(meta) if meta else pd.DataFrame(columns=["user","type","fid","name","deleted_at"])
        r2_save(df,"backup/_meta.parquet")

    # ── Tree ──────────────────────────────────────────────────────────────────
    def get_tree(self,u:str) -> list:
        try: return json.loads(_srow(_sget(),u) or "[]")
        except: return []

    def _wtree(self,u:str,t:list):
        df=_sget(); df=_sup(df,u,json.dumps(t)); _ssave(df)

    def add_cat(self,u:str,name:str,pid:str|None=None) -> dict:
        t=self.get_tree(u); n={"id":uuid.uuid4().hex[:8],"name":name,"children":[],"items":[],"collapsed":False}
        if pid is None: t.append(n)
        else: _ti(t,pid,n)
        self._wtree(u,t); return n

    def del_cat(self,u:str,cid:str):
        tree=self.get_tree(u)
        # Backup all items recursively
        all_items=[]
        node=_tf(tree,cid)
        if node: _tfl([node],[],all_items,u)
        for cat in all_items:
            for it in cat["items"]: self._backup_item(u,it)
        t=_tr(tree,cid); self._wtree(u,t)

    def ren_cat(self,u,cid,nm): t=self.get_tree(u); _trn(t,cid,nm); self._wtree(u,t)
    def tog_col(self,u,cid): t=self.get_tree(u); _ttog(t,cid); self._wtree(u,t)
    def set_all_collapsed(self,u:str,collapsed:bool):
        t=self.get_tree(u); _tset_col(t,collapsed); self._wtree(u,t)
    def mv_cat(self,u,nid,newp:str|None):
        t=self.get_tree(u); nd=_tf(t,nid)
        if not nd: return
        if newp and _tdesc(nd,newp): return
        t=_tr(t,nid)
        if newp is None: t.append(nd)
        else: _ti(t,newp,nd)
        self._wtree(u,t)
    def add_item(self,u,cid,item): t=self.get_tree(u); _tai(t,cid,item); self._wtree(u,t)
    def rm_item(self,u,cid,iid,to_backup=True):
        t=self.get_tree(u)
        if to_backup:
            node=_tf(t,cid)
            if node:
                for it in node.get("items",[]):
                    if it["id"]==iid: self._backup_item(u,it); break
        _tri(t,cid,iid); self._wtree(u,t)
    def get_path(self,u,cid) -> list[str]:
        t=self.get_tree(u); p=[]; _tpn(t,cid,p); return p
    def get_flat(self,u) -> list[dict]:
        t=self.get_tree(u); r=[]; _tfl(t,[],r,u); return r
    def gen_id(self,pfx="tbl"): return f"{pfx}_{uuid.uuid4().hex[:16]}"

    # ── Versions ──────────────────────────────────────────────────────────────
    def save_version(self,fid:str, src_key:str):
        """Save a dated version if not already saved today. Max 30 versions."""
        today=str(dt_date.today())
        vkey=f"versions/{fid}/{today}.parquet"
        existing=r2_list(f"versions/{fid}/")
        if vkey in existing: return  # Pas de doublon le même jour
        # Copy current file as today's version
        r2_copy(src_key,vkey)
        # Prune to MAX_VERSIONS
        sorted_vers=sorted(existing)
        while len(sorted_vers)>=MAX_VERSIONS:
            r2_del(sorted_vers.pop(0))

    def get_versions(self,fid:str) -> list[str]:
        """Return sorted list of version keys (dates)."""
        keys=r2_list(f"versions/{fid}/")
        return sorted([k.split("/")[-1].replace(".parquet","") for k in keys],reverse=True)

    def load_version(self,fid:str, date_str:str, cols:list[str]) -> pd.DataFrame:
        return r2_load(f"versions/{fid}/{date_str}.parquet",cols)

    # ── Tables ────────────────────────────────────────────────────────────────
    def mk_table(self,fid,bc,rows=20,cols=10) -> pd.DataFrame:
        cns=[f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}" for i in range(cols)]
        df=pd.DataFrame({c:[""]*rows for c in cns})
        df.insert(0,"_location_",[" > ".join(bc)]+[""]*(rows-1))
        r2_save(df,f"tables/{fid}.parquet")
        self.save_version(fid,f"tables/{fid}.parquet")
        return df

    def resize_table(self,fid,df,nr,nc) -> pd.DataFrame:
        vis=[c for c in df.columns if c!="_location_"]; loc=df["_location_"].copy(); data=df[vis].copy()
        cur_c=len(vis)
        if nc>cur_c:
            for i in range(cur_c,nc):
                cn=f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}"; data[cn]=""
        elif nc<cur_c: data=data.iloc[:,:nc]
        cur_r=len(data)
        if nr>cur_r:
            ex=pd.DataFrame({c:[""]*(nr-cur_r) for c in data.columns}); data=pd.concat([data,ex],ignore_index=True)
        elif nr<cur_r: data=data.iloc[:nr]
        loc=loc.reindex(range(nr)).fillna(""); data.insert(0,"_location_",loc.values); return data

    def ld_table(self,fid) -> pd.DataFrame|None:
        df=r2_load(f"tables/{fid}.parquet",[])
        return None if (df.empty and "_location_" not in df.columns) else df

    def sv_table(self,fid,df):
        r2_save(df,f"tables/{fid}.parquet")
        self.save_version(fid,f"tables/{fid}.parquet")

    def dl_table(self,fid): r2_del(f"tables/{fid}.parquet")

    # ── Maps ──────────────────────────────────────────────────────────────────
    def mk_map(self,fid,bc) -> pd.DataFrame:
        df=pd.DataFrame([{"_location_":" > ".join(bc),"object_id":"_meta_","type":"meta",
                          "label":"map","coords":"{}","writable":"false"}])
        r2_save(df,f"maps/{fid}.parquet")
        self.save_version(fid,f"maps/{fid}.parquet")
        return df

    def ld_map(self,fid) -> pd.DataFrame|None:
        cols=["_location_","object_id","type","label","coords","writable"]
        df=r2_load(f"maps/{fid}.parquet",cols)
        if "writable" not in df.columns: df["writable"]="true"
        return None if (df.empty and "object_id" not in df.columns) else df

    def sv_map(self,fid,df):
        r2_save(df,f"maps/{fid}.parquet")
        self.save_version(fid,f"maps/{fid}.parquet")

    def dl_map(self,fid): r2_del(f"maps/{fid}.parquet")

    # ── Backup list ───────────────────────────────────────────────────────────
    def get_backup_list(self) -> list[dict]:
        return self._get_backup_meta()

# ── Tree helpers ──────────────────────────────────────────────────────────────
def _ti(ns,pid,nd):
    for n in ns:
        if n["id"]==pid: n.setdefault("children",[]).append(nd); return True
        if _ti(n.get("children",[]),pid,nd): return True
    return False
def _tr(ns,nid):
    r=[]
    for n in ns:
        if n["id"]==nid: continue
        n["children"]=_tr(n.get("children",[]),nid); r.append(n)
    return r
def _trn(ns,nid,nm):
    for n in ns:
        if n["id"]==nid: n["name"]=nm; return True
        if _trn(n.get("children",[]),nid,nm): return True
    return False
def _ttog(ns,nid):
    for n in ns:
        if n["id"]==nid: n["collapsed"]=not n.get("collapsed",False); return True
        if _ttog(n.get("children",[]),nid): return True
    return False
def _tset_col(ns,col):
    for n in ns: n["collapsed"]=col; _tset_col(n.get("children",[]),col)
def _tf(ns,nid):
    for n in ns:
        if n["id"]==nid: return n
        f=_tf(n.get("children",[]),nid)
        if f: return f
    return None
def _tdesc(nd,tid):
    for c in nd.get("children",[]):
        if c["id"]==tid or _tdesc(c,tid): return True
    return False
def _tai(ns,cid,item):
    for n in ns:
        if n["id"]==cid: n.setdefault("items",[]).append(item); return True
        if _tai(n.get("children",[]),cid,item): return True
    return False
def _tri(ns,cid,iid):
    for n in ns:
        if n["id"]==cid: n["items"]=[i for i in n.get("items",[]) if i["id"]!=iid]; return True
        if _tri(n.get("children",[]),cid,iid): return True
    return False
def _tpn(ns,tid,p):
    for n in ns:
        p.append(n["name"])
        if n["id"]==tid: return True
        if _tpn(n.get("children",[]),tid,p): return True
        p.pop()
    return False
def _tfl(ns,path,res,u):
    for n in ns:
        cp=path+[n["name"]]
        res.append({"id":n["id"],"name":n["name"],"path":cp,"items":n.get("items",[]),"username":u})
        _tfl(n.get("children",[]),cp,res,u)

# ══════════════════════════════════════════════════════════════════════════════
#  §4 — STATE
# ══════════════════════════════════════════════════════════════════════════════
def get_ds() -> DataStore:
    if "_ds" not in st.session_state: st.session_state._ds=DataStore()
    return st.session_state._ds

def init_state():
    defs={
        "current_user":None,"current_cat_id":None,"current_item":None,"view":"folder",
        "table_df":None,"table_serial":None,"undo_stack":[],"redo_stack":[],
    }
    for k,v in defs.items():
        if k not in st.session_state: st.session_state[k]=v
    get_ds()

def set_user(u): st.session_state.update(current_user=u,current_cat_id=None,current_item=None,view="folder")
def set_cat(c): st.session_state.update(current_cat_id=c,current_item=None,view="folder")
def open_item(it): st.session_state.update(current_item=it,view=it["type"],undo_stack=[],redo_stack=[],table_df=None,table_serial=None)
def go_back(): st.session_state.update(current_item=None,view="folder",table_df=None,table_serial=None)

def _ser(df:pd.DataFrame) -> str:
    """Hash déterministe basé sur CSV (plus stable que JSON/parquet)."""
    try: return df.fillna("").astype(str).to_csv(index=False)
    except: return str(time.time())

# ══════════════════════════════════════════════════════════════════════════════
#  §5 — CSS MATCHA
# ══════════════════════════════════════════════════════════════════════════════
CSS="""
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,300;0,400;0,500;1,400&family=Fraunces:opsz,wght@9..144,300;9..144,600;9..144,800&display=swap');
:root{
  --bg:#f4f7f2; --bg2:#eef3ea; --bg3:#e6ede0; --bg4:#dde7d6; --card:#ffffff;
  --ac:#4a7c59; --acl:#5e9b6e; --acd:rgba(74,124,89,.13); --acc:#3a6246;
  --t1:#1e2e22; --t2:#4a6657; --t3:#7a9e88;
  --bd:#c8dbc0; --bda:rgba(74,124,89,.35);
  --shadow:0 2px 12px rgba(74,124,89,.10); --shadow2:0 4px 24px rgba(74,124,89,.15);
  --br:10px;
}
*{font-family:'DM Mono',monospace;}
.stApp{background:var(--bg)!important;color:var(--t1)!important;}
#MainMenu,footer,header{visibility:hidden;}.stDeployButton{display:none;}
section[data-testid="stSidebar"]{background:var(--bg2)!important;border-right:2px solid var(--bd)!important;box-shadow:var(--shadow);}
section[data-testid="stSidebar"] *{color:var(--t2)!important;}
.app-title{font-family:'Fraunces',serif;font-size:1.2rem;font-weight:800;color:var(--ac);
  padding:16px 12px 8px;letter-spacing:.02em;border-bottom:1px solid var(--bd);}
.app-sub{font-size:.62rem;color:var(--t3);font-weight:300;}
.pt{font-family:'Fraunces',serif;font-size:1.65rem;font-weight:800;color:var(--t1);margin-bottom:16px;letter-spacing:-.01em;}
.bc{display:flex;align-items:center;gap:4px;font-size:.75rem;color:var(--t3);margin-bottom:12px;}
.bci{cursor:pointer;color:var(--t2);padding:2px 6px;border-radius:5px;transition:all .15s;}
.bci:hover{background:var(--acd);color:var(--ac);}
.bci.cur{color:var(--ac);cursor:default;font-weight:500;}
.bcs{color:var(--t3);}
.ush{font-family:'Fraunces',serif;font-size:.88rem;font-weight:700;color:var(--t3);
  text-transform:uppercase;letter-spacing:.12em;padding:10px 0 5px;
  border-bottom:1px solid var(--bd);margin:16px 0 10px;}
/* Buttons */
.stButton>button{font-family:'DM Mono',monospace!important;background:var(--card)!important;
  color:var(--t2)!important;border:1.5px solid var(--bd)!important;border-radius:8px!important;
  transition:all .18s!important;font-size:.78rem!important;box-shadow:0 1px 4px rgba(74,124,89,.07)!important;}
.stButton>button:hover{border-color:var(--bda)!important;color:var(--ac)!important;background:var(--acd)!important;box-shadow:var(--shadow)!important;}
.stButton>button[kind="primary"]{background:var(--acd)!important;color:var(--acc)!important;border-color:var(--bda)!important;font-weight:500!important;}
/* Inputs */
.stTextInput>div>div>input,.stNumberInput>div>div>input{background:var(--card)!important;color:var(--t1)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  font-family:'DM Mono',monospace!important;font-size:.82rem!important;}
.stTextInput>div>div>input:focus,.stNumberInput>div>div>input:focus{border-color:var(--ac)!important;box-shadow:0 0 0 3px var(--acd)!important;}
.stSelectbox>div>div{background:var(--card)!important;color:var(--t1)!important;border:1.5px solid var(--bd)!important;border-radius:8px!important;}
.stTextArea>div>div>textarea{background:var(--card)!important;color:var(--t1)!important;border:1.5px solid var(--bd)!important;border-radius:8px!important;font-family:'DM Mono',monospace!important;font-size:.8rem!important;}
.stMarkdown p{font-family:'DM Mono',monospace!important;font-size:.82rem!important;color:var(--t2)!important;}
div[data-testid="stHorizontalBlock"]{gap:5px!important;}
/* Sidebar tree */
.sb-section-label{font-size:.62rem;color:#7a9e88;text-transform:uppercase;letter-spacing:.1em;padding:10px 4px 2px;display:block;}
/* Folder cards */
.fc{background:var(--card);border:1.5px solid var(--bd);border-radius:var(--br);
  padding:14px 12px;transition:all .18s;box-shadow:var(--shadow);}
.fc:hover{border-color:var(--bda);box-shadow:var(--shadow2);transform:translateY(-2px);}
.fc-icon{font-size:1.8rem;margin-bottom:6px;}
.fc-name{font-size:.82rem;color:var(--t1);font-weight:500;word-break:break-word;}
.fc-meta{font-size:.63rem;color:var(--t3);margin-top:3px;}
.fc-badge{font-size:.58rem;padding:2px 6px;border-radius:4px;background:var(--acd);color:var(--ac);font-weight:500;display:inline-block;margin-top:4px;}
/* Branch in folder view */
.branch-row{display:flex;align-items:center;margin:4px 0;}
.branch-vline{width:2px;background:var(--bd);align-self:stretch;margin:0 8px 0 14px;border-radius:2px;min-height:56px;}
.branch-vline.last{background:linear-gradient(var(--bd) 50%,transparent 50%);}
.branch-hline{width:18px;height:2px;background:var(--bd);flex-shrink:0;}
.branch-dot{width:8px;height:8px;border-radius:50%;background:var(--ac);flex-shrink:0;margin-right:8px;box-shadow:0 0 0 2px var(--acd);}
/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg2);}
::-webkit-scrollbar-thumb{background:var(--bd);border-radius:4px;}
"""

# ══════════════════════════════════════════════════════════════════════════════
#  §6 — SIDEBAR (avec branches visuelles fixes + popover ⋯ intégré au nom)
# ══════════════════════════════════════════════════════════════════════════════
def render_sidebar():
    ds=get_ds(); users=ds.get_users(); cu=st.session_state.get("current_user")

    st.markdown('<div class="app-title">🍵 WorkSpace<div class="app-sub">Matcha Edition</div></div>',unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)

    if st.button("🌐 Toutes les sessions",key="btn_all",
                 type="primary" if cu is None else "secondary",use_container_width=True):
        set_user(None); st.rerun()
    for u in users:
        if st.button(f"👤 {u}",key=f"bu_{u}",
                     type="primary" if cu==u else "secondary",use_container_width=True):
            set_user(u); st.rerun()

    st.markdown("---")
    with st.expander("➕ Nouvelle session"):
        nu=st.text_input("Nom",key="nu_i",label_visibility="collapsed",placeholder="Nom d'utilisateur…")
        if st.button("Créer session",key="btn_nu",use_container_width=True):
            if nu.strip():
                if ds.add_user(nu.strip()): set_user(nu.strip()); st.rerun()
                else: st.error("Nom déjà utilisé.")

    if cu:
        st.markdown('<span class="sb-section-label">📁 Arborescence</span>',unsafe_allow_html=True)

        # Tout plier / tout déplier
        cc1,cc2=st.columns(2)
        with cc1:
            if st.button("▶ Tout plier",key="col_all",use_container_width=True):
                ds.set_all_collapsed(cu,True); st.rerun()
        with cc2:
            if st.button("▼ Tout déplier",key="exp_all",use_container_width=True):
                ds.set_all_collapsed(cu,False); st.rerun()

        tree=ds.get_tree(cu)
        _render_sidebar_nodes(ds,cu,tree,depth=0,parent_is_last_flags=[])

        st.markdown("---")
        with st.expander("➕ Catégorie racine"):
            cn=st.text_input("Nom",key="new_cat",label_visibility="collapsed",placeholder="Nom de catégorie…")
            if st.button("Créer",key="btn_nc",use_container_width=True):
                if cn.strip(): ds.add_cat(cu,cn.strip()); st.rerun()

        st.markdown("---")
        with st.expander("⚠️ Supprimer session"):
            st.warning(f"Supprimer la session « {cu} » ?")
            confirm=st.text_input("Taper DELETE pour confirmer",key="del_sess_conf",label_visibility="collapsed",placeholder="DELETE")
            if st.button("Supprimer définitivement",key="btn_du",type="primary",use_container_width=True):
                if confirm.strip()=="DELETE":
                    ds.rm_user(cu,to_backup=True); set_user(None); st.rerun()
                else: st.error("Taper exactement DELETE pour confirmer.")


def _trunc(s:str,n:int=18) -> str:
    return s[:n]+"…" if len(s)>n else s


def _render_sidebar_nodes(ds,user,nodes,depth,parent_is_last_flags,max_depth=2):
    """Rendu arbre avec branches visuelles ASCII propres, 3 niveaux max."""
    cc=st.session_state.get("current_cat_id")
    for idx,n in enumerate(nodes):
        nid=n["id"]; name=n["name"]
        kids=n.get("children",[]); collapsed=n.get("collapsed",False)
        sel=cc==nid; is_last=(idx==len(nodes)-1)

        # ── Branche ASCII ─────────────────────────────────────────────────────
        prefix=""
        for il in parent_is_last_flags:
            prefix+="   " if il else "│  "
        prefix+="└─ " if is_last else "├─ "

        # Couleurs selon profondeur
        if depth==0: name_style="font-size:.82rem;font-weight:500;color:var(--t1);"
        elif depth==1: name_style="font-size:.75rem;color:var(--t2);"
        else: name_style="font-size:.70rem;color:var(--t3);"

        icon="📁" if kids else "📂"
        display=f"{prefix}{icon} {_trunc(name)}"

        # Layout : toggle | label+popover
        if kids and depth<max_depth:
            ca,cb,cc2=st.columns([.1,.75,.15])
            with ca:
                lbl="▶" if collapsed else "▼"
                if st.button(lbl,key=f"tgl_{nid}",use_container_width=True):
                    ds.tog_col(user,nid); st.rerun()
            with cb:
                if st.button(display,key=f"cat_{nid}",
                             type="primary" if sel else "secondary",use_container_width=True):
                    set_cat(nid); st.rerun()
            with cc2:
                _cat_popover(ds,user,nid,name,cc)
        else:
            ca,cb=st.columns([.85,.15])
            with ca:
                if st.button(display,key=f"cat_{nid}",
                             type="primary" if sel else "secondary",use_container_width=True):
                    set_cat(nid); st.rerun()
            with cb:
                _cat_popover(ds,user,nid,name,cc)

        if kids and not collapsed and depth<max_depth:
            _render_sidebar_nodes(ds,user,kids,depth+1,parent_is_last_flags+[is_last],max_depth)


def _cat_popover(ds,user,nid,name,current_cat):
    with st.popover("⋯",use_container_width=False):
        sub=st.text_input("Nouvelle sous-cat.",key=f"sub_{nid}",placeholder="Nom…",label_visibility="collapsed")
        if st.button("➕ Sous-catégorie",key=f"addsub_{nid}",use_container_width=True):
            if sub.strip(): ds.add_cat(user,sub.strip(),pid=nid); st.rerun()
        nn=st.text_input("Renommer",key=f"ren_{nid}",value=name,label_visibility="collapsed")
        if st.button("✏️ Renommer",key=f"doRen_{nid}",use_container_width=True):
            if nn.strip() and nn!=name: ds.ren_cat(user,nid,nn.strip()); st.rerun()
        flat=ds.get_flat(user)
        opts={c["name"]:c["id"] for c in flat if c["id"]!=nid}; opts["(Racine)"]=None
        tgt=st.selectbox("Déplacer vers",list(opts.keys()),key=f"mv_{nid}")
        if st.button("↗️ Déplacer",key=f"doMv_{nid}",use_container_width=True):
            ds.mv_cat(user,nid,opts[tgt]); st.rerun()
        st.divider()
        conf=st.text_input("DELETE pour confirmer",key=f"delconf_{nid}",placeholder="DELETE",label_visibility="collapsed")
        if st.button("🗑️ Supprimer catégorie",key=f"del_{nid}",type="primary",use_container_width=True):
            if conf.strip()=="DELETE":
                ds.del_cat(user,nid)
                if current_cat==nid: set_cat(None)
                st.rerun()
            else: st.error("Taper DELETE")

# ══════════════════════════════════════════════════════════════════════════════
#  §7 — ALL SESSIONS
# ══════════════════════════════════════════════════════════════════════════════
def render_all_sessions():
    ds=get_ds(); users=ds.get_users()
    st.markdown('<div class="pt">🌐 Toutes les sessions</div>',unsafe_allow_html=True)
    if not users: st.info("Aucune session. Créez-en une depuis la barre latérale."); return
    for user in users:
        st.markdown(f'<div class="ush">👤 {user}</div>',unsafe_allow_html=True)
        tree=ds.get_tree(user)
        if not tree: st.markdown('<p style="color:var(--t3);font-size:.8rem;">Aucune catégorie</p>',unsafe_allow_html=True)
        else: _all_cats(ds,user,tree,0)
        if st.button(f"→ Ouvrir {user}",key=f"oas_{user}"): set_user(user); st.rerun()
        st.markdown("<br>",unsafe_allow_html=True)

    # Backup section
    st.markdown("---")
    st.markdown('<div class="pt" style="font-size:1.2rem;">🗄️ Corbeille (backup)</div>',unsafe_allow_html=True)
    _render_backup(ds)


def _all_cats(ds,user,nodes,depth):
    for n in nodes:
        ni=len(n.get("items",[])); nc=len(n.get("children",[]))
        pre="　"*depth; icon="📁" if nc else "📂"
        ca,cb,cc=st.columns([.55,.25,.2])
        with ca: st.markdown(f'<span style="font-size:.82rem;color:var(--t2);">{pre}{icon} <b>{n["name"]}</b></span>',unsafe_allow_html=True)
        with cb:
            if ni or nc: st.markdown(f'<span style="font-size:.68rem;color:var(--ac);">{ni} fich.·{nc} ss</span>',unsafe_allow_html=True)
        with cc:
            if st.button("→",key=f"oac_{user}_{n['id']}"): set_user(user); set_cat(n["id"]); st.rerun()
        if depth==0 and n.get("children"): _all_cats(ds,user,n["children"],1)


def _render_backup(ds:DataStore):
    items=ds.get_backup_list()
    if not items: st.info("Aucun élément supprimé."); return
    for i,it in enumerate(items):
        c1,c2,c3,c4=st.columns([.3,.2,.2,.3])
        with c1: st.markdown(f'**{it.get("name","?")}** ({it.get("type","?")})')
        with c2: st.markdown(f'<span style="font-size:.7rem;color:var(--t3);">Utilisateur: {it.get("user","?")}</span>',unsafe_allow_html=True)
        with c3: st.markdown(f'<span style="font-size:.68rem;color:var(--t3);">{str(it.get("deleted_at",""))[:10]}</span>',unsafe_allow_html=True)
        with c4:
            fid=it.get("fid",""); tp=it.get("type","table")
            vkey=f"backup/{it['user']}/{tp}/{fid}.parquet"
            vers=r2_list(f"backup/{it['user']}/{tp}/versions/{fid}/")
            if st.button(f"⬇ Restaurer",key=f"rst_{fid}_{i}",use_container_width=True):
                # Restore to R2
                dst=f"{'tables' if tp=='table' else 'maps'}/{fid}.parquet"
                r2_copy(vkey,dst)
                st.success(f"Fichier {fid} restauré dans R2. Rajoutez-le manuellement à une catégorie.")

# ══════════════════════════════════════════════════════════════════════════════
#  §8 — FOLDER VIEW
# ══════════════════════════════════════════════════════════════════════════════
def render_folder():
    ds=get_ds(); user=st.session_state.current_user; cat_id=st.session_state.get("current_cat_id")
    _render_bc(ds,user,cat_id)
    if cat_id is None:
        st.markdown('<div class="pt">🏠 Accueil</div>',unsafe_allow_html=True)
        st.info("Sélectionnez ou créez une catégorie dans la barre latérale."); return
    node=_tf(ds.get_tree(user),cat_id)
    if node is None: st.warning("Catégorie introuvable."); set_cat(None); st.rerun(); return
    st.markdown(f'<div class="pt">📁 {node["name"]}</div>',unsafe_allow_html=True)

    c1,c2,c3,_=st.columns([1,1,1,4])
    with c1:
        if st.button("➕ Dossier",use_container_width=True): st.session_state._sn="folder"
    with c2:
        if st.button("📊 Table",use_container_width=True): st.session_state._sn="table"
    with c3:
        if st.button("🧠 Map",use_container_width=True): st.session_state._sn="map"

    sn=st.session_state.get("_sn")
    if sn=="folder":
        with st.container(border=True):
            st.markdown("**➕ Nouveau sous-dossier**")
            fn=st.text_input("Nom",key="nfn",placeholder="Nom…")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="cfn",use_container_width=True):
                    if fn.strip(): ds.add_cat(user,fn.strip(),pid=cat_id); st.session_state._sn=None; st.rerun()
            with b:
                if st.button("Annuler",key="xfn",use_container_width=True): st.session_state._sn=None; st.rerun()
    elif sn=="table":
        with st.container(border=True):
            st.markdown("**📊 Nouvelle table**")
            tn=st.text_input("Nom",key="ntn",placeholder="Nom…")
            tc2,tr2=st.columns(2)
            with tc2: tcols=st.number_input("Colonnes",1,200,10,key="ntc")
            with tr2: trows=st.number_input("Lignes",1,2000,20,key="ntr")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="ctn",use_container_width=True):
                    if tn.strip(): _mk_table(ds,user,cat_id,tn.strip(),int(tcols),int(trows)); st.session_state._sn=None; st.rerun()
            with b:
                if st.button("Annuler",key="xtn",use_container_width=True): st.session_state._sn=None; st.rerun()
    elif sn=="map":
        with st.container(border=True):
            st.markdown("**🧠 Nouvelle map**")
            mn=st.text_input("Nom",key="nmn",placeholder="Nom…")
            a,b=st.columns(2)
            with a:
                if st.button("Créer",key="cmn",use_container_width=True):
                    if mn.strip(): _mk_map(ds,user,cat_id,mn.strip()); st.session_state._sn=None; st.rerun()
            with b:
                if st.button("Annuler",key="xmn",use_container_width=True): st.session_state._sn=None; st.rerun()

    kids=node.get("children",[]); items=node.get("items",[])

    # ── Branches visuelles sous-dossiers ──────────────────────────────────────
    if kids:
        st.markdown('<div style="font-size:.7rem;color:var(--t3);text-transform:uppercase;letter-spacing:.1em;margin:18px 0 6px;">📁 Sous-dossiers</div>',unsafe_allow_html=True)
        for idx,ch in enumerate(kids):
            is_last=(idx==len(kids)-1)
            ni=len(ch.get("items",[])); nc=len(ch.get("children",[]))
            col_branch,col_content=st.columns([.05,.95])
            with col_branch:
                # SVG branche propre
                line_h="calc(50%)" if is_last else "100%"
                st.markdown(f"""<div style="position:relative;width:20px;height:64px;">
  <div style="position:absolute;left:9px;top:0;width:2px;height:{line_h};background:var(--bd,#c8dbc0);"></div>
  <div style="position:absolute;left:9px;top:31px;width:11px;height:2px;background:var(--bd,#c8dbc0);"></div>
  <div style="position:absolute;left:18px;top:27px;width:8px;height:8px;border-radius:50%;background:#4a7c59;box-shadow:0 0 0 2px rgba(74,124,89,.2);"></div>
</div>""",unsafe_allow_html=True)
            with col_content:
                # Sous-dossier : fond transparent, nom compact
                st.markdown(f"""<div style="padding:6px 10px 6px 6px;border-left:2px solid #dde7d6;margin-left:4px;">
  <div style="font-size:.82rem;color:#1e2e22;font-weight:500;">📁 {ch['name']}</div>
  <div style="font-size:.63rem;color:#7a9e88;">{ni} fichiers · {nc} sous-dossiers</div>
</div>""",unsafe_allow_html=True)
                if st.button("Ouvrir →",key=f"okid_{ch['id']}"):
                    set_cat(ch["id"]); st.rerun()
        st.markdown("<br>",unsafe_allow_html=True)

    # ── Fichiers ──────────────────────────────────────────────────────────────
    if items:
        st.markdown('<div style="font-size:.7rem;color:var(--t3);text-transform:uppercase;letter-spacing:.1em;margin:8px 0 10px;">📄 Fichiers</div>',unsafe_allow_html=True)
        cols=st.columns(min(len(items),4))
        for i,it in enumerate(items):
            with cols[i%4]:
                icon="📊" if it["type"]=="table" else "🧠"
                badge="TABLE" if it["type"]=="table" else "MAP"
                st.markdown(f'<div class="fc"><div class="fc-icon">{icon}</div>'
                            f'<div class="fc-name">{it["name"]}</div>'
                            f'<span class="fc-badge">{badge}</span></div>',unsafe_allow_html=True)
                a,b=st.columns(2)
                with a:
                    if st.button("Ouvrir",key=f"oit_{it['id']}",use_container_width=True):
                        open_item(it); st.rerun()
                with b:
                    if st.button("🗑️",key=f"dit_{it['id']}",use_container_width=True,help="Supprimer"):
                        st.session_state[f"del_confirm_{it['id']}"]=True
                # Confirmation DELETE
                if st.session_state.get(f"del_confirm_{it['id']}"):
                    dc=st.text_input("Taper DELETE",key=f"dc_{it['id']}",placeholder="DELETE",label_visibility="collapsed")
                    if st.button("Confirmer suppression",key=f"dco_{it['id']}",type="primary",use_container_width=True):
                        if dc.strip()=="DELETE":
                            ds.rm_item(user,cat_id,it["id"],to_backup=True)
                            (ds.dl_table if it["type"]=="table" else ds.dl_map)(it["file_id"])
                            st.session_state.pop(f"del_confirm_{it['id']}",None); st.rerun()
                        else: st.error("Taper exactement DELETE")

    if not kids and not items:
        st.markdown('<p style="color:var(--t3);font-size:.85rem;margin-top:20px;text-align:center;">Dossier vide — utilisez la barre d\'outils ci-dessus.</p>',unsafe_allow_html=True)


def _render_bc(ds,user,cat_id):
    path=ds.get_path(user,cat_id) if cat_id else []
    parts=["🏠 Accueil"]+path
    html='<div class="bc">'
    for i,p in enumerate(parts):
        cls="bci cur" if i==len(parts)-1 else "bci"
        html+=f'<span class="{cls}">{p}</span>'
        if i<len(parts)-1: html+='<span class="bcs"> › </span>'
    html+="</div>"; st.markdown(html,unsafe_allow_html=True)
    if path:
        if st.button("🏠",key="bc_home",help="Accueil"): set_cat(None); st.rerun()


def _mk_table(ds,user,cat_id,name,cols,rows):
    bc=[user]+ds.get_path(user,cat_id); fid=ds.gen_id("tbl")
    ds.mk_table(fid,bc,rows,cols)
    ds.add_item(user,cat_id,{"id":fid,"name":name,"type":"table","file_id":fid})


def _mk_map(ds,user,cat_id,name):
    bc=[user]+ds.get_path(user,cat_id); fid=ds.gen_id("map")
    ds.mk_map(fid,bc)
    ds.add_item(user,cat_id,{"id":fid,"name":name,"type":"map","file_id":fid})

# ══════════════════════════════════════════════════════════════════════════════
#  §9 — TABLE VIEW
#  FIX DÉFINITIF : on utilise st.session_state[editor_key] directement
#  comme source de vérité, pas le retour de data_editor.
#  Streamlit stocke les éditions dans session_state[key] = {"edited_rows",...}
#  On reconstruit le df depuis ce diff plutôt que depuis le retour du widget.
# ══════════════════════════════════════════════════════════════════════════════
def render_table():
    ds=get_ds(); item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]

    if st.session_state.table_df is None:
        df=ds.ld_table(fid)
        if df is None or df.empty: st.error("Table introuvable."); go_back(); st.rerun(); return
        st.session_state.table_df=df
        vis=[c for c in df.columns if c!="_location_"]
        st.session_state.table_serial=_ser(df[vis])
        st.session_state.undo_stack=[]; st.session_state.redo_stack=[]

    df=st.session_state.table_df
    vis=[c for c in df.columns if c!="_location_"]

    # Header
    cb,ct,_=st.columns([.12,.6,.28])
    with cb:
        if st.button("← Retour",key="tb_back",use_container_width=True):
            ds.sv_table(fid,st.session_state.table_df); go_back(); st.rerun()
    with ct: st.markdown(f'<div class="pt">📊 {item["name"]}</div>',unsafe_allow_html=True)

    # Toolbar
    can_u=bool(st.session_state.undo_stack); can_r=bool(st.session_state.redo_stack)
    cu2,cr2,cs2,cv2,_=st.columns([.09,.09,.09,.3,.43])
    with cu2:
        if st.button("↩",key="tu",disabled=not can_u,help=f"Annuler ({len(st.session_state.undo_stack)})",use_container_width=True):
            _tundo(ds,fid); st.rerun()
    with cr2:
        if st.button("↪",key="tr",disabled=not can_r,help=f"Rétablir ({len(st.session_state.redo_stack)})",use_container_width=True):
            _tredo(ds,fid); st.rerun()
    with cs2:
        if st.button("💾",key="ts",help="Sauvegarder",use_container_width=True):
            ds.sv_table(fid,st.session_state.table_df); st.toast("Sauvegardé ✓",icon="✅")
    with cv2:
        _render_version_picker(ds,fid,"table")

    # Resize
    with st.expander("⚙️ Redimensionner"):
        rr3,rc3,rb3=st.columns([.35,.35,.3])
        with rr3: nr=st.number_input("Lignes",1,5000,len(df),key="rsz_r")
        with rc3: nc=st.number_input("Colonnes",1,500,len(vis),key="rsz_c")
        with rb3:
            st.markdown("<br>",unsafe_allow_html=True)
            if st.button("Appliquer",key="rsz_ok",use_container_width=True):
                _tpush_undo()
                ndf=ds.resize_table(fid,st.session_state.table_df,int(nr),int(nc))
                _commit_table(ds,fid,ndf); st.rerun()

    st.markdown("---")
    visible_df=df[vis].copy().reset_index(drop=True)
    ekey=f"tbl_{fid}"

    # ── DATA EDITOR — clé stable, num_rows fixed ──────────────────────────────
    edited=st.data_editor(
        visible_df,
        use_container_width=True,
        num_rows="fixed",
        key=ekey,
        column_config={c:st.column_config.TextColumn(c,width="medium") for c in vis},
        hide_index=False,
    )

    # ── Comparaison CSV déterministe ──────────────────────────────────────────
    new_serial=_ser(edited)
    if new_serial != st.session_state.table_serial:
        _tpush_undo()
        loc=st.session_state.table_df["_location_"].reset_index(drop=True)
        new_df=edited.reset_index(drop=True).copy()
        if len(loc)!=len(new_df):
            loc=loc.reindex(range(len(new_df))).fillna("")
        new_df.insert(0,"_location_",loc.values)
        _commit_table(ds,fid,new_df)

    nr2,nc2=visible_df.shape
    st.markdown(f'<div style="font-size:.68rem;color:var(--t3);margin-top:5px;">📐 {nr2}×{nc2} &nbsp;|&nbsp; ☁️ tables/{fid}.parquet &nbsp;|&nbsp; 🔄 Autosave actif</div>',unsafe_allow_html=True)


def _commit_table(ds,fid,new_df):
    st.session_state.table_df=new_df
    vis=[c for c in new_df.columns if c!="_location_"]
    st.session_state.table_serial=_ser(new_df[vis])
    st.session_state.redo_stack=[]
    ds.sv_table(fid,new_df)

def _tpush_undo():
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    if len(st.session_state.undo_stack)>50: st.session_state.undo_stack.pop(0)

def _tundo(ds,fid):
    if not st.session_state.undo_stack: return
    st.session_state.redo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df=st.session_state.undo_stack.pop()
    vis=[c for c in st.session_state.table_df.columns if c!="_location_"]
    st.session_state.table_serial=_ser(st.session_state.table_df[vis])
    ds.sv_table(fid,st.session_state.table_df)

def _tredo(ds,fid):
    if not st.session_state.redo_stack: return
    st.session_state.undo_stack.append(st.session_state.table_df.copy(deep=True))
    st.session_state.table_df=st.session_state.redo_stack.pop()
    vis=[c for c in st.session_state.table_df.columns if c!="_location_"]
    st.session_state.table_serial=_ser(st.session_state.table_df[vis])
    ds.sv_table(fid,st.session_state.table_df)


def _render_version_picker(ds:DataStore, fid:str, kind:str):
    """Sélecteur de version (table ou map)."""
    versions=ds.get_versions(fid)
    if not versions: return
    with st.popover(f"🕐 Versions ({len(versions)})"):
        sel_v=st.selectbox("Choisir une version",versions,key=f"vsel_{fid}")
        if st.button("⬇ Restaurer cette version",key=f"vrst_{fid}",type="primary",use_container_width=True):
            cols=[] if kind=="table" else ["_location_","object_id","type","label","coords","writable"]
            restored=ds.load_version(fid,sel_v,cols)
            if not restored.empty:
                if kind=="table":
                    st.session_state.table_df=restored
                    vis=[c for c in restored.columns if c!="_location_"]
                    st.session_state.table_serial=_ser(restored[vis])
                    ds.sv_table(fid,restored)
                    st.success(f"Version {sel_v} restaurée !"); st.rerun()
                else:
                    ds.sv_map(fid,restored)
                    st.success(f"Version {sel_v} restaurée !"); st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
#  §10 — MAP CANVAS
#  FIXES :
#  - Bug duplication : le mousedown sur zone vide déclenchait un draw=true
#    alors que draw n'était pas reset. Fix : reset draw en mousedown si aucun hit
#  - Bug disparition : closeEditor sur blur efface avant que l'objet soit ancré.
#    Fix : on ne ferme PAS l'éditeur sur blur, seulement sur Escape ou clic externe.
#  - Sauvegarde immédiate : chaque événement appelle saveNow() directement.
#  - Le canal de communication : on utilise un localStorage key que Python
#    lit via un st.text_input mis à jour par un composant HTML bridge.
# ══════════════════════════════════════════════════════════════════════════════
MAX_CHARS=500

def _canvas_html(objs_json:str, fid:str, loc:str) -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#f4f7f2;font-family:'DM Mono',monospace;overflow:hidden;user-select:none;}}
#tb{{display:flex;align-items:center;gap:4px;padding:6px 10px;
     background:#eef3ea;border-bottom:1.5px solid #c8dbc0;flex-wrap:wrap;min-height:44px;}}
.tb{{padding:4px 9px;border-radius:7px;border:1.5px solid #c8dbc0;background:#fff;
    color:#4a6657;font-size:.7rem;cursor:pointer;font-family:inherit;transition:all .15s;white-space:nowrap;}}
.tb:hover{{border-color:#4a7c59;color:#4a7c59;background:#f0f8f2;}}
.tb.on{{border-color:#4a7c59;background:#dff0e2;color:#3a6246;font-weight:500;}}
.sep{{width:1px;height:16px;background:#c8dbc0;margin:0 2px;flex-shrink:0;}}
#zd{{font-size:.65rem;color:#7a9e88;min-width:34px;text-align:center;}}
#hint{{font-size:.62rem;color:#7a9e88;margin-left:4px;flex:1;}}
#st{{font-size:.62rem;color:#4a7c59;font-weight:500;}}
#cw{{position:relative;overflow:hidden;width:100%;height:calc(100vh - 48px);
     background:#f8faf6;
     background-image:radial-gradient(circle,#d4e6cc 1px,transparent 1px);
     background-size:24px 24px;}}
#cv{{display:block;}}
#te{{position:absolute;display:none;border:2px solid #4a7c59;
     background:rgba(255,255,255,.98);color:#1e2e22;
     font-family:'DM Mono',monospace;resize:none;outline:none;
     padding:7px;border-radius:7px;overflow:hidden;text-align:center;
     line-height:1.45;box-shadow:0 4px 20px rgba(74,124,89,.18);}}
#cc{{position:absolute;display:none;font-size:.55rem;color:#7a9e88;pointer-events:none;text-align:center;}}
#ah{{position:absolute;top:6px;right:10px;font-size:.65rem;color:#4a7c59;
     background:rgba(74,124,89,.12);padding:3px 8px;border-radius:5px;display:none;pointer-events:none;}}
</style></head><body>
<div id="tb">
  <button class="tb on" id="bs" onclick="setTool('select')">↖ Sélection</button>
  <button class="tb" id="br" onclick="setTool('rect')">▭ Rectangle</button>
  <button class="tb" id="ba" onclick="setTool('arrow')">→ Flèche</button>
  <div class="sep"></div>
  <button class="tb" onclick="zoom(.15)">🔍+</button>
  <div id="zd">100%</div>
  <button class="tb" onclick="zoom(-.15)">🔍−</button>
  <button class="tb" onclick="resetView()">⊡</button>
  <div class="sep"></div>
  <button class="tb" onclick="mapUndo()">↩</button>
  <button class="tb" onclick="mapRedo()">↪</button>
  <div class="sep"></div>
  <button class="tb" onclick="toggleWritable()" id="btn_wr">✎ OUI</button>
  <div class="sep"></div>
  <button class="tb" onclick="delSel()">🗑</button>
  <span id="hint">Alt+drag: panoramique • Ctrl+Z/Y</span>
  <span id="st"></span>
</div>
<div id="cw">
  <canvas id="cv"></canvas>
  <textarea id="te" maxlength="{MAX_CHARS}"></textarea>
  <div id="cc"></div>
  <div id="ah">Source → cible</div>
</div>
<script>
const cv=document.getElementById('cv'),ctx=cv.getContext('2d'),
      cw=document.getElementById('cw'),te=document.getElementById('te'),
      ccEl=document.getElementById('cc'),ah=document.getElementById('ah'),
      stEl=document.getElementById('st'),hintEl=document.getElementById('hint'),
      btnWr=document.getElementById('btn_wr');
const MAX_C={MAX_CHARS},FID="{fid}",LOC="{loc}";

let objs={objs_json};
let tool='select',sc=1,ox=60,oy=60;
let selId=null;
let drag=false,dsx=0,dsy=0,dox=0,doy=0;
let draw=false,drawSt={{x:0,y:0}},drawCr={{x:0,y:0}};
let mouseDownOnExisting=false;  // FIX: track if mousedown hit an existing obj
let rsz=false,rh=null,rs=null;
let pan=false,px=0,py=0,pox=0,poy=0;
let eid=null,asrc=null,idc=1;
let undoSt=[],redoSt=[];
objs.forEach(o=>{{const n=parseInt(String(o.id).replace(/[^0-9]/g,''))||0;if(n>=idc)idc=n+1;}});

// ── Canvas ────────────────────────────────────────────────────────────────
function resizeCv(){{cv.width=cw.clientWidth;cv.height=cw.clientHeight;render();}}
window.addEventListener('resize',resizeCv);resizeCv();

function tw(cx,cy){{return{{x:(cx-ox)/sc,y:(cy-oy)/sc}};}}
function gp(e){{const r=cv.getBoundingClientRect();return{{x:e.clientX-r.left,y:e.clientY-r.top}};}}

// ── Hit testing ───────────────────────────────────────────────────────────
function hR(o,wx,wy){{return o.type==='rectangle'&&wx>=o.x&&wx<=o.x+o.w&&wy>=o.y&&wy<=o.y+o.h;}}
function hA(o,wx,wy){{
  if(o.type!=='arrow')return false;
  const dx=o.x2-o.x1,dy=o.y2-o.y1,len=Math.sqrt(dx*dx+dy*dy);
  if(len<1)return false;
  const t2=((wx-o.x1)*dx+(wy-o.y1)*dy)/(len*len);
  if(t2<0||t2>1)return false;
  const ppx=o.x1+t2*dx,ppy=o.y1+t2*dy;
  return Math.sqrt((wx-ppx)**2+(wy-ppy)**2)<8/sc;
}}
function hTest(wx,wy){{
  for(let i=objs.length-1;i>=0;i--){{if(hR(objs[i],wx,wy)||hA(objs[i],wx,wy))return objs[i];}}
  return null;
}}
function hTestR(wx,wy){{for(let i=objs.length-1;i>=0;i--){{if(hR(objs[i],wx,wy))return objs[i];}}return null;}}
function gH(o){{
  if(o.type!=='rectangle')return[];
  return[{{id:'se',x:o.x+o.w,y:o.y+o.h}},{{id:'e',x:o.x+o.w,y:o.y+o.h/2}},
         {{id:'s',x:o.x+o.w/2,y:o.y+o.h}},{{id:'n',x:o.x+o.w/2,y:o.y}},
         {{id:'nw',x:o.x,y:o.y}},{{id:'w',x:o.x,y:o.y+o.h/2}}];
}}
function hH(o,wx,wy){{const t=8/sc;for(const h of gH(o)){{if(Math.abs(wx-h.x)<t&&Math.abs(wy-h.y)<t)return h.id;}}return null;}}

function minSz(o){{
  if(!o.label)return{{w:80,h:44}};
  ctx.save();ctx.font='13px DM Mono,monospace';
  const ws=o.label.split(' ');let mw=0;
  ws.forEach(w=>{{const m=ctx.measureText(w).width;if(m>mw)mw=m;}});
  ctx.restore();
  return{{w:Math.max(80,mw+32),h:Math.max(44,Math.ceil((o.label.length*7.5)/Math.max(1,mw+8))*13*1.5+24)}};
}}

// ── Undo/Redo ─────────────────────────────────────────────────────────────
function pushU(){{undoSt.push(JSON.stringify(objs));if(undoSt.length>50)undoSt.shift();redoSt=[];}}
function mapUndo(){{if(!undoSt.length)return;redoSt.push(JSON.stringify(objs));objs=JSON.parse(undoSt.pop());selId=null;render();saveNow();}}
function mapRedo(){{if(!redoSt.length)return;undoSt.push(JSON.stringify(objs));objs=JSON.parse(redoSt.pop());selId=null;render();saveNow();}}

// ── Writable ──────────────────────────────────────────────────────────────
function toggleWritable(){{
  const o=objs.find(x=>x.id===selId);if(!o||o.type!=='rectangle')return;
  o.writable=!(o.writable!==false);updateWBtn();render();saveNow();
}}
function updateWBtn(){{
  const o=objs.find(x=>x.id===selId);
  if(!o||o.type!=='rectangle'){{btnWr.textContent='✎ —';return;}}
  btnWr.textContent='✎ '+(o.writable!==false?'OUI':'NON');
}}

// ── Render ────────────────────────────────────────────────────────────────
function render(){{
  ctx.clearRect(0,0,cv.width,cv.height);
  ctx.save();ctx.translate(ox,oy);ctx.scale(sc,sc);
  objs.filter(o=>o.type==='arrow').forEach(drawArrow);
  objs.filter(o=>o.type==='rectangle').forEach(drawRect);
  if(draw){{
    const w=drawCr.x-drawSt.x,h=drawCr.y-drawSt.y;
    ctx.save();ctx.strokeStyle='#4a7c59';ctx.fillStyle='rgba(74,124,89,.06)';
    ctx.lineWidth=1.5/sc;ctx.setLineDash([5/sc,3/sc]);
    rr(ctx,drawSt.x,drawSt.y,w,h,6/sc);ctx.fill();ctx.stroke();ctx.restore();
  }}
  ctx.restore();
  document.getElementById('zd').textContent=Math.round(sc*100)+'%';
}}

function drawRect(o){{
  const sel=o.id===selId;
  ctx.save();
  if(sel){{ctx.shadowColor='rgba(74,124,89,.4)';ctx.shadowBlur=14/sc;}}
  ctx.fillStyle=o.fill||'#ffffff';
  ctx.strokeStyle=sel?'#4a7c59':(o.writable===false?'#b0c8b0':'#c8dbc0');
  ctx.lineWidth=(sel?2.5:1.5)/sc;
  if(o.writable===false){{ctx.setLineDash([5/sc,2/sc]);ctx.strokeStyle='#a0b8a0';}}
  ctx.beginPath();rr(ctx,o.x,o.y,o.w,o.h,10/sc);ctx.fill();ctx.stroke();
  ctx.setLineDash([]);
  if(o.writable===false){{
    ctx.save();ctx.fillStyle='rgba(122,158,136,.5)';
    ctx.font=(9/sc)+'px DM Mono,monospace';ctx.textAlign='right';ctx.textBaseline='top';
    ctx.fillText('🔒',o.x+o.w-4/sc,o.y+4/sc);ctx.restore();
  }}
  if(o.label&&o.id!==eid){{
    ctx.save();
    const fs=Math.max(9,Math.min(14,o.w/12));
    ctx.font=fs+'px DM Mono,monospace';
    ctx.fillStyle='#1e2e22';ctx.textAlign='center';ctx.textBaseline='middle';
    const pad=Math.max(10,o.w*.08);
    const lines=wrapTxt(ctx,o.label,o.w-pad*2),lh=fs*1.5,th=lines.length*lh;
    const sy=o.y+o.h/2-th/2+lh/2;
    ctx.save();ctx.beginPath();ctx.rect(o.x+4/sc,o.y+4/sc,o.w-8/sc,o.h-8/sc);ctx.clip();
    lines.forEach((l,i)=>ctx.fillText(l,o.x+o.w/2,sy+i*lh));
    ctx.restore();ctx.restore();
    // Counter when selected
    if(sel&&o.id!==eid){{
      const cnt=o.label.length;
      ctx.save();ctx.font=(8/sc)+'px DM Mono,monospace';
      ctx.fillStyle=cnt>=MAX_C?'#c0392b':'rgba(122,158,136,.7)';
      ctx.textAlign='center';ctx.textBaseline='top';
      ctx.fillText(cnt+'/'+MAX_C,o.x+o.w/2,o.y+o.h+3/sc);ctx.restore();
    }}
  }}
  if(sel){{
    ctx.save();
    gH(o).forEach(h=>{{
      ctx.fillStyle='#4a7c59';ctx.strokeStyle='#fff';ctx.lineWidth=1.2/sc;
      ctx.shadowColor='rgba(74,124,89,.3)';ctx.shadowBlur=4/sc;
      ctx.beginPath();ctx.arc(h.x,h.y,5.5/sc,0,Math.PI*2);ctx.fill();ctx.stroke();
    }});
    ctx.restore();
  }}
}}

function drawArrow(o){{
  const sel=o.id===selId;
  const x1=o.x1,y1=o.y1,x2=o.x2,y2=o.y2;
  const ang=Math.atan2(y2-y1,x2-x1);
  const AH=18/sc;
  const ex=x2-Math.cos(ang)*AH*.65,ey=y2-Math.sin(ang)*AH*.65;
  ctx.save();
  const grd=ctx.createLinearGradient(x1,y1,x2,y2);
  grd.addColorStop(0,sel?'#7ab88a':'#a8c8b0');
  grd.addColorStop(1,sel?'#4a7c59':'#6a9e7a');
  ctx.strokeStyle=grd;ctx.lineWidth=3/sc;ctx.lineCap='round';
  ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(ex,ey);ctx.stroke();
  ctx.fillStyle=sel?'#3a6246':'#5a8c6a';
  ctx.shadowColor='rgba(74,124,89,.25)';ctx.shadowBlur=5/sc;
  ctx.beginPath();
  ctx.moveTo(x2,y2);
  ctx.lineTo(x2-AH*Math.cos(ang-.42),y2-AH*Math.sin(ang-.42));
  ctx.lineTo(x2-AH*.5*Math.cos(ang),y2-AH*.5*Math.sin(ang));
  ctx.lineTo(x2-AH*Math.cos(ang+.42),y2-AH*Math.sin(ang+.42));
  ctx.closePath();ctx.fill();
  if(sel){{
    const mx=(x1+x2)/2,my=(y1+y2)/2;
    ctx.fillStyle='rgba(74,124,89,.15)';ctx.shadowBlur=0;
    ctx.beginPath();ctx.arc(mx,my,8/sc,0,Math.PI*2);ctx.fill();
  }}
  ctx.restore();
}}

function rr(ctx,x,y,w,h,r){{
  if(w<0){{x+=w;w=-w;}}if(h<0){{y+=h;h=-h;}}r=Math.min(r,w/2,h/2);
  ctx.beginPath();ctx.moveTo(x+r,y);ctx.lineTo(x+w-r,y);ctx.quadraticCurveTo(x+w,y,x+w,y+r);
  ctx.lineTo(x+w,y+h-r);ctx.quadraticCurveTo(x+w,y+h,x+w-r,y+h);
  ctx.lineTo(x+r,y+h);ctx.quadraticCurveTo(x,y+h,x,y+h-r);
  ctx.lineTo(x,y+r);ctx.quadraticCurveTo(x,y,x+r,y);ctx.closePath();
}}
function wrapTxt(ctx,text,maxW){{
  if(!text)return[''];
  const ws=text.split(' ');const lines=[];let line='';
  for(const w of ws){{const t=line?line+' '+w:w;if(ctx.measureText(t).width>maxW&&line){{lines.push(line);line=w;}}else{{line=t;}}}}
  if(line)lines.push(line);return lines.length?lines:[''];
}}

// ── Editor — FIX: ne ferme PAS sur blur ───────────────────────────────────
function openEditor(o,append){{
  if(o.writable===false)return;
  eid=o.id;
  const cx=o.x*sc+ox,cy=o.y*sc+oy,fw=o.w*sc,fh=o.h*sc;
  te.style.cssText=`display:block;position:absolute;left:${{cx}}px;top:${{cy}}px;width:${{fw}}px;height:${{fh}}px;font-size:${{Math.max(10,Math.min(14,o.w/12))}}px;`;
  if(append!==undefined){{te.value=(o.label||'')+append;o.label=te.value;}}
  else{{te.value=o.label||'';}}
  te.maxLength=MAX_C;te.focus();
  const l=te.value.length;te.setSelectionRange(l,l);
  updateCounter(o);render();
}}
function closeEditorSave(){{
  // Called explicitly — not on blur
  if(eid!==null){{
    const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;
    eid=null;te.style.display='none';ccEl.style.display='none';
    render();saveNow();
  }}
}}
function updateCounter(o){{
  if(!o)return;
  const cnt=te.value.length,cx=o.x*sc+ox,cy=o.y*sc+oy,fw=o.w*sc;
  ccEl.style.cssText=`display:block;left:${{cx}}px;top:${{cy+o.h*sc+3}}px;width:${{fw}}px;color:${{cnt>=MAX_C?'#c0392b':'#7a9e88'}};`;
  ccEl.textContent=cnt+'/'+MAX_C;
}}
te.addEventListener('input',()=>{{
  const o=objs.find(x=>x.id===eid);
  if(o){{o.label=te.value;render();updateCounter(o);saveNow();}}
}});
// FIX: blur does NOT close editor — user must click elsewhere on canvas or press Escape
te.addEventListener('blur',()=>{{
  // Re-focus unless we intentionally closed
  if(eid!==null)setTimeout(()=>{{if(eid!==null&&document.activeElement!==te)closeEditorSave();}},150);
}});
te.addEventListener('keydown',e=>{{if(e.key==='Escape'){{closeEditorSave();}}e.stopPropagation();}});

// ── Tool switching ────────────────────────────────────────────────────────
function setTool(t){{
  tool=t;asrc=null;ah.style.display='none';draw=false;mouseDownOnExisting=false;
  ['bs','br','ba'].forEach(id=>document.getElementById(id)?.classList.remove('on'));
  const btn=document.getElementById('b'+t[0]);if(btn)btn.classList.add('on');
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};cv.style.cursor=curs[t]||'default';
  const hints={{select:'Clic: sélectionner | 2e clic: écrire | Drag: déplacer',
                rect:'Glisser: créer | Clic sur existant: sélectionner',
                arrow:'Clic source → clic cible'}};
  hintEl.textContent=hints[t]||'';
}}

// ── Mouse ─────────────────────────────────────────────────────────────────
cv.addEventListener('mousedown',e=>{{
  e.preventDefault();const cp=gp(e),wp=tw(cp.x,cp.y);
  if(e.button===1||(e.button===0&&e.altKey)){{
    pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';
    draw=false;mouseDownOnExisting=false;return;
  }}
  if(e.button!==0)return;

  // FIX: close editor only if clicking outside current editing obj
  if(eid!==null){{
    const eo=objs.find(x=>x.id===eid);
    if(eo&&hR(eo,wp.x,wp.y))return;  // Click inside editor area — let textarea handle it
    closeEditorSave();
  }}

  if(tool==='select'){{
    const sel=selId?objs.find(o=>o.id===selId):null;
    if(sel&&sel.type==='rectangle'){{
      const h=hH(sel,wp.x,wp.y);
      if(h){{pushU();rsz=true;rh=h;dsx=wp.x;dsy=wp.y;rs={{x:sel.x,y:sel.y,w:sel.w,h:sel.h}};mouseDownOnExisting=true;return;}}
    }}
    const hit=hTest(wp.x,wp.y);
    if(hit){{
      mouseDownOnExisting=true;
      if(hit.type==='rectangle'){{
        if(selId===hit.id){{openEditor(hit);return;}}  // 2e clic → écrire
        selId=hit.id;updateWBtn();pushU();drag=true;dsx=wp.x;dsy=wp.y;dox=hit.x;doy=hit.y;
      }}else{{selId=hit.id;updateWBtn();}}
    }}else{{
      mouseDownOnExisting=false;
      selId=null;updateWBtn();pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';
    }}
    render();
  }}else if(tool==='rect'){{
    const hit=hTest(wp.x,wp.y);
    if(hit){{
      mouseDownOnExisting=true;
      selId=hit.id;updateWBtn();setTool('select');
      if(hit.type==='rectangle')openEditor(hit);
      render();return;
    }}
    mouseDownOnExisting=false;
    // FIX: only start drawing if no existing object was hit
    draw=true;drawSt={{x:wp.x,y:wp.y}};drawCr={{x:wp.x,y:wp.y}};
  }}else if(tool==='arrow'){{
    const hit=hTest(wp.x,wp.y);
    if(!hit){{asrc=null;ah.style.display='none';mouseDownOnExisting=false;return;}}
    mouseDownOnExisting=true;
    if(!asrc){{asrc=hit.id;selId=hit.id;updateWBtn();ah.style.display='block';render();}}
    else if(asrc!==hit.id){{
      pushU();
      const src=objs.find(o=>o.id===asrc),dst=hit;
      if(src&&src.type==='rectangle'&&dst.type==='rectangle'){{
        objs.push({{id:'a'+(idc++),type:'arrow',
                    x1:src.x+src.w/2,y1:src.y+src.h/2,
                    x2:dst.x+dst.w/2,y2:dst.y+dst.h/2,
                    label:'',srcId:asrc,dstId:hit.id,writable:true}});
      }}
      asrc=null;ah.style.display='none';render();saveNow();
    }}
  }}
}});

cv.addEventListener('mousemove',e=>{{
  const cp=gp(e),wp=tw(cp.x,cp.y);
  if(pan){{ox=pox+(cp.x-px);oy=poy+(cp.y-py);render();return;}}
  if(drag&&selId){{
    const o=objs.find(x=>x.id===selId);
    if(o&&o.type==='rectangle'){{o.x=dox+(wp.x-dsx);o.y=doy+(wp.y-dsy);syncArrows(o);render();}}
  }}
  if(rsz&&selId){{
    const o=objs.find(x=>x.id===selId);
    if(o){{
      const ms=minSz(o),dx=wp.x-dsx,dy=wp.y-dsy;
      if(rh.includes('e'))o.w=Math.max(ms.w,rs.w+dx);
      if(rh.includes('s'))o.h=Math.max(ms.h,rs.h+dy);
      if(rh.includes('w')){{const nw=Math.max(ms.w,rs.w-dx);o.x=rs.x+(rs.w-nw);o.w=nw;}}
      if(rh.includes('n')){{const nh=Math.max(ms.h,rs.h-dy);o.y=rs.y+(rs.h-nh);o.h=nh;}}
      syncArrows(o);render();
    }}
  }}
  if(draw&&!mouseDownOnExisting){{drawCr={{x:wp.x,y:wp.y}};render();}}
}});

cv.addEventListener('mouseup',e=>{{
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};
  cv.style.cursor=curs[tool]||'default';
  if(pan){{pan=false;return;}}
  // FIX: only create shape if draw was really started (not mouseDownOnExisting)
  if(draw&&!mouseDownOnExisting){{
    draw=false;
    const w=drawCr.x-drawSt.x,h=drawCr.y-drawSt.y;
    if(Math.abs(w)>20&&Math.abs(h)>16){{
      pushU();
      const o={{id:'r'+(idc++),type:'rectangle',
               x:w>0?drawSt.x:drawSt.x+w,y:h>0?drawSt.y:drawSt.y+h,
               w:Math.abs(w),h:Math.abs(h),label:'',fill:'#ffffff',writable:true}};
      objs.push(o);selId=o.id;updateWBtn();
      render();
      // FIX: save IMMEDIATELY before editor opens to lock the object in
      saveNow();
      // Switch to select mode and open editor
      setTool('select');
      openEditor(o,'');
    }}else{{
      draw=false;render();
    }}
  }}else{{
    draw=false;
  }}
  mouseDownOnExisting=false;
  if(drag&&selId){{drag=false;saveNow();}}
  if(rsz){{rsz=false;saveNow();}}
}});

// Frappe directe
document.addEventListener('keydown',e=>{{
  if(e.target!==document.body)return;
  if(e.key==='Delete'||e.key==='Backspace'){{delSel();return;}}
  if(e.key==='Escape'){{if(eid!==null)closeEditorSave();else{{selId=null;updateWBtn();render();}}return;}}
  if((e.key==='z'&&(e.ctrlKey||e.metaKey)&&!e.shiftKey)){{mapUndo();return;}}
  if((e.key==='y'&&(e.ctrlKey||e.metaKey))||(e.key==='z'&&(e.ctrlKey||e.metaKey)&&e.shiftKey)){{mapRedo();return;}}
  if(selId&&e.key.length===1&&!e.ctrlKey&&!e.metaKey){{
    const o=objs.find(x=>x.id===selId);
    if(o&&o.type==='rectangle'&&o.writable!==false){{
      if((o.label||'').length>=MAX_C)return;
      openEditor(o,e.key);
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

function delSel(){{
  if(!selId)return;pushU();
  objs=objs.filter(o=>o.id!==selId&&o.srcId!==selId&&o.dstId!==selId);
  selId=null;updateWBtn();render();saveNow();
}}
function syncArrows(rect){{
  objs.filter(o=>o.type==='arrow').forEach(a=>{{
    const s=objs.find(x=>x.id===a.srcId),d=objs.find(x=>x.id===a.dstId);
    if(s){{a.x1=s.x+s.w/2;a.y1=s.y+s.h/2;}}
    if(d){{a.x2=d.x+d.w/2;a.y2=d.y+d.h/2;}}
  }});
}}

// ── Save — postMessage to Streamlit bridge ────────────────────────────────
function serObjs(){{
  return objs.map(o=>{{
    let c={{}};
    if(o.type==='rectangle')c={{x:o.x,y:o.y,w:o.w,h:o.h}};
    else if(o.type==='arrow')c={{x1:o.x1,y1:o.y1,x2:o.x2,y2:o.y2,srcId:o.srcId,dstId:o.dstId}};
    return{{id:o.id,type:o.type,label:o.label||'',coords:JSON.stringify(c),writable:o.writable!==false}};
  }});
}}
function saveNow(){{
  const payload=JSON.stringify({{fid:FID,loc:LOC,objs:serObjs(),ts:Date.now()}});
  try{{window.parent.postMessage({{type:'mapSave',payload}},'*');}}catch(e){{}}
  stEl.textContent='✓';setTimeout(()=>stEl.textContent='',1500);
}}
render();
</script></body></html>"""


def render_map():
    ds=get_ds(); item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]
    df=ds.ld_map(fid)
    if df is None or df.empty: st.error("Map introuvable."); go_back(); st.rerun(); return

    loc=""
    objs=[]
    for _,row in df.iterrows():
        t=str(row.get("type",""))
        if t=="meta": loc=str(row.get("_location_","")); continue
        coords={}
        try: coords=json.loads(str(row.get("coords","{}")))
        except: pass
        o={"id":str(row["object_id"]),"type":t,"label":str(row.get("label","")),"writable":str(row.get("writable","true")).lower()!="false"}
        if t=="rectangle":
            o.update({"x":float(coords.get("x",100)),"y":float(coords.get("y",100)),
                      "w":float(coords.get("w",180)),"h":float(coords.get("h",100)),"fill":"#ffffff"})
        elif t=="arrow":
            o.update({"x1":float(coords.get("x1",0)),"y1":float(coords.get("y1",0)),
                      "x2":float(coords.get("x2",100)),"y2":float(coords.get("y2",100)),
                      "srcId":coords.get("srcId"),"dstId":coords.get("dstId")})
        objs.append(o)
    objs_json=json.dumps(objs,ensure_ascii=False)

    cb,ct=st.columns([.12,.88])
    with cb:
        if st.button("← Retour",key="map_back",use_container_width=True): go_back(); st.rerun()
    with ct: st.markdown(f'<div class="pt">🧠 {item["name"]}</div>',unsafe_allow_html=True)

    # Versions + bridge
    c_ver,c_hint=st.columns([.3,.7])
    with c_ver: _render_version_picker(ds,fid,"map")
    with c_hint:
        st.markdown('<div style="font-size:.68rem;color:var(--t3);padding-top:8px;">💡 Clic: sél | 2e clic/frappe: écrire | Alt+drag: pan | Ctrl+Z/Y: undo/redo | Sauvegarde auto</div>',unsafe_allow_html=True)

    # ── Bridge postMessage → Python ───────────────────────────────────────────
    # Un composant HTML minuscule écoute les postMessage du canvas et les injecte
    # dans un st.text_input via DOM manipulation.
    bridge=f"""<script>
    (function(){{
      function inject(payload){{
        // Find the hidden input by data-testid proximity
        const all=window.parent.document.querySelectorAll('input[type="text"]');
        all.forEach(inp=>{{
          if(inp.getAttribute('data-mapfid')==='{fid}'){{
            const nv=Object.getOwnPropertyDescriptor(window.parent.HTMLInputElement.prototype,'value');
            nv.set.call(inp,payload);
            inp.dispatchEvent(new Event('input',{{bubbles:true}}));
          }}
        }});
      }}
      window.addEventListener('message',function(e){{
        if(!e.data||e.data.type!=='mapSave')return;
        inject(e.data.payload);
      }});
      // Tag our specific input
      setTimeout(()=>{{
        const inputs=window.parent.document.querySelectorAll('[data-testid="stTextInput"] input');
        // Find by placeholder pattern
        inputs.forEach(inp=>{{
          if(inp.placeholder==='__mapsink_{fid}__')inp.setAttribute('data-mapfid','{fid}');
        }});
      }},500);
    }})();
    </script>"""
    components.html(bridge,height=0)

    sink=st.text_input("",key=f"ms_{fid}",placeholder=f"__mapsink_{fid}__",label_visibility="collapsed")
    if sink and sink.strip().startswith("{"):
        try:
            payload=json.loads(sink)
            if payload.get("fid")==fid and payload.get("objs"):
                _map_save(ds,fid,loc,payload["objs"])
        except: pass

    components.html(_canvas_html(objs_json,fid,loc),height=640,scrolling=False)


def _map_save(ds,fid,loc,objects):
    rows=[{"_location_":loc,"object_id":"_meta_","type":"meta","label":"map","coords":"{}","writable":"false"}]
    for o in objects:
        t=o.get("type",""); coords={}
        if t=="rectangle": coords={k:o.get(k,0) for k in("x","y","w","h")}
        elif t=="arrow": coords={k:o.get(k) for k in("x1","y1","x2","y2","srcId","dstId")}
        rows.append({"_location_":"","object_id":str(o.get("id","")),"type":t,
                     "label":str(o.get("label","")),"coords":json.dumps(coords),
                     "writable":str(o.get("writable",True)).lower()})
    ds.sv_map(fid,pd.DataFrame(rows))

# ══════════════════════════════════════════════════════════════════════════════
#  §11 — MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(page_title="🍵 WorkSpace",layout="wide",initial_sidebar_state="expanded")
    st.markdown(f"<style>{CSS}</style>",unsafe_allow_html=True)
    init_state()
    with st.sidebar: render_sidebar()
    user=st.session_state.get("current_user"); view=st.session_state.get("view","folder")
    if user is None: render_all_sessions()
    elif view=="table": render_table()
    elif view=="map": render_map()
    else: render_folder()

if __name__=="__main__": main()
