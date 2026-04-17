# =============================================================================
#  WorkSpace Manager — Streamlit Community Cloud  v3  (Matcha Edition)
#  Stockage : Cloudflare R2 (boto3)
# =============================================================================
#  secrets.toml : R2_ACCOUNT_ID, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET
# =============================================================================
from __future__ import annotations
import io, json, uuid, hashlib, time
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

# ══════════════════════════════════════════════════════════════════════════════
#  §2 — CACHE SESSIONS (une seule lecture R2 par cycle)
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
class DataStore:
    def __init__(self):
        try:
            df=_sget()
            if df.empty or "_users_meta_" not in df["key"].values: self._boot()
        except: self._boot()

    def _boot(self):
        try: _ssave(pd.DataFrame([{"key":"_users_meta_","value":"{}"}]))
        except: pass

    # Users
    def get_users(self) -> list[str]:
        try: return list(json.loads(_srow(_sget(),"_users_meta_") or "{}").keys())
        except: return []

    def add_user(self,name:str) -> bool:
        df=_sget(); us=json.loads(_srow(df,"_users_meta_") or "{}")
        if name in us: return False
        us[name]={"created_at":str(pd.Timestamp.now())}
        df=_sup(df,"_users_meta_",json.dumps(us)); df=_sup(df,name,json.dumps([]))
        _ssave(df); return True

    def rm_user(self,name:str):
        df=_sget(); us=json.loads(_srow(df,"_users_meta_") or "{}")
        us.pop(name,None); df=_sup(df,"_users_meta_",json.dumps(us)); df=_sdel(df,name); _ssave(df)

    # Tree
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

    def del_cat(self,u,cid): t=_tr(self.get_tree(u),cid); self._wtree(u,t)
    def ren_cat(self,u,cid,nm): t=self.get_tree(u); _trn(t,cid,nm); self._wtree(u,t)
    def tog_col(self,u,cid): t=self.get_tree(u); _ttog(t,cid); self._wtree(u,t)
    def mv_cat(self,u,nid,newp:str|None):
        t=self.get_tree(u); nd=_tf(t,nid)
        if not nd: return
        if newp and _tdesc(nd,newp): return
        t=_tr(t,nid)
        if newp is None: t.append(nd)
        else: _ti(t,newp,nd)
        self._wtree(u,t)
    def add_item(self,u,cid,item): t=self.get_tree(u); _tai(t,cid,item); self._wtree(u,t)
    def rm_item(self,u,cid,iid): t=self.get_tree(u); _tri(t,cid,iid); self._wtree(u,t)
    def get_path(self,u,cid) -> list[str]:
        t=self.get_tree(u); p=[]; _tpn(t,cid,p); return p
    def get_flat(self,u) -> list[dict]:
        t=self.get_tree(u); r=[]; _tfl(t,[],r,u); return r
    def gen_id(self,pfx="tbl"): return f"{pfx}_{uuid.uuid4().hex[:16]}"

    # Tables
    def mk_table(self,fid,bc,rows=20,cols=10) -> pd.DataFrame:
        cns=[f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}" for i in range(cols)]
        df=pd.DataFrame({c:[""]*rows for c in cns})
        df.insert(0,"_location_",[" > ".join(bc)]+[""]*(rows-1))
        r2_save(df,f"tables/{fid}.parquet"); return df

    def resize_table(self,fid,df,nr,nc) -> pd.DataFrame:
        vis=[c for c in df.columns if c!="_location_"]; loc=df["_location_"].copy(); data=df[vis].copy()
        cur_c=len(vis)
        if nc>cur_c:
            for i in range(cur_c,nc):
                cn=f"Col_{chr(65+i%26)}{'_'+str(i//26) if i>=26 else ''}"; data[cn]=""
        elif nc<cur_c: data=data.iloc[:,:nc]
        cur_r=len(data)
        if nr>cur_r:
            ex=pd.DataFrame({c:[""]*( nr-cur_r) for c in data.columns}); data=pd.concat([data,ex],ignore_index=True)
        elif nr<cur_r: data=data.iloc[:nr]
        loc=loc.reindex(range(nr)).fillna(""); data.insert(0,"_location_",loc.values); return data

    def ld_table(self,fid) -> pd.DataFrame|None:
        df=r2_load(f"tables/{fid}.parquet",[])
        return None if (df.empty and "_location_" not in df.columns) else df
    def sv_table(self,fid,df): r2_save(df,f"tables/{fid}.parquet")
    def dl_table(self,fid): r2_del(f"tables/{fid}.parquet")

    # Maps
    def mk_map(self,fid,bc) -> pd.DataFrame:
        df=pd.DataFrame([{"_location_":" > ".join(bc),"object_id":"_meta_","type":"meta",
                          "label":"map","coords":"{}","writable":"false"}])
        r2_save(df,f"maps/{fid}.parquet"); return df
    def ld_map(self,fid) -> pd.DataFrame|None:
        cols=["_location_","object_id","type","label","coords","writable"]
        df=r2_load(f"maps/{fid}.parquet",cols)
        if "writable" not in df.columns: df["writable"]="true"
        return None if (df.empty and "object_id" not in df.columns) else df
    def sv_map(self,fid,df): r2_save(df,f"maps/{fid}.parquet")
    def dl_map(self,fid): r2_del(f"maps/{fid}.parquet")

# Tree helpers
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
    defs={"current_user":None,"current_cat_id":None,"current_item":None,"view":"folder",
          "table_df":None,"table_serial":None,"undo_stack":[],"redo_stack":[],
          "map_json_inbox":"",  # canal de comm iframe → Python
          }
    for k,v in defs.items():
        if k not in st.session_state: st.session_state[k]=v
    get_ds()

def set_user(u): st.session_state.update(current_user=u,current_cat_id=None,current_item=None,view="folder")
def set_cat(c): st.session_state.update(current_cat_id=c,current_item=None,view="folder")
def open_item(it): st.session_state.update(current_item=it,view=it["type"],undo_stack=[],redo_stack=[],table_df=None,table_serial=None)
def go_back(): st.session_state.update(current_item=None,view="folder",table_df=None,table_serial=None)

def _ser(df:pd.DataFrame) -> str:
    """Sérialise un df en JSON déterministe pour détecter les changements."""
    try: return df.fillna("").to_json(orient="values")
    except: return ""

# ══════════════════════════════════════════════════════════════════════════════
#  §5 — CSS MATCHA
# ══════════════════════════════════════════════════════════════════════════════
CSS="""
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,300;0,400;0,500;1,400&family=Fraunces:opsz,wght@9..144,300;9..144,600;9..144,800&display=swap');
:root{
  --bg:  #f4f7f2;
  --bg2: #eef3ea;
  --bg3: #e6edd e0;
  --bg4: #dde7d6;
  --card:#ffffff;
  --ac:  #4a7c59;
  --acl: #5e9b6e;
  --acd: rgba(74,124,89,.13);
  --acc: #3a6246;
  --t1:  #1e2e22;
  --t2:  #4a6657;
  --t3:  #7a9e88;
  --bd:  #c8dbc0;
  --bda: rgba(74,124,89,.35);
  --shadow: 0 2px 12px rgba(74,124,89,.10);
  --shadow2:0 4px 24px rgba(74,124,89,.15);
  --br: 10px;
}
*{font-family:'DM Mono',monospace;}
.stApp{background:var(--bg)!important;color:var(--t1)!important;}
#MainMenu,footer,header{visibility:hidden;}.stDeployButton{display:none;}
section[data-testid="stSidebar"]{background:var(--bg2)!important;border-right:2px solid var(--bd)!important;box-shadow:var(--shadow);}
section[data-testid="stSidebar"] *{color:var(--t2)!important;}

/* Titre app */
.app-title{font-family:'Fraunces',serif;font-size:1.25rem;font-weight:800;color:var(--ac);
  padding:18px 12px 10px;letter-spacing:.02em;border-bottom:1px solid var(--bd);
  display:flex;align-items:center;gap:8px;}
.app-subtitle{font-size:.65rem;color:var(--t3);font-weight:300;margin-top:2px;}

/* Page title */
.pt{font-family:'Fraunces',serif;font-size:1.7rem;font-weight:800;color:var(--t1);margin-bottom:18px;letter-spacing:-.01em;}

/* Breadcrumb */
.bc{display:flex;align-items:center;gap:4px;font-size:.75rem;color:var(--t3);margin-bottom:14px;}
.bci{cursor:pointer;color:var(--t2);padding:2px 6px;border-radius:5px;transition:all .15s;}
.bci:hover{background:var(--acd);color:var(--ac);}
.bci.cur{color:var(--ac);cursor:default;font-weight:500;}
.bcs{color:var(--t3);}
.ush{font-family:'Fraunces',serif;font-size:.9rem;font-weight:700;color:var(--t3);text-transform:uppercase;letter-spacing:.12em;padding:10px 0 5px;border-bottom:1px solid var(--bd);margin:16px 0 10px;}

/* Buttons */
.stButton>button{
  font-family:'DM Mono',monospace!important;
  background:var(--card)!important;color:var(--t2)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  transition:all .18s!important;font-size:.78rem!important;
  box-shadow:0 1px 4px rgba(74,124,89,.07)!important;
}
.stButton>button:hover{border-color:var(--bda)!important;color:var(--ac)!important;background:var(--acd)!important;box-shadow:var(--shadow)!important;}
.stButton>button[kind="primary"]{background:var(--acd)!important;color:var(--acc)!important;border-color:var(--bda)!important;font-weight:500!important;}

/* Inputs */
.stTextInput>div>div>input,.stNumberInput>div>div>input{
  background:var(--card)!important;color:var(--t1)!important;
  border:1.5px solid var(--bd)!important;border-radius:8px!important;
  font-family:'DM Mono',monospace!important;font-size:.82rem!important;
}
.stTextInput>div>div>input:focus,.stNumberInput>div>div>input:focus{border-color:var(--ac)!important;box-shadow:0 0 0 3px var(--acd)!important;}
.stSelectbox>div>div{background:var(--card)!important;color:var(--t1)!important;border:1.5px solid var(--bd)!important;border-radius:8px!important;}
.stTextArea>div>div>textarea{background:var(--card)!important;color:var(--t1)!important;border:1.5px solid var(--bd)!important;border-radius:8px!important;font-family:'DM Mono',monospace!important;font-size:.8rem!important;}
.stMarkdown p{font-family:'DM Mono',monospace!important;font-size:.82rem!important;color:var(--t2)!important;}
div[data-testid="stHorizontalBlock"]{gap:6px!important;}
.stToggle>label{color:var(--t2)!important;}

/* Sidebar tree branch */
.tree-branch{
  display:flex;align-items:stretch;
  margin:0;padding:0;
}
.tree-line{
  width:18px;flex-shrink:0;
  display:flex;flex-direction:column;align-items:center;
  position:relative;
}
.tree-line::before{
  content:'';position:absolute;left:50%;top:0;bottom:0;
  width:1.5px;background:var(--bd);
}
.tree-line.last::before{height:50%;}
.tree-line::after{
  content:'';position:absolute;left:50%;top:50%;
  width:8px;height:1.5px;background:var(--bd);
}
.tree-node{flex:1;min-width:0;}
.tree-root-line{width:2px;background:linear-gradient(to bottom,var(--ac),var(--bd));border-radius:2px;margin:3px 6px 3px 4px;flex-shrink:0;}

/* Folder view cards */
.folder-card{
  background:var(--card);border:1.5px solid var(--bd);border-radius:var(--br);
  padding:14px 12px;cursor:pointer;transition:all .18s;
  box-shadow:var(--shadow);
}
.folder-card:hover{border-color:var(--bda);box-shadow:var(--shadow2);transform:translateY(-2px);}
.folder-card.selected{border-color:var(--ac);background:var(--acd);}
.fc-icon{font-size:1.8rem;margin-bottom:6px;}
.fc-name{font-size:.82rem;color:var(--t1);font-weight:500;word-break:break-word;}
.fc-meta{font-size:.65rem;color:var(--t3);margin-top:3px;}
.fc-badge{font-size:.6rem;padding:2px 6px;border-radius:4px;background:var(--acd);color:var(--ac);font-weight:500;display:inline-block;margin-top:4px;}

/* Branch folder view */
.branch-container{display:flex;gap:0;margin:8px 0;}
.branch-vert{width:20px;flex-shrink:0;position:relative;}
.branch-vert::before{content:'';position:absolute;left:9px;top:0;bottom:0;width:2px;background:var(--bd);}
.branch-vert.last::before{bottom:50%;}
.branch-horiz{display:flex;align-items:center;margin:4px 0;}
.branch-h-line{width:16px;height:2px;background:var(--bd);flex-shrink:0;margin-top:0;}
.branch-dot{width:7px;height:7px;border-radius:50%;background:var(--ac);flex-shrink:0;margin-right:6px;}

/* Scrollbar */
::-webkit-scrollbar{width:5px;height:5px;}
::-webkit-scrollbar-track{background:var(--bg2);}
::-webkit-scrollbar-thumb{background:var(--bd);border-radius:4px;}
::-webkit-scrollbar-thumb:hover{background:var(--t3);}
"""

# ══════════════════════════════════════════════════════════════════════════════
#  §6 — SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
def render_sidebar():
    ds=get_ds(); users=ds.get_users(); cu=st.session_state.get("current_user")
    st.markdown('<div class="app-title">🍵 WorkSpace<div class="app-subtitle">Matcha Edition</div></div>',unsafe_allow_html=True)
    st.markdown("<br>",unsafe_allow_html=True)
    if st.button("🌐 Toutes les sessions",key="btn_all",type="primary" if cu is None else "secondary",use_container_width=True):
        set_user(None); st.rerun()
    for u in users:
        if st.button(f"👤 {u}",key=f"bu_{u}",type="primary" if cu==u else "secondary",use_container_width=True):
            set_user(u); st.rerun()
    st.markdown("---")
    with st.expander("➕ Nouvelle session"):
        nu=st.text_input("Nom",key="nu_i",label_visibility="collapsed",placeholder="Nom d'utilisateur…")
        if st.button("Créer",key="btn_nu",use_container_width=True):
            if nu.strip():
                if ds.add_user(nu.strip()): set_user(nu.strip()); st.rerun()
                else: st.error("Nom déjà utilisé.")
    if cu:
        st.markdown('<div style="font-size:.68rem;color:var(--t3,#7a9e88);text-transform:uppercase;letter-spacing:.1em;padding:10px 0 4px;">📁 Arborescence</div>',unsafe_allow_html=True)
        tree=ds.get_tree(cu)
        _render_tree_sidebar(ds,cu,tree,depth=0,is_last_list=[])
        st.markdown("---")
        with st.expander("➕ Catégorie racine"):
            cn=st.text_input("Nom",key="new_cat",label_visibility="collapsed",placeholder="Nom de catégorie…")
            if st.button("Créer",key="btn_nc",use_container_width=True):
                if cn.strip(): ds.add_cat(cu,cn.strip()); st.rerun()
        st.markdown("---")
        with st.expander("⚠️ Supprimer session"):
            st.warning(f"Supprimer « {cu} » ?")
            if st.button("Confirmer",key="btn_du",type="primary"):
                ds.rm_user(cu); set_user(None); st.rerun()

def _render_tree_sidebar(ds,user,nodes,depth,is_last_list,max_depth=1):
    cc=st.session_state.get("current_cat_id")
    for idx,n in enumerate(nodes):
        nid=n["id"]; name=n["name"]; kids=n.get("children",[]); collapsed=n.get("collapsed",False)
        sel=cc==nid; icon="📂" if not kids else ("📁" if not collapsed else "📁")
        is_last=(idx==len(nodes)-1)

        # Branch visual prefix
        prefix_html=""
        for il in is_last_list:
            if il: prefix_html+='<span style="display:inline-block;width:16px;"></span>'
            else:  prefix_html+='<span style="display:inline-block;width:16px;border-left:1.5px solid #c8dbc0;height:100%;"></span>'
        corner="└" if is_last else "├"
        branch_char=f'<span style="color:#c8dbc0;font-size:.75rem;">{corner}─</span>' if depth>0 else ""

        c1,c2,c3=st.columns([.12,.67,.21])
        with c1:
            if kids and depth<max_depth:
                lbl="▶" if collapsed else "▼"
                if st.button(lbl,key=f"tgl_{nid}",help="Plier/déplier"):
                    ds.tog_col(user,nid); st.rerun()
        with c2:
            indent="　"*depth
            label=f"{icon} {name}"
            btn_label=f"{indent}{branch_char}{label}" if depth==0 else f"{indent}{label}"
            if st.button(btn_label,key=f"cat_{nid}",type="primary" if sel else "secondary",use_container_width=True):
                set_cat(nid); st.rerun()
        with c3:
            with st.popover("⋯"):
                sub=st.text_input("Sous-cat.",key=f"sub_{nid}",placeholder="Nom…",label_visibility="collapsed")
                if st.button("➕ Sous-catégorie",key=f"addsub_{nid}"):
                    if sub.strip(): ds.add_cat(user,sub.strip(),pid=nid); st.rerun()
                nn=st.text_input("Renommer",key=f"ren_{nid}",value=name,label_visibility="collapsed")
                if st.button("✏️ Renommer",key=f"doRen_{nid}"):
                    if nn.strip() and nn!=name: ds.ren_cat(user,nid,nn.strip()); st.rerun()
                flat=ds.get_flat(user)
                opts={c["name"]:c["id"] for c in flat if c["id"]!=nid}; opts["(Racine)"]=None
                tgt=st.selectbox("Déplacer vers",list(opts.keys()),key=f"mv_{nid}")
                if st.button("↗️ Déplacer",key=f"doMv_{nid}"): ds.mv_cat(user,nid,opts[tgt]); st.rerun()
                st.divider()
                if st.button("🗑️ Supprimer",key=f"del_{nid}",type="primary"):
                    ds.del_cat(user,nid)
                    if cc==nid: set_cat(None)
                    st.rerun()

        if kids and not collapsed and depth<max_depth:
            _render_tree_sidebar(ds,user,kids,depth+1,is_last_list+[is_last],max_depth)

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
        if not tree: st.markdown('<p style="color:var(--t3);font-size:.8rem;padding-left:6px;">Aucune catégorie</p>',unsafe_allow_html=True)
        else: _all_sess_cats(ds,user,tree,0)
        if st.button(f"→ Ouvrir {user}",key=f"oas_{user}"): set_user(user); st.rerun()
        st.markdown("<br>",unsafe_allow_html=True)

def _all_sess_cats(ds,user,nodes,depth):
    for n in nodes:
        ni=len(n.get("items",[])); nc=len(n.get("children",[]))
        pre="　"*depth; icon="📁" if nc else "📂"
        ca,cb,cc=st.columns([.55,.25,.2])
        with ca: st.markdown(f'<span style="font-size:.82rem;color:var(--t2);">{pre}{icon} <b>{n["name"]}</b></span>',unsafe_allow_html=True)
        with cb:
            if ni or nc: st.markdown(f'<span style="font-size:.68rem;color:var(--ac);">{ni} fich.·{nc} ss</span>',unsafe_allow_html=True)
        with cc:
            if st.button("→",key=f"oac_{user}_{n['id']}"):
                set_user(user); set_cat(n["id"]); st.rerun()
        if depth==0 and n.get("children"): _all_sess_cats(ds,user,n["children"],1)

# ══════════════════════════════════════════════════════════════════════════════
#  §8 — FOLDER VIEW (branch design)
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

    # Toolbar
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
            tc,tr=st.columns(2)
            with tc: tcols=st.number_input("Colonnes",1,200,10,key="ntc")
            with tr: trows=st.number_input("Lignes",1,2000,20,key="ntr")
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

    # ── Branch view pour les sous-dossiers ────────────────────────────────────
    if kids:
        st.markdown('<div style="font-size:.7rem;color:var(--t3);text-transform:uppercase;letter-spacing:.1em;margin:16px 0 8px;">📁 Sous-dossiers</div>',unsafe_allow_html=True)
        for idx,ch in enumerate(kids):
            is_last=(idx==len(kids)-1)
            ni=len(ch.get("items",[])); nc=len(ch.get("children",[]))
            corner="└──" if is_last else "├──"
            col_line,col_card,col_btn=st.columns([.04,.78,.18])
            with col_line:
                line_color="transparent" if is_last else "var(--bd)"
                st.markdown(f'<div style="position:relative;height:100%;min-height:60px;">'
                            f'<div style="position:absolute;left:8px;top:0;bottom:0;width:2px;background:{line_color};"></div>'
                            f'<div style="position:absolute;left:8px;top:30px;width:14px;height:2px;background:var(--bd);"></div>'
                            f'<div style="position:absolute;left:20px;top:25px;width:8px;height:8px;border-radius:50%;background:var(--ac);"></div>'
                            f'</div>',unsafe_allow_html=True)
            with col_card:
                st.markdown(f'<div class="folder-card" style="margin:3px 0;">'
                            f'<div class="fc-icon">📁</div>'
                            f'<div class="fc-name">{ch["name"]}</div>'
                            f'<div class="fc-meta">{ni} fichiers · {nc} sous-dossiers</div>'
                            f'</div>',unsafe_allow_html=True)
            with col_btn:
                st.markdown("<br>",unsafe_allow_html=True)
                if st.button("Ouvrir",key=f"okid_{ch['id']}",use_container_width=True):
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
                st.markdown(f'<div class="folder-card">'
                            f'<div class="fc-icon">{icon}</div>'
                            f'<div class="fc-name">{it["name"]}</div>'
                            f'<span class="fc-badge">{badge}</span>'
                            f'</div>',unsafe_allow_html=True)
                a,b=st.columns(2)
                with a:
                    if st.button("Ouvrir",key=f"oit_{it['id']}",use_container_width=True):
                        open_item(it); st.rerun()
                with b:
                    if st.button("🗑️",key=f"dit_{it['id']}",use_container_width=True):
                        ds.rm_item(user,cat_id,it["id"])
                        (ds.dl_table if it["type"]=="table" else ds.dl_map)(it["file_id"])
                        st.rerun()

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
#  BUG FIX DÉFINITIF : on ne compare JAMAIS le df retourné par data_editor
#  avec visible_df (calculé depuis session_state au même rerun).
#  On utilise st.session_state[editor_key] (données brutes du widget) pour
#  détecter les vrais changements via hash JSON des valeurs.
# ══════════════════════════════════════════════════════════════════════════════
def render_table():
    ds=get_ds(); item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]

    if st.session_state.table_df is None:
        df=ds.ld_table(fid)
        if df is None or df.empty: st.error("Table introuvable."); go_back(); st.rerun(); return
        st.session_state.table_df=df
        st.session_state.table_serial=_ser(df[[c for c in df.columns if c!="_location_"]])
        st.session_state.undo_stack=[]; st.session_state.redo_stack=[]

    df=st.session_state.table_df
    vis=[c for c in df.columns if c!="_location_"]

    # Header
    cb,ct,_=st.columns([.12,.62,.26])
    with cb:
        if st.button("← Retour",key="tb_back",use_container_width=True):
            ds.sv_table(fid,st.session_state.table_df); go_back(); st.rerun()
    with ct: st.markdown(f'<div class="pt">📊 {item["name"]}</div>',unsafe_allow_html=True)

    # Toolbar
    can_u=bool(st.session_state.undo_stack); can_r=bool(st.session_state.redo_stack)
    cu,cr,cs,_=st.columns([.09,.09,.09,.73])
    with cu:
        if st.button("↩",key="tu",disabled=not can_u,help=f"Annuler ({len(st.session_state.undo_stack)})",use_container_width=True):
            _tundo(ds,fid); st.rerun()
    with cr:
        if st.button("↪",key="tr",disabled=not can_r,help=f"Rétablir ({len(st.session_state.redo_stack)})",use_container_width=True):
            _tredo(ds,fid); st.rerun()
    with cs:
        if st.button("💾",key="ts",help="Sauvegarder",use_container_width=True):
            ds.sv_table(fid,st.session_state.table_df); st.toast("Sauvegardé ✓",icon="✅")

    # Resize
    with st.expander("⚙️ Redimensionner"):
        rr2,rc2,rb2=st.columns([.35,.35,.3])
        with rr2: nr=st.number_input("Lignes",1,5000,len(df),key="rsz_r")
        with rc2: nc=st.number_input("Colonnes",1,500,len(vis),key="rsz_c")
        with rb2:
            st.markdown("<br>",unsafe_allow_html=True)
            if st.button("Appliquer",key="rsz_ok",use_container_width=True):
                _tpush_undo()
                ndf=ds.resize_table(fid,st.session_state.table_df,int(nr),int(nc))
                st.session_state.table_df=ndf
                st.session_state.table_serial=_ser(ndf[[c for c in ndf.columns if c!="_location_"]])
                st.session_state.redo_stack=[]; ds.sv_table(fid,ndf); st.rerun()

    st.markdown("---")

    # ── DATA EDITOR ──────────────────────────────────────────────────────────
    # PRINCIPE : on passe TOUJOURS le df courant dans le widget.
    # Le widget retourne les données modifiées par l'utilisateur.
    # On compare via sérialisation JSON (déterministe, pas de faux positifs).
    visible_df=df[vis].copy().reset_index(drop=True)
    ekey=f"tbl_{fid}"

    edited=st.data_editor(
        visible_df,
        use_container_width=True,
        num_rows="fixed",
        key=ekey,
        column_config={c:st.column_config.TextColumn(c,width="medium") for c in vis},
        hide_index=False,
    )

    new_serial=_ser(edited)
    if new_serial != st.session_state.table_serial:
        # Vraie modification détectée
        _tpush_undo()
        loc=st.session_state.table_df["_location_"].reset_index(drop=True)
        new_df=edited.copy().reset_index(drop=True)
        # Ajuster _location_ si le nombre de lignes a changé
        if len(loc)!=len(new_df):
            loc=loc.reindex(range(len(new_df))).fillna("")
        new_df.insert(0,"_location_",loc.values)
        st.session_state.table_df=new_df
        st.session_state.table_serial=new_serial
        st.session_state.redo_stack=[]
        ds.sv_table(fid,new_df)   # Autosave systématique

    nr2,nc2=visible_df.shape
    st.markdown(f'<div style="font-size:.68rem;color:var(--t3);margin-top:5px;">📐 {nr2} × {nc2} &nbsp;|&nbsp; ☁️ tables/{fid}.parquet &nbsp;|&nbsp; 🔄 Autosave actif</div>',unsafe_allow_html=True)

def _ser(df): 
    try: return df.fillna("").astype(str).to_json(orient="values")
    except: return str(id(df))

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

# ══════════════════════════════════════════════════════════════════════════════
#  §10 — MAP CANVAS
#  Sauvegarde auto : le canvas envoie un postMessage avec les données JSON.
#  Un composant HTML adjacent écoute et écrit dans un Streamlit text_input
#  via un trick : on utilise un iframe qui écrit dans window.parent.__mapdata
#  puis un polling via st_autorefresh. Approche alternative retenue :
#  le canvas encode ses données dans l'URL hash et Streamlit lit via JS.
#
#  APPROCHE RETENUE (robuste) : le canvas est self-contained avec localStorage
#  + un bouton "Sync vers R2" visible. Pour l'autosave réel, on utilise un
#  hidden text_input Streamlit que le canvas remplit via un pont JS.
# ══════════════════════════════════════════════════════════════════════════════

MAX_CHARS=500  # Limite de caractères par rectangle

def _canvas_html(objs_json:str, fid:str, loc:str) -> str:
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:#f0f5ee;font-family:'DM Mono',monospace;overflow:hidden;user-select:none;}}
#tb{{display:flex;align-items:center;gap:4px;padding:6px 10px;
     background:#e6eddf;border-bottom:1.5px solid #c8dbc0;flex-wrap:wrap;}}
.tb{{padding:4px 10px;border-radius:7px;border:1.5px solid #c8dbc0;background:#fff;
    color:#4a6657;font-size:.72rem;cursor:pointer;font-family:inherit;transition:all .15s;white-space:nowrap;}}
.tb:hover{{border-color:#4a7c59;color:#4a7c59;background:#eef5ea;}}
.tb.on{{border-color:#4a7c59;background:#dff0e2;color:#3a6246;font-weight:500;}}
.sep{{width:1px;height:18px;background:#c8dbc0;margin:0 2px;flex-shrink:0;}}
#zd{{font-size:.68rem;color:#7a9e88;min-width:38px;text-align:center;}}
#hint{{font-size:.65rem;color:#7a9e88;margin-left:4px;flex:1;}}
#st{{font-size:.65rem;color:#4a7c59;font-weight:500;}}
#cw{{position:relative;overflow:hidden;width:100%;height:calc(100vh - 48px);
     background:#f8faf6;
     background-image:radial-gradient(circle,#d4e6cc 1px,transparent 1px);
     background-size:24px 24px;}}
#cv{{display:block;}}
#te{{position:absolute;display:none;border:2px solid #4a7c59;
     background:rgba(255,255,255,.97);color:#1e2e22;
     font-family:'DM Mono',monospace;resize:none;outline:none;
     padding:6px;border-radius:6px;overflow:hidden;text-align:center;
     line-height:1.45;box-shadow:0 4px 20px rgba(74,124,89,.2);}}
#char-counter{{position:absolute;display:none;font-size:.58rem;color:#7a9e88;
               pointer-events:none;text-align:center;}}
#ah{{position:absolute;top:6px;right:10px;font-size:.68rem;color:#4a7c59;
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
  <button class="tb" onclick="mapUndo()">↩ Annuler</button>
  <button class="tb" onclick="mapRedo()">↪ Rétablir</button>
  <div class="sep"></div>
  <button class="tb" onclick="toggleWritable()" id="btn_wr">✎ Éditable: OUI</button>
  <div class="sep"></div>
  <button class="tb" onclick="delSel()">🗑 Suppr.</button>
  <span id="hint">Alt+drag: panoramique • Ctrl+Z/Y: undo/redo</span>
  <span id="st"></span>
</div>
<div id="cw">
  <canvas id="cv"></canvas>
  <textarea id="te" maxlength="{MAX_CHARS}"></textarea>
  <div id="char-counter"></div>
  <div id="ah">Source → cliquer la cible</div>
</div>
<script>
const cv=document.getElementById('cv'),ctx=cv.getContext('2d'),
      cw=document.getElementById('cw'),te=document.getElementById('te'),
      cc_el=document.getElementById('char-counter'),
      ah=document.getElementById('ah'),stEl=document.getElementById('st'),
      hintEl=document.getElementById('hint'),btnWr=document.getElementById('btn_wr');
const MAX_CHARS={MAX_CHARS};
const LOC="{loc}"; const FID="{fid}";

let objs={objs_json};
let tool='select',sc=1,ox=60,oy=60;
let selId=null,drag=false,dsx=0,dsy=0,dox=0,doy=0;
let draw=false,ds={{x:0,y:0}},dc={{x:0,y:0}};
let rsz=false,rh=null,rs=null,pan=false,px=0,py=0,pox=0,poy=0;
let eid=null,asrc=null,idc=1;
let undoSt=[],redoSt=[];
let saveTimer=null;
objs.forEach(o=>{{const n=parseInt(String(o.id).replace(/[^0-9]/g,''))||0;if(n>=idc)idc=n+1;}});

function resizeCv(){{cv.width=cw.clientWidth;cv.height=cw.clientHeight;render();}}
window.addEventListener('resize',resizeCv);resizeCv();

function tw(cx,cy){{return{{x:(cx-ox)/sc,y:(cy-oy)/sc}};}}
function gp(e){{const r=cv.getBoundingClientRect();return{{x:e.clientX-r.left,y:e.clientY-r.top}};}}

// Hit testing — rectangles
function hR(o,wx,wy){{return o.type==='rectangle'&&wx>=o.x&&wx<=o.x+o.w&&wy>=o.y&&wy<=o.y+o.h;}}
function hA(o,wx,wy){{
  if(o.type!=='arrow')return false;
  const dx=o.x2-o.x1,dy=o.y2-o.y1,len=Math.sqrt(dx*dx+dy*dy);
  if(len<1)return false;
  const t=((wx-o.x1)*dx+(wy-o.y1)*dy)/(len*len);
  if(t<0||t>1)return false;
  const px2=o.x1+t*dx,py2=o.y1+t*dy;
  return Math.sqrt((wx-px2)**2+(wy-py2)**2)<8/sc;
}}
function hTest(wx,wy){{
  for(let i=objs.length-1;i>=0;i--){{if(hR(objs[i],wx,wy)||hA(objs[i],wx,wy))return objs[i];}}
  return null;
}}
function hTestRect(wx,wy){{for(let i=objs.length-1;i>=0;i--){{if(hR(objs[i],wx,wy))return objs[i];}}return null;}}
function gHandles(o){{
  if(o.type!=='rectangle')return[];
  return[{{id:'se',x:o.x+o.w,y:o.y+o.h}},{{id:'e',x:o.x+o.w,y:o.y+o.h/2}},
         {{id:'s',x:o.x+o.w/2,y:o.y+o.h}},{{id:'n',x:o.x+o.w/2,y:o.y}},
         {{id:'nw',x:o.x,y:o.y}},{{id:'w',x:o.x,y:o.y+o.h/2}}];
}}
function hHandle(o,wx,wy){{const t=8/sc;for(const h of gHandles(o)){{if(Math.abs(wx-h.x)<t&&Math.abs(wy-h.y)<t)return h.id;}}return null;}}

// Min size
function minSize(o){{
  if(!o.label)return{{w:80,h:44}};
  const fs=13; ctx.save();ctx.font=fs+'px DM Mono,monospace';
  const words=o.label.split(' ');let mw=0;
  words.forEach(w=>{{const m=ctx.measureText(w).width;if(m>mw)mw=m;}});
  ctx.restore();
  const minW=Math.max(80,mw+32);
  const linesEst=Math.ceil((o.label.length*7.5)/Math.max(1,minW-24));
  return{{w:minW,h:Math.max(44,linesEst*fs*1.5+24)}};
}}

// Undo/Redo
function pushU(){{undoSt.push(JSON.stringify(objs));if(undoSt.length>50)undoSt.shift();redoSt=[];}}
function mapUndo(){{if(!undoSt.length)return;redoSt.push(JSON.stringify(objs));objs=JSON.parse(undoSt.pop());selId=null;render();scheduleSave();}}
function mapRedo(){{if(!redoSt.length)return;undoSt.push(JSON.stringify(objs));objs=JSON.parse(redoSt.pop());selId=null;render();scheduleSave();}}

// Writable
function toggleWritable(){{
  const o=objs.find(x=>x.id===selId);
  if(!o||o.type!=='rectangle')return;
  o.writable=!(o.writable!==false);updateWBtn();render();scheduleSave();
}}
function updateWBtn(){{
  const o=objs.find(x=>x.id===selId);
  if(!o||o.type!=='rectangle'){{btnWr.textContent='✎ Éditable: —';return;}}
  btnWr.textContent='✎ Éditable: '+(o.writable!==false?'OUI':'NON');
}}

// Render
function render(){{
  ctx.clearRect(0,0,cv.width,cv.height);
  ctx.save();ctx.translate(ox,oy);ctx.scale(sc,sc);
  objs.filter(o=>o.type==='arrow').forEach(drawArrow);
  objs.filter(o=>o.type==='rectangle').forEach(drawRect);
  if(draw&&tool==='rect'){{
    const w=dc.x-ds.x,h=dc.y-ds.y;
    ctx.save();ctx.strokeStyle='#4a7c59';ctx.fillStyle='rgba(74,124,89,.06)';
    ctx.lineWidth=1.5/sc;ctx.setLineDash([5/sc,3/sc]);
    rr(ctx,ds.x,ds.y,w,h,6/sc);ctx.fill();ctx.stroke();ctx.restore();
  }}
  ctx.restore();
  document.getElementById('zd').textContent=Math.round(sc*100)+'%';
}}

function drawRect(o){{
  const sel=o.id===selId;
  ctx.save();
  if(sel){{ctx.shadowColor='rgba(74,124,89,.45)';ctx.shadowBlur=14/sc;}}
  ctx.fillStyle=o.fill||'#ffffff';
  ctx.strokeStyle=sel?'#4a7c59':(o.writable===false?'#b0c4b0':'#c8dbc0');
  ctx.lineWidth=(sel?2.5:1.5)/sc;
  if(o.writable===false){{ctx.setLineDash([5/sc,2/sc]);ctx.strokeStyle='#a0b8a0';}}
  ctx.beginPath();rr(ctx,o.x,o.y,o.w,o.h,10/sc);ctx.fill();ctx.stroke();
  ctx.setLineDash([]);
  // Lock badge
  if(o.writable===false){{
    ctx.save();ctx.fillStyle='rgba(122,158,136,.6)';ctx.font=(9/sc)+'px DM Mono,monospace';
    ctx.textAlign='right';ctx.textBaseline='top';ctx.fillText('🔒',o.x+o.w-5/sc,o.y+5/sc);ctx.restore();
  }}
  // Text
  if(o.label&&o.id!==eid){{
    ctx.save();
    const maxFs=14,minFs=9;
    const fs=Math.max(minFs,Math.min(maxFs,o.w/13));
    ctx.font=fs+'px DM Mono,monospace';
    ctx.fillStyle='#1e2e22';ctx.textAlign='center';ctx.textBaseline='middle';
    const pad=Math.max(12,o.w*.08);
    const lines=wrapText(ctx,o.label,o.w-pad*2),lh=fs*1.5,th=lines.length*lh;
    const sy=o.y+o.h/2-th/2+lh/2;
    ctx.save();ctx.beginPath();ctx.rect(o.x+4/sc,o.y+4/sc,o.w-8/sc,o.h-8/sc);ctx.clip();
    lines.forEach((l,i)=>ctx.fillText(l,o.x+o.w/2,sy+i*lh));
    ctx.restore();ctx.restore();
    // Char counter below shape
    if(sel&&o.id!==eid){{
      const cnt=o.label.length;
      ctx.save();ctx.font=(8/sc)+'px DM Mono,monospace';
      ctx.fillStyle=cnt>=MAX_CHARS?'#c0392b':'rgba(122,158,136,.7)';
      ctx.textAlign='center';ctx.textBaseline='top';
      ctx.fillText(`${{cnt}}/${{MAX_CHARS}}`,o.x+o.w/2,o.y+o.h+4/sc);
      ctx.restore();
    }}
  }}
  // Handles
  if(sel){{
    ctx.save();
    gHandles(o).forEach(h=>{{
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
  const AH=18/sc, AW=6/sc;  // arrowhead size
  // Shorten line to not overlap arrowhead
  const ex=x2-Math.cos(ang)*AH*.7, ey=y2-Math.sin(ang)*AH*.7;
  ctx.save();
  // Line with gradient look
  const grad=ctx.createLinearGradient(x1,y1,x2,y2);
  grad.addColorStop(0,sel?'#7ab88a':'#a8c8b0');
  grad.addColorStop(1,sel?'#4a7c59':'#6a9e7a');
  ctx.strokeStyle=grad;ctx.lineWidth=3/sc;
  ctx.lineCap='round';
  ctx.beginPath();ctx.moveTo(x1,y1);ctx.lineTo(ex,ey);ctx.stroke();
  // Filled arrowhead (chevron style)
  ctx.fillStyle=sel?'#3a6246':'#5a8c6a';
  ctx.shadowColor='rgba(74,124,89,.3)';ctx.shadowBlur=5/sc;
  ctx.beginPath();
  ctx.moveTo(x2,y2);
  ctx.lineTo(x2-AH*Math.cos(ang-0.42),y2-AH*Math.sin(ang-0.42));
  ctx.lineTo(x2-(AH*.55)*Math.cos(ang),y2-(AH*.55)*Math.sin(ang));
  ctx.lineTo(x2-AH*Math.cos(ang+0.42),y2-AH*Math.sin(ang+0.42));
  ctx.closePath();ctx.fill();
  // Selection indicator on arrow
  if(sel){{
    const mx=(x1+x2)/2,my=(y1+y2)/2;
    ctx.fillStyle='rgba(74,124,89,.2)';
    ctx.beginPath();ctx.arc(mx,my,7/sc,0,Math.PI*2);ctx.fill();
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
function wrapText(ctx,text,maxW){{
  if(!text)return[''];
  const words=text.split(' ');const lines=[];let line='';
  for(const w of words){{const t=line?line+' '+w:w;if(ctx.measureText(t).width>maxW&&line){{lines.push(line);line=w;}}else{{line=t;}}}}
  if(line)lines.push(line);return lines.length?lines:[''];
}}

// Editor
function openEditor(o,appendChar){{
  if(o.writable===false)return;
  eid=o.id;
  const cx=o.x*sc+ox,cy=o.y*sc+oy;
  const fw=o.w*sc,fh=o.h*sc;
  te.style.cssText=`display:block;position:absolute;left:${{cx}}px;top:${{cy}}px;width:${{fw}}px;height:${{fh}}px;font-size:${{Math.max(10,Math.min(14,o.w/13))}}px;`;
  te.value=o.label||'';
  te.maxLength=MAX_CHARS;
  if(appendChar!==undefined){{
    te.value=(o.label||'')+appendChar;o.label=te.value;
  }}
  te.focus();const l=te.value.length;te.setSelectionRange(l,l);
  updateCounter();render();
}}
function closeEditor(){{
  if(eid!==null){{
    const o=objs.find(x=>x.id===eid);if(o)o.label=te.value;
    eid=null;te.style.display='none';cc_el.style.display='none';render();scheduleSave();
  }}
}}
function updateCounter(){{
  if(eid===null)return;
  const o=objs.find(x=>x.id===eid);if(!o)return;
  const cx=o.x*sc+ox,cy=o.y*sc+oy,fw=o.w*sc;
  const cnt=te.value.length;
  cc_el.style.cssText=`display:block;left:${{cx}}px;top:${{cy+o.h*sc+4}}px;width:${{fw}}px;color:${{cnt>=MAX_CHARS?'#c0392b':'#7a9e88'}};`;
  cc_el.textContent=`${{cnt}}/${{MAX_CHARS}}`;
}}
te.addEventListener('input',()=>{{
  const o=objs.find(x=>x.id===eid);
  if(o){{o.label=te.value;render();updateCounter();scheduleSave();}}
}});
te.addEventListener('blur',closeEditor);
te.addEventListener('keydown',e=>{{if(e.key==='Escape')closeEditor();e.stopPropagation();}});

// Tool switching
function setTool(t){{
  tool=t;asrc=null;ah.style.display='none';
  ['bs','br','ba'].forEach(id=>document.getElementById(id)?.classList.remove('on'));
  const btn=document.getElementById('b'+t[0]);if(btn)btn.classList.add('on');
  closeEditor();
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};cv.style.cursor=curs[t]||'default';
  const hints={{select:'Clic: sélectionner | Clic sur forme: écrire | Drag: déplacer',
                rect:'Glisser: créer | Clic sur existant: sélectionner',
                arrow:'Clic source → clic cible | Clic sur flèche: sélectionner'}};
  hintEl.textContent=hints[t]||'';
}}

// Mouse
cv.addEventListener('mousedown',e=>{{
  e.preventDefault();const cp=gp(e),wp=tw(cp.x,cp.y);
  if(e.button===1||(e.button===0&&e.altKey)){{pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';return;}}
  if(e.button!==0)return;closeEditor();
  if(tool==='select'){{
    const sel=selId?objs.find(o=>o.id===selId):null;
    if(sel&&sel.type==='rectangle'){{
      const h=hHandle(sel,wp.x,wp.y);
      if(h){{pushU();rsz=true;rh=h;dsx=wp.x;dsy=wp.y;rs={{x:sel.x,y:sel.y,w:sel.w,h:sel.h}};return;}}
    }}
    const hit=hTest(wp.x,wp.y);
    if(hit){{
      if(hit.type==='rectangle'){{
        // Un seul clic → ouvrir éditeur si déjà sélectionné, sinon sélectionner
        if(selId===hit.id){{openEditor(hit);return;}}
        selId=hit.id;updateWBtn();pushU();drag=true;dsx=wp.x;dsy=wp.y;dox=hit.x;doy=hit.y;
      }}else{{selId=hit.id;updateWBtn();}}
    }}else{{selId=null;updateWBtn();pan=true;px=cp.x;py=cp.y;pox=ox;poy=oy;cv.style.cursor='grabbing';}}
    render();
  }}else if(tool==='rect'){{
    const hit=hTest(wp.x,wp.y);
    if(hit){{selId=hit.id;updateWBtn();setTool('select');
      // Si rectangle : ouvrir immédiatement l'éditeur
      if(hit.type==='rectangle')openEditor(hit);
      render();return;}}
    draw=true;ds={{x:wp.x,y:wp.y}};dc={{x:wp.x,y:wp.y}};
  }}else if(tool==='arrow'){{
    const hit=hTest(wp.x,wp.y);
    if(!hit){{asrc=null;ah.style.display='none';return;}}
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
      asrc=null;ah.style.display='none';render();scheduleSave();
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
      const ms=minSize(o),dx=wp.x-dsx,dy=wp.y-dsy;
      if(rh.includes('e'))o.w=Math.max(ms.w,rs.w+dx);
      if(rh.includes('s'))o.h=Math.max(ms.h,rs.h+dy);
      if(rh.includes('w')){{const nw=Math.max(ms.w,rs.w-dx);o.x=rs.x+(rs.w-nw);o.w=nw;}}
      if(rh.includes('n')){{const nh=Math.max(ms.h,rs.h-dy);o.y=rs.y+(rs.h-nh);o.h=nh;}}
      syncArrows(o);render();
    }}
  }}
  if(draw){{dc={{x:wp.x,y:wp.y}};render();}}
}});

cv.addEventListener('mouseup',e=>{{
  const curs={{select:'default',rect:'crosshair',arrow:'crosshair'}};cv.style.cursor=curs[tool]||'default';
  if(pan){{pan=false;return;}}
  if(draw){{
    draw=false;const w=dc.x-ds.x,h=dc.y-ds.y;
    if(Math.abs(w)>20&&Math.abs(h)>16){{
      pushU();
      const o={{id:'r'+(idc++),type:'rectangle',
               x:w>0?ds.x:ds.x+w,y:h>0?ds.y:ds.y+h,
               w:Math.abs(w),h:Math.abs(h),label:'',fill:'#ffffff',writable:true}};
      objs.push(o);selId=o.id;updateWBtn();
      // Passer en mode sélection et ouvrir l'éditeur immédiatement
      setTool('select');
      openEditor(o,'');
    }}render();scheduleSave();
  }}
  if(drag&&selId){{drag=false;scheduleSave();}}
  if(rsz){{rsz=false;scheduleSave();}}
}});

// Frappe directe (objet sélectionné en mode select)
document.addEventListener('keydown',e=>{{
  if(e.target!==document.body)return;
  if(e.key==='Delete'||e.key==='Backspace'){{delSel();return;}}
  if(e.key==='Escape'){{selId=null;updateWBtn();render();return;}}
  if(e.key==='z'&&(e.ctrlKey||e.metaKey)&&!e.shiftKey){{mapUndo();return;}}
  if((e.key==='y'&&(e.ctrlKey||e.metaKey))||(e.key==='z'&&(e.ctrlKey||e.metaKey)&&e.shiftKey)){{mapRedo();return;}}
  if(selId&&e.key.length===1&&!e.ctrlKey&&!e.metaKey){{
    const o=objs.find(x=>x.id===selId);
    if(o&&o.type==='rectangle'&&o.writable!==false){{
      if((o.label||'').length>=MAX_CHARS)return;
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
  selId=null;updateWBtn();render();scheduleSave();
}}
function syncArrows(rect){{
  objs.filter(o=>o.type==='arrow').forEach(a=>{{
    const s=objs.find(x=>x.id===a.srcId),d=objs.find(x=>x.id===a.dstId);
    if(s){{a.x1=s.x+s.w/2;a.y1=s.y+s.h/2;}}
    if(d){{a.x2=d.x+d.w/2;a.y2=d.y+d.h/2;}}
  }});
}}

// ── AUTOSAVE via postMessage vers le parent Streamlit ─────────────────────
function serializeObjs(){{
  return objs.map(o=>{{
    let c={{}};
    if(o.type==='rectangle')c={{x:o.x,y:o.y,w:o.w,h:o.h}};
    else if(o.type==='arrow')c={{x1:o.x1,y1:o.y1,x2:o.x2,y2:o.y2,srcId:o.srcId,dstId:o.dstId}};
    return{{id:o.id,type:o.type,label:o.label||'',coords:JSON.stringify(c),writable:o.writable!==false}};
  }});
}}
function scheduleSave(){{
  clearTimeout(saveTimer);
  saveTimer=setTimeout(()=>{{
    const payload=JSON.stringify({{fid:FID,loc:LOC,objs:serializeObjs()}});
    // Write to a hidden input in the parent document
    try{{
      window.parent.postMessage({{type:'mapAutosave',payload}},'*');
    }}catch(e){{}}
    // Also store in sessionStorage as backup
    try{{sessionStorage.setItem('mapSave_'+FID,payload);}}catch(e){{}}
    stEl.textContent='✓ Sauvé';setTimeout(()=>stEl.textContent='',2000);
  }},800);
}}
render();
</script></body></html>"""

def render_map():
    ds=get_ds(); item=st.session_state.get("current_item")
    if not item: go_back(); st.rerun(); return
    fid=item["file_id"]
    df=ds.ld_map(fid)
    if df is None or df.empty: st.error("Map introuvable."); go_back(); st.rerun(); return

    # Build objs
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

    # Header
    cb,ct=st.columns([.12,.88])
    with cb:
        if st.button("← Retour",key="map_back",use_container_width=True): go_back(); st.rerun()
    with ct: st.markdown(f'<div class="pt">🧠 {item["name"]}</div>',unsafe_allow_html=True)

    # ── Autosave bridge ──────────────────────────────────────────────────────
    # Le canvas envoie les données via postMessage. On les récupère via un
    # st.text_input hidden que du JS parent écrit. C'est le seul moyen fiable.
    # On utilise un composant HTML séparé comme "pont" d'écoute.
    bridge_html=f"""<script>
    window.addEventListener('message',function(e){{
      if(!e.data||e.data.type!=='mapAutosave')return;
      const inp=window.parent.document.querySelector('input[data-map-fid="{fid}"]');
      if(inp){{inp.value=e.data.payload;inp.dispatchEvent(new Event('input',{{bubbles:true}}));}}
    }});
    </script>"""
    components.html(bridge_html,height=0)

    # Hidden text input for receiving autosave data
    saved_payload=st.text_input(
        "map_autosave_data",
        key=f"mapdata_{fid}",
        label_visibility="collapsed",
    )
    # Inject data-map-fid attribute via JS
    st.markdown(f"""<script>
    (function(){{
      const inputs=window.parent.document.querySelectorAll('input');
      inputs.forEach(inp=>{{
        if(inp.getAttribute('data-map-fid')==='{fid}')return;
        const k=inp.getAttribute('data-testid')||inp.id||'';
        if(inp.closest('[data-testid="stTextInput"]')){{
          // Find our specific input by proximity to mapdata key
        }}
      }});
      // Simpler: tag all text inputs in this stTextInput block
      const blocks=window.parent.document.querySelectorAll('[data-testid="stTextInput"]');
      blocks.forEach(b=>{{const inp=b.querySelector('input');if(inp&&!inp.getAttribute('data-map-fid'))inp.setAttribute('data-map-fid','{fid}');}});
    }})();
    </script>""",unsafe_allow_html=True)

    if saved_payload and saved_payload.strip().startswith("{{"):
        try:
            payload=json.loads(saved_payload)
            if payload.get("fid")==fid:
                _map_save_objs(ds,fid,loc,payload["objs"]); 
        except: pass

    # Canvas
    components.html(_canvas_html(objs_json,fid,loc),height=640,scrolling=False)
    st.markdown('<div style="font-size:.68rem;color:var(--t3);margin-top:4px;">💡 <b>Alt+Drag</b>: panoramique &nbsp;|&nbsp; <b>Molette</b>: zoom &nbsp;|&nbsp; <b>Clic</b>: sélectionner/écrire &nbsp;|&nbsp; <b>Frappe</b>: écrire dans la forme &nbsp;|&nbsp; <b>Ctrl+Z/Y</b>: undo/redo &nbsp;|&nbsp; Sauvegarde automatique</div>',unsafe_allow_html=True)

def _map_save_objs(ds,fid,loc,objects):
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
