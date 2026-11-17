import csv,hashlib,io,os,shutil,time
from decimal import Decimal
from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from src.config.settings import ARTIFACTS_DIR,DATABASE_URL,SETTLEMENT_CHUNK_ROWS
from src.models.settlement import Settlement
from src.services.audit import log
from src.services.calculation import emissions
LEASE_SECONDS=30
STREAM_BATCH=2000          # 服务端游标每次往返行数，仅占位内存数MiB
COPY_BUFFER=1<<20          # 组装/拷贝缓冲1MiB
HEADERS=['冻结序号','月份','部门','活动','原量','原单位','换算量','统一单位','冻结因子kg每单位','排放kgCO2e']
# 全局排序键：部门→活动→冻结序号；COLLATE "C" 为字节序，与UTF-8码点序一致，跨块有序且与分块大小无关
STREAM_SQL_RAW='''
SELECT t.seq, t.elem
FROM carbon_runs r
CROSS JOIN LATERAL json_array_elements(r.snapshot -> 'sources') WITH ORDINALITY AS t(elem, seq)
WHERE r.id = %s
ORDER BY t.elem ->> 'department' COLLATE "C", t.elem ->> 'activity' COLLATE "C", t.seq
OFFSET %s
'''
class Fenced(Exception): pass  # 被更新的申请取代，旧执行者立即停止且不得再写
def base_dir(): return os.path.join(ARTIFACTS_DIR,'settlements')
def work_dir(sid,epoch): return os.path.join(base_dir(),f'work-{sid}-e{epoch}')
def final_path(sid,epoch): return os.path.join(base_dir(),f'pkg-{sid}-e{epoch}.csv')
def conninfo(): return DATABASE_URL.replace('postgresql+psycopg://','postgresql://',1)
def chunk_rows(): return max(100,SETTLEMENT_CHUNK_ROWS)
def request_package(db,user,run_id):
 row=db.execute(text("SELECT id,status,approved,json_array_length(snapshot->'sources') AS n FROM carbon_runs WHERE id=:i"),{'i':run_id}).first()
 if not row: raise HTTPException(404,'报告不存在')
 if row.status!='completed' or not row.approved: raise HTTPException(409,'仅已复核完成的报告可申请结算包')
 if not row.n: raise HTTPException(422,'报告快照没有底稿')
 now=time.time();st=db.query(Settlement).filter_by(run_id=run_id).first()
 if st:
  st.epoch+=1;st.status='queued';st.worker='';st.locked_until=0;st.rows_done=0;st.chunks_done=0;st.kg_sum='0';st.total_rows=row.n;st.error='';st.requested_by=user.email;st.updated=now
  # file_path/checksum/total_kg保留：新文件发布前，旧下载入口继续可用
 else:
  st=Settlement(run_id=run_id,status='queued',epoch=1,total_rows=row.n,requested_by=user.email,created=now,updated=now)
  db.add(st)
 log(db,user.email,'settlement.request',f'run={run_id}')
 try: db.commit()
 except IntegrityError:
  db.rollback();raise HTTPException(409,'结算包申请处理中，请勿重复提交')
 return st
def claim(db,worker_id):
 now=time.time()
 ids=[r[0] for r in db.execute(text("SELECT id FROM settlement_packages WHERE status='queued' OR (status='running' AND locked_until<:n) ORDER BY id"),{'n':now})]
 for i in ids:
  res=db.execute(text("UPDATE settlement_packages SET status='running',worker=:w,locked_until=:lu,updated=:u WHERE id=:i AND (status='queued' OR (status='running' AND locked_until<:n))"),{'w':worker_id,'lu':now+LEASE_SECONDS,'u':now,'i':i,'n':now})
  db.commit()
  if res.rowcount==1: return db.get(Settlement,i)
 return None
def _flush(db,st,wdir,seq,data,rows_done,kg):
 # 分块原子落盘：tmp+fsync+rename，随后条件推进断点；epoch不符即被栅栏
 tmp=os.path.join(wdir,f'chunk-{seq:06d}.csv.tmp');final=tmp[:-4]
 with open(tmp,'w',encoding='utf-8',newline='') as f: f.write(data);f.flush();os.fsync(f.fileno())
 os.replace(tmp,final)
 now=time.time()
 res=db.execute(text("UPDATE settlement_packages SET rows_done=:r,chunks_done=:c,kg_sum=:k,locked_until=:lu,updated=:u WHERE id=:i AND epoch=:e"),{'r':rows_done,'c':seq+1,'k':format(kg,'f'),'lu':now+LEASE_SECONDS,'u':now,'i':st.id,'e':st.epoch})
 db.commit()
 if res.rowcount!=1: raise Fenced()
 return seq+1
def _assemble(sid,epoch,wdir,chunks):
 final=final_path(sid,epoch);tmp=final+'.tmp';h=hashlib.sha256();size=0
 with open(tmp,'wb') as out:
  for i in range(chunks):
   with open(os.path.join(wdir,f'chunk-{i:06d}.csv'),'rb') as f:
    while True:
     b=f.read(COPY_BUFFER)
     if not b: break
     out.write(b);h.update(b);size+=len(b)
  out.flush();os.fsync(out.fileno())
 os.replace(tmp,final)  # 同目录原子替换
 dfd=os.open(base_dir(),os.O_DIRECTORY);os.fsync(dfd);os.close(dfd)
 return final,h.hexdigest(),size
def _cleanup(st,epoch):
 shutil.rmtree(work_dir(st.id,epoch),ignore_errors=True)
 if not os.path.isdir(base_dir()): return
 for name in os.listdir(base_dir()):
  if name.startswith(f'pkg-{st.id}-e') and name!=f'pkg-{st.id}-e{epoch}.csv':
   try: os.unlink(os.path.join(base_dir(),name))
   except OSError: pass
  if name.startswith(f'work-{st.id}-e') and name!=f'work-{st.id}-e{epoch}':
   shutil.rmtree(os.path.join(base_dir(),name),ignore_errors=True)
def generate(db,st,heartbeat=None):
 import psycopg
 run_id,epoch=st.run_id,st.epoch
 policy=db.execute(text("SELECT snapshot->'policy' FROM carbon_runs WHERE id=:i"),{'i':run_id}).scalar()
 if not policy or not isinstance(policy.get('factors'),dict): raise ValueError('报告快照缺少冻结口径')
 factors=policy['factors']
 wdir=work_dir(st.id,epoch);os.makedirs(wdir,exist_ok=True)
 rows_done=st.rows_done;seq=st.chunks_done;kg=Decimal(st.kg_sum or '0');limit=chunk_rows()
 buf=io.StringIO();w=csv.writer(buf)
 if rows_done==0: buf.write('\ufeff');w.writerow(HEADERS)
 count=0;conn=psycopg.connect(conninfo())
 try:
  cur=conn.cursor(name=f'settlement-{st.id}-{epoch}-{int(time.time()*1000)}');cur.itersize=STREAM_BATCH
  cur.execute(STREAM_SQL_RAW,(run_id,rows_done))
  for ord_,elem in cur:
   qty,row_kg=emissions(elem['activity'],elem['quantity'],elem['unit'],factors);kg+=row_kg
   f=factors[elem['activity']]
   w.writerow([ord_,elem['period'],elem['department'],elem['activity'],str(elem['quantity']),elem['unit'],format(qty,'f'),f['unit'],str(f['kg_per_unit']),format(row_kg,'f')])
   rows_done+=1;count+=1
   if count>=limit:
    seq=_flush(db,st,wdir,seq,buf.getvalue(),rows_done,kg);buf=io.StringIO();w=csv.writer(buf);count=0
    if heartbeat: heartbeat()
 finally:
  conn.close()
 if count: seq=_flush(db,st,wdir,seq,buf.getvalue(),rows_done,kg)
 if rows_done!=st.total_rows: raise ValueError(f'快照行数变动：期望{st.total_rows}实得{rows_done}')
 final,digest,size=_assemble(st.id,epoch,wdir,seq)
 now=time.time()
 res=db.execute(text("UPDATE settlement_packages SET status='completed',file_path=:p,checksum=:h,size_bytes=:s,total_kg=:t,finished=:f,updated=:u,locked_until=0 WHERE id=:i AND epoch=:e"),{'p':final,'h':digest,'s':size,'t':format(kg,'f'),'f':now,'u':now,'i':st.id,'e':epoch})
 if res.rowcount!=1:  # 旧执行者晚到：新成果已就位，删除自己的未引用文件，绝不覆盖
  db.rollback()
  try: os.unlink(final)
  except OSError: pass
  raise Fenced()
 log(db,'worker','settlement.completed',f'id={st.id} rows={rows_done}')
 db.commit()
 _cleanup(st,epoch)
 return st
def execute(db,st,heartbeat=None):
 import traceback
 try: generate(db,st,heartbeat)
 except Fenced:
  log(db,'worker','settlement.fenced',f'id={st.id} epoch={st.epoch}');db.commit()
 except Exception as exc:
  traceback.print_exc()
  db.execute(text("UPDATE settlement_packages SET status='failed',error=:e,updated=:u,locked_until=0 WHERE id=:i AND epoch=:ep"),{'e':str(exc)[:1500],'u':time.time(),'i':st.id,'ep':st.epoch})
  log(db,'worker','settlement.failed',f'id={st.id}');db.commit()
