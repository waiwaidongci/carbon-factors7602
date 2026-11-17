import os,socket,time,json,traceback
from pathlib import Path
import redis
from src.config.database import create_all,Session
from src.config.settings import REDIS_URL
from src.models import identity,source,run,settlement
from src.models.run import Run
from src.services.calculation import calculate
from src.services.delivery import tick
from src.services.settlement import claim,execute
create_all()
r=redis.Redis.from_url(REDIS_URL)
WORKER_ID=socket.gethostname()+'-'+str(os.getpid())
def heartbeat(): Path('/tmp/worker-heartbeat').write_text(str(time.time()))
while True:
 heartbeat()
 try:
  item=r.lpop('report-jobs')
  if item:
   with Session() as db:
    job=db.get(Run,int(item))
    if job and job.status=='queued':
     job.status='running';db.commit()
     try: job.result=calculate(job.snapshot['sources'],job.snapshot['policy']);job.status='completed'
     except Exception as exc: job.status='failed';job.error=str(exc);traceback.print_exc()
     db.commit()
  with Session() as db: tick(db)
  with Session() as db:
   task=claim(db,WORKER_ID)
   if task: execute(db,task,heartbeat)
 except Exception: traceback.print_exc()
 time.sleep(1)
