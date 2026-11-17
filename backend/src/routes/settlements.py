import os
from fastapi import APIRouter,Depends,HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import select
from src.config.database import database
from src.config.settings import ARTIFACTS_DIR
from src.controllers.auth import current,editor
from src.models.run import Run
from src.models.settlement import Settlement
from src.services.settlement import request_package
from src.utils.serialize import row
router=APIRouter()
@router.get('/settlements')
def settlements(db=Depends(database),u=Depends(current)):
 rows=db.execute(select(Settlement,Run.name).join(Run,Run.id==Settlement.run_id).order_by(Settlement.id.desc())).all()
 return [{**row(s),'run_name':name} for s,name in rows]
@router.post('/runs/{id}/settlement')
def create(id:int,db=Depends(database),u=Depends(editor)): return row(request_package(db,u,id))
@router.get('/runs/{id}/settlement')
def by_run(id:int,db=Depends(database),u=Depends(current)):
 obj=db.query(Settlement).filter_by(run_id=id).first()
 return row(obj) if obj else None
@router.get('/settlements/{id}/csv')
def download(id:int,db=Depends(database),u=Depends(current)):
 obj=db.get(Settlement,id)
 if not obj or not obj.file_path: raise HTTPException(409,'结算包尚未生成完成')
 root=os.path.realpath(ARTIFACTS_DIR);path=os.path.realpath(obj.file_path)
 if not path.startswith(root+os.sep) or not os.path.isfile(path): raise HTTPException(409,'结算包文件缺失，请重新申请')
 return FileResponse(path,media_type='text/csv',filename=f'settlement-{id}.csv',headers={'X-Checksum-Sha256':obj.checksum})
