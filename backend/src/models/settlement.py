from sqlalchemy import Column,Integer,String,Float,Text
from src.config.database import Base
class Settlement(Base):
 __tablename__='settlement_packages'
 id=Column(Integer,primary_key=True)
 run_id=Column(Integer,unique=True)          # 一份报告至多一个结算包
 status=Column(String(20))                   # queued/running/completed/failed
 epoch=Column(Integer)                       # 栅栏令牌：每次申请+1，旧执行者凭旧epoch写入会被拒绝
 worker=Column(String(80),default='')        # 当前租约持有者
 locked_until=Column(Float,default=0)        # 租约到期时间，过期可被其他执行者接管
 rows_done=Column(Integer,default=0)         # 已落盘底稿行数（断点）
 chunks_done=Column(Integer,default=0)       # 已落盘分块数
 kg_sum=Column(String(80),default='0')       # 已落盘行的Decimal累计排放，续跑不累重
 total_rows=Column(Integer,default=0)
 total_kg=Column(String(80),default='')      # 发布后：排放合计（与报告一致的Decimal文本）
 file_path=Column(String(250),default='')    # 发布后：下载入口指向的文件，仅原子切换
 checksum=Column(String(64),default='')
 size_bytes=Column(Integer,default=0)
 error=Column(Text,default='')
 requested_by=Column(String(120),default='')
 created=Column(Float); updated=Column(Float); finished=Column(Float,default=None)
