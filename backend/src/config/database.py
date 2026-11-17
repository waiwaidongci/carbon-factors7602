from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,DeclarativeBase
from src.config.settings import DATABASE_URL
class Base(DeclarativeBase): pass
engine=create_engine(DATABASE_URL,pool_pre_ping=True)
Session=sessionmaker(bind=engine,expire_on_commit=False)
def database():
 with Session() as db: yield db
def create_all():
 import time
 for attempt in range(10):
  try: Base.metadata.create_all(engine);return
  except Exception:
   if attempt==9: raise
   time.sleep(1+attempt)  # 多进程首启并发建表时退避重试
