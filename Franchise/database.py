from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# SQLALCHAMY_DATABASE_URL = 'postgresql://apiuser:Api_Ibl_123_456@192.168.130.81:5433/DATAWAREHOUSE'
SQLALCHEMY_DATABASE_URL = 'postgresql://franchise:franchisePassword!123!456@35.216.155.219:5432/franchise_db'
# SQLALCHAMY_DATABASE_URL = 'postgresql://postgres:kamil0343@localhost:5432/franchise_portal'

engine = create_engine(SQLALCHEMY_DATABASE_URL,  connect_args={
                       'options': '-csearch_path={}'.format('franchise')}, pool_pre_ping=True
                       
)
#  pool_pre_ping=True

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
