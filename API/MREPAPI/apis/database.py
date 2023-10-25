from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

SQLALCHAMY_DATABASE_URL = 'postgresql://postgres:kamil0343@192.168.130.51:5432/focall'

engine = create_engine(SQLALCHAMY_DATABASE_URL, connect_args={'options': '-csearch_path={}'.format('public')})

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
