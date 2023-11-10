import pandas as pd
import os
from sqlalchemy import create_engine
import sqlalchemy

host = '34.65.6.130'
db = 'test_db'
user = 'practicesql'
password = 'practicesql'
schema = 'practiceSql'
connect_string =f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
engine = sqlalchemy.create_engine(connect_string)

print(engine)