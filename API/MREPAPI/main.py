from fastai import FastAPI
from fastai.middleware.cors import CORSMiddleware
from apis import models
from apis.database import engine
from apis.routers import user, master_data, activity
# log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.config')

# logging.config.fileConfig(log_file_path)

# logger = logging.getLogger(__name__)

app = FastAPI(title="Activity Portal")

origins = [
    'http://localhost:3000',
    'http://focall.iblgrp.com'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

models.Base.metadata.create_all(engine)

app.include_router(master_data.router)
app.include_router(user.router)
app.include_router(activity.router)
