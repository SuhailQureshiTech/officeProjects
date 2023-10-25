from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from franchise import models
from franchise.database import engine
from franchise.routers import user, distributors
import uvicorn

app = FastAPI(title="Franchise Portal")
app = FastAPI(debug=False)
origins = [
    'http://10.172.0.3:9001',
    'http://35.216.155.219:9001',
    'http://localhost:8000',
    'http://localhost:3001',
    'http://franchise.prosoulsinc.com',
    'https://franchise.prosoulsinc.com',
    'http://franchise.iblgrp.com',
    'https://franchise.iblgrp.com',
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*']
)

models.Base.metadata.create_all(engine)

app.include_router(user.router)
app.include_router(distributors.router)

# if __name__=="__main__":
#    uvicorn.run(app,host="10.172.0.3",port=9001)