from fastapi import FastAPI
from app.api.v1 import router as api_v1_router

app = FastAPI(title="HAWK Hedge Orchestration API")
app.include_router(api_v1_router, prefix="/api/v1")

@app.get("/")
def healthcheck():
    return {"status": "ok", "message": "HAWK API is running"}
