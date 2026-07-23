from fastapi import FastAPI
from api.app.routers.search import router as search_router


app = FastAPI(
    title="University Data Platform API"
)


app.include_router(search_router)


@app.get("/")
def home():
    return {
        "message": "University Data Platform API is running"
    }