from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pathlib import Path

from app.api import clusters as clusters_router
from app.api import topics as topics_router
from app.api import search as search_router
from app import store
from app.models import ClusterSummary

_HERE = Path(__file__).parent  # app/ directory

app = FastAPI(title="KafkaLens", version="1.0.0", docs_url="/api/docs")

app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
templates = Jinja2Templates(directory=str(_HERE / "templates"))

app.include_router(clusters_router.router)
app.include_router(topics_router.router)
app.include_router(search_router.router)


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    clusters = store.list_clusters()
    cluster_summaries = [
        {
            "id": c.id,
            "name": c.name,
            "bootstrap_servers": c.bootstrap_servers,
            "security_protocol": c.security_protocol,
            "sasl_mechanism": c.sasl_mechanism,
            "schema_registry_url": c.schema_registry_url,
        }
        for c in clusters
    ]
    return templates.TemplateResponse(
        request,
        "index.html",
        {"clusters": cluster_summaries},
    )
