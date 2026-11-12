import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from app import store
from app.models import SearchRequest, SearchTaskResponse
from app.search.engine import create_task, cancel_task, stream_results, _tasks

router = APIRouter(prefix="/api", tags=["search"])


@router.post(
    "/clusters/{cluster_id}/topics/{topic}/search",
    response_model=SearchTaskResponse,
)
def start_search(cluster_id: str, topic: str, request: SearchRequest):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    task_id = create_task(cluster, topic, request)
    return SearchTaskResponse(task_id=task_id)


@router.get("/search/{task_id}/stream")
async def stream_search(task_id: str):
    if task_id not in _tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    return StreamingResponse(
        stream_results(task_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.delete("/search/{task_id}", status_code=204)
def cancel_search(task_id: str):
    if not cancel_task(task_id):
        raise HTTPException(status_code=404, detail="Task not found")
