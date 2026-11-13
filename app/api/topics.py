import logging
from fastapi import APIRouter, HTTPException
from app import store
from app.kafka.consumer import list_topics, get_topic_info
from app.models import TopicInfo
from app.schema.registry import SchemaRegistryClient

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/clusters", tags=["topics"])


@router.get("/{cluster_id}/topics")
def get_topics(cluster_id: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        topics = list_topics(cluster)
        log.info("list_topics %s → %d topics", cluster.name, len(topics))
        return topics
    except Exception as e:
        log.error("list_topics %s failed: %s", cluster.name, e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{cluster_id}/topics/{topic}/info", response_model=TopicInfo)
def get_topic_detail(cluster_id: str, topic: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        info = get_topic_info(cluster, topic)
        if cluster.schema_registry_url:
            try:
                reg = SchemaRegistryClient(
                    cluster.schema_registry_url,
                    cluster.schema_registry_username,
                    cluster.schema_registry_password,
                )
                subjects = reg.list_subjects()
                for suffix in ["-value", "-key", ""]:
                    if f"{topic}{suffix}" in subjects:
                        info.schema_subject = f"{topic}{suffix}"
                        break
            except Exception:
                pass
        return info
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{cluster_id}/schema-registry/subjects")
def list_subjects(cluster_id: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if not cluster.schema_registry_url:
        raise HTTPException(status_code=400, detail="No schema registry configured")
    try:
        reg = SchemaRegistryClient(
            cluster.schema_registry_url,
            cluster.schema_registry_username,
            cluster.schema_registry_password,
        )
        return reg.list_subjects()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{cluster_id}/schema-registry/{subject}")
def get_subject_schema(cluster_id: str, subject: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    if not cluster.schema_registry_url:
        raise HTTPException(status_code=400, detail="No schema registry configured")
    try:
        reg = SchemaRegistryClient(
            cluster.schema_registry_url,
            cluster.schema_registry_username,
            cluster.schema_registry_password,
        )
        return reg.get_latest_schema(subject)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
