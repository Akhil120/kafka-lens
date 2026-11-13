from fastapi import APIRouter, Body, HTTPException
from app.models import ClusterConfig, ClusterSummary, TestConnectionResponse
from app import store
from app.kafka.consumer import test_connection, invalidate_admin

router = APIRouter(prefix="/api/clusters", tags=["clusters"])


@router.get("", response_model=list[ClusterSummary])
def list_clusters():
    clusters = store.list_clusters()
    return [
        ClusterSummary(
            id=c.id,
            name=c.name,
            bootstrap_servers=c.bootstrap_servers,
            security_protocol=c.security_protocol,
            sasl_mechanism=c.sasl_mechanism,
            schema_registry_url=c.schema_registry_url,
            tls_skip_verify=c.tls_skip_verify,
        )
        for c in clusters
    ]


@router.post("", response_model=ClusterSummary, status_code=201)
def create_cluster(cluster: ClusterConfig):
    store.save_cluster(cluster)
    return ClusterSummary(
        id=cluster.id,
        name=cluster.name,
        bootstrap_servers=cluster.bootstrap_servers,
        security_protocol=cluster.security_protocol,
        sasl_mechanism=cluster.sasl_mechanism,
        schema_registry_url=cluster.schema_registry_url,
        tls_skip_verify=cluster.tls_skip_verify,
    )


@router.put("/{cluster_id}", response_model=ClusterSummary)
def update_cluster(cluster_id: str, data: dict = Body(...)):
    existing = store.get_cluster(cluster_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Cluster not found")
    # Merge stored secrets so the edit form doesn't need to re-enter passwords
    if not data.get("password"):
        data["password"] = existing.password
    if not data.get("schema_registry_password"):
        data["schema_registry_password"] = existing.schema_registry_password
    if data.get("ssl") and not data["ssl"].get("key_password") and existing.ssl:
        data["ssl"]["key_password"] = existing.ssl.key_password
    try:
        cluster = ClusterConfig.model_validate(data)
    except Exception as e:
        raise HTTPException(status_code=422, detail=str(e))
    cluster.id = cluster_id
    invalidate_admin(cluster_id)
    store.save_cluster(cluster)
    return ClusterSummary(
        id=cluster.id,
        name=cluster.name,
        bootstrap_servers=cluster.bootstrap_servers,
        security_protocol=cluster.security_protocol,
        sasl_mechanism=cluster.sasl_mechanism,
        schema_registry_url=cluster.schema_registry_url,
        tls_skip_verify=cluster.tls_skip_verify,
    )


@router.delete("/{cluster_id}", status_code=204)
def delete_cluster(cluster_id: str):
    if not store.delete_cluster(cluster_id):
        raise HTTPException(status_code=404, detail="Cluster not found")
    invalidate_admin(cluster_id)


@router.post("/{cluster_id}/test", response_model=TestConnectionResponse)
def test_cluster(cluster_id: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        brokers = test_connection(cluster)
        return TestConnectionResponse(ok=True, brokers=brokers)
    except Exception as e:
        return TestConnectionResponse(ok=False, error=str(e))
