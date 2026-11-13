from fastapi import APIRouter, Body, HTTPException
from app.models import ClusterConfig, ClusterSummary, TestConnectionResponse
from app import store
from app.kafka.consumer import test_connection, invalidate_admin, _consumer_pool

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
            username=c.username,
            schema_registry_url=c.schema_registry_url,
            schema_registry_username=c.schema_registry_username,
            tls_skip_verify=c.tls_skip_verify,
            consumer_group_id=c.consumer_group_id,
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
        username=cluster.username,
        schema_registry_url=cluster.schema_registry_url,
        schema_registry_username=cluster.schema_registry_username,
        tls_skip_verify=cluster.tls_skip_verify,
        consumer_group_id=cluster.consumer_group_id,
    )


@router.put("/{cluster_id}", response_model=ClusterSummary)
def update_cluster(cluster_id: str, data: dict = Body(...)):
    existing = store.get_cluster(cluster_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Cluster not found")
    # Merge stored credentials/secrets — blank fields in the edit form mean "keep existing"
    if not data.get("username"):
        data["username"] = existing.username
    if not data.get("password"):
        data["password"] = existing.password
    if not data.get("schema_registry_username"):
        data["schema_registry_username"] = existing.schema_registry_username
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
    _consumer_pool.invalidate(cluster_id)
    store.save_cluster(cluster)
    return ClusterSummary(
        id=cluster.id,
        name=cluster.name,
        bootstrap_servers=cluster.bootstrap_servers,
        security_protocol=cluster.security_protocol,
        sasl_mechanism=cluster.sasl_mechanism,
        username=cluster.username,
        schema_registry_url=cluster.schema_registry_url,
        schema_registry_username=cluster.schema_registry_username,
        tls_skip_verify=cluster.tls_skip_verify,
        consumer_group_id=cluster.consumer_group_id,
    )


@router.delete("/{cluster_id}", status_code=204)
def delete_cluster(cluster_id: str):
    if not store.delete_cluster(cluster_id):
        raise HTTPException(status_code=404, detail="Cluster not found")
    invalidate_admin(cluster_id)
    _consumer_pool.invalidate(cluster_id)


@router.post("/{cluster_id}/test", response_model=TestConnectionResponse)
def test_cluster(cluster_id: str):
    cluster = store.get_cluster(cluster_id)
    if not cluster:
        raise HTTPException(status_code=404, detail="Cluster not found")
    try:
        brokers, hosts_entries = test_connection(cluster)
        return TestConnectionResponse(ok=True, brokers=brokers, hosts_entries=hosts_entries)
    except Exception as e:
        return TestConnectionResponse(ok=False, error=str(e))
