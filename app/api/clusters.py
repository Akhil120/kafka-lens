from fastapi import APIRouter, HTTPException
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
    )


@router.put("/{cluster_id}", response_model=ClusterSummary)
def update_cluster(cluster_id: str, cluster: ClusterConfig):
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
