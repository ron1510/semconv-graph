"""Generated semantic edge interfaces."""

from types import MappingProxyType
from typing import ClassVar

from extended_otel_semconv.edges import SemanticEdge

class ContainerRuntimeRunsK8sContainerEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.container_runtime_runs_k8s_container"
    relationship_type: ClassVar[str] = "runs"
    source_entity_type: ClassVar[str] = "container.runtime"
    target_entity_type: ClassVar[str] = "k8s.container"


class EtlPipelineContainsEtlRunEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.etl_pipeline_contains_run"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "etl.pipeline"
    target_entity_type: ClassVar[str] = "etl.run"


class EtlRunContainsEtlPartRunEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.etl_run_contains_part_run"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "etl.run"
    target_entity_type: ClassVar[str] = "etl.part.run"


class K8sClusterContainsK8sNamespaceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_cluster_contains_namespace"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.cluster"
    target_entity_type: ClassVar[str] = "k8s.namespace"


class K8sClusterContainsK8sNodeEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_cluster_contains_node"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.cluster"
    target_entity_type: ClassVar[str] = "k8s.node"


class K8sClusterContainsK8sPersistentvolumeEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_cluster_contains_persistentvolume"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.cluster"
    target_entity_type: ClassVar[str] = "k8s.persistentvolume"


class K8sContainerRunsProcessEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_container_runs_process"
    relationship_type: ClassVar[str] = "runs"
    source_entity_type: ClassVar[str] = "k8s.container"
    target_entity_type: ClassVar[str] = "process"


class K8sNamespaceContainsK8sCronjobEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_cronjob"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.cronjob"


class K8sNamespaceContainsK8sDaemonsetEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_daemonset"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.daemonset"


class K8sNamespaceContainsK8sDeploymentEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_deployment"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.deployment"


class K8sNamespaceContainsK8sHpaEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_hpa"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.hpa"


class K8sNamespaceContainsK8sJobEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_job"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.job"


class K8sNamespaceContainsK8sPersistentvolumeclaimEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_persistentvolumeclaim"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.persistentvolumeclaim"


class K8sNamespaceContainsK8sPodEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_pod"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.pod"


class K8sNamespaceContainsK8sReplicasetEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_replicaset"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.replicaset"


class K8sNamespaceContainsK8sReplicationcontrollerEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_replicationcontroller"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.replicationcontroller"


class K8sNamespaceContainsK8sResourcequotaEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_resourcequota"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.resourcequota"


class K8sNamespaceContainsK8sServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_service"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.service"


class K8sNamespaceContainsK8sStatefulsetEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_namespace_contains_statefulset"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.namespace"
    target_entity_type: ClassVar[str] = "k8s.statefulset"


class K8sNodeRunsK8sPodEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_node_runs_pod"
    relationship_type: ClassVar[str] = "runs"
    source_entity_type: ClassVar[str] = "k8s.node"
    target_entity_type: ClassVar[str] = "k8s.pod"


class K8sPodContainsK8sContainerEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_pod_contains_container"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "k8s.pod"
    target_entity_type: ClassVar[str] = "k8s.container"


class K8sPodRunsServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_pod_runs_service"
    relationship_type: ClassVar[str] = "runs"
    source_entity_type: ClassVar[str] = "k8s.pod"
    target_entity_type: ClassVar[str] = "service"


class K8sPodRunsServiceInstanceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.k8s_pod_runs_service_instance"
    relationship_type: ClassVar[str] = "runs"
    source_entity_type: ClassVar[str] = "k8s.pod"
    target_entity_type: ClassVar[str] = "service.instance"


class ProcessUsesProcessExecutableEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.process_uses_executable"
    relationship_type: ClassVar[str] = "uses"
    source_entity_type: ClassVar[str] = "process"
    target_entity_type: ClassVar[str] = "process.executable"


class ProcessUsesProcessRuntimeEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.process_uses_runtime"
    relationship_type: ClassVar[str] = "uses"
    source_entity_type: ClassVar[str] = "process"
    target_entity_type: ClassVar[str] = "process.runtime"


class ServiceBuiltFromVcsRepositoryEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_built_from_vcs_repository"
    relationship_type: ClassVar[str] = "built_from"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "vcs.repository"


class ServiceCallsServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_calls_service"
    relationship_type: ClassVar[str] = "calls"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "service"


class ServiceContainsServiceInstanceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_contains_service_instance"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "service.instance"


class ServiceExposesAppEndpointEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_exposes_app_endpoint"
    relationship_type: ClassVar[str] = "exposes"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "app.endpoint"


class ServiceInstrumentedByTelemetryDistroEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_instrumented_by_telemetry_distro"
    relationship_type: ClassVar[str] = "instrumented_by"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "telemetry.distro"


class ServiceInstrumentedByTelemetrySdkEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_instrumented_by_telemetry_sdk"
    relationship_type: ClassVar[str] = "instrumented_by"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "telemetry.sdk"


class ServiceNamespaceContainsServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_namespace_contains_service"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "service.namespace"
    target_entity_type: ClassVar[str] = "service"


class ServicePublishesToServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_publishes_to_service"
    relationship_type: ClassVar[str] = "publishes_to"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "service"


class ServiceQueriesServiceEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.service_queries_service"
    relationship_type: ClassVar[str] = "queries"
    source_entity_type: ClassVar[str] = "service"
    target_entity_type: ClassVar[str] = "service"


class VcsRepositoryContainsVcsRefEdge(SemanticEdge):
    relationship_id: ClassVar[str] = "relationship.vcs_repository_contains_ref"
    relationship_type: ClassVar[str] = "contains"
    source_entity_type: ClassVar[str] = "vcs.repository"
    target_entity_type: ClassVar[str] = "vcs.ref"


EDGE_MODELS = MappingProxyType({
    ("container.runtime", "runs", "k8s.container"): ContainerRuntimeRunsK8sContainerEdge,
    ("etl.pipeline", "contains", "etl.run"): EtlPipelineContainsEtlRunEdge,
    ("etl.run", "contains", "etl.part.run"): EtlRunContainsEtlPartRunEdge,
    ("k8s.cluster", "contains", "k8s.namespace"): K8sClusterContainsK8sNamespaceEdge,
    ("k8s.cluster", "contains", "k8s.node"): K8sClusterContainsK8sNodeEdge,
    ("k8s.cluster", "contains", "k8s.persistentvolume"): K8sClusterContainsK8sPersistentvolumeEdge,
    ("k8s.container", "runs", "process"): K8sContainerRunsProcessEdge,
    ("k8s.namespace", "contains", "k8s.cronjob"): K8sNamespaceContainsK8sCronjobEdge,
    ("k8s.namespace", "contains", "k8s.daemonset"): K8sNamespaceContainsK8sDaemonsetEdge,
    ("k8s.namespace", "contains", "k8s.deployment"): K8sNamespaceContainsK8sDeploymentEdge,
    ("k8s.namespace", "contains", "k8s.hpa"): K8sNamespaceContainsK8sHpaEdge,
    ("k8s.namespace", "contains", "k8s.job"): K8sNamespaceContainsK8sJobEdge,
    ("k8s.namespace", "contains", "k8s.persistentvolumeclaim"): K8sNamespaceContainsK8sPersistentvolumeclaimEdge,
    ("k8s.namespace", "contains", "k8s.pod"): K8sNamespaceContainsK8sPodEdge,
    ("k8s.namespace", "contains", "k8s.replicaset"): K8sNamespaceContainsK8sReplicasetEdge,
    ("k8s.namespace", "contains", "k8s.replicationcontroller"): K8sNamespaceContainsK8sReplicationcontrollerEdge,
    ("k8s.namespace", "contains", "k8s.resourcequota"): K8sNamespaceContainsK8sResourcequotaEdge,
    ("k8s.namespace", "contains", "k8s.service"): K8sNamespaceContainsK8sServiceEdge,
    ("k8s.namespace", "contains", "k8s.statefulset"): K8sNamespaceContainsK8sStatefulsetEdge,
    ("k8s.node", "runs", "k8s.pod"): K8sNodeRunsK8sPodEdge,
    ("k8s.pod", "contains", "k8s.container"): K8sPodContainsK8sContainerEdge,
    ("k8s.pod", "runs", "service"): K8sPodRunsServiceEdge,
    ("k8s.pod", "runs", "service.instance"): K8sPodRunsServiceInstanceEdge,
    ("process", "uses", "process.executable"): ProcessUsesProcessExecutableEdge,
    ("process", "uses", "process.runtime"): ProcessUsesProcessRuntimeEdge,
    ("service", "built_from", "vcs.repository"): ServiceBuiltFromVcsRepositoryEdge,
    ("service", "calls", "service"): ServiceCallsServiceEdge,
    ("service", "contains", "service.instance"): ServiceContainsServiceInstanceEdge,
    ("service", "exposes", "app.endpoint"): ServiceExposesAppEndpointEdge,
    ("service", "instrumented_by", "telemetry.distro"): ServiceInstrumentedByTelemetryDistroEdge,
    ("service", "instrumented_by", "telemetry.sdk"): ServiceInstrumentedByTelemetrySdkEdge,
    ("service.namespace", "contains", "service"): ServiceNamespaceContainsServiceEdge,
    ("service", "publishes_to", "service"): ServicePublishesToServiceEdge,
    ("service", "queries", "service"): ServiceQueriesServiceEdge,
    ("vcs.repository", "contains", "vcs.ref"): VcsRepositoryContainsVcsRefEdge,
})


__all__ = [
    "ContainerRuntimeRunsK8sContainerEdge",
    "EtlPipelineContainsEtlRunEdge",
    "EtlRunContainsEtlPartRunEdge",
    "K8sClusterContainsK8sNamespaceEdge",
    "K8sClusterContainsK8sNodeEdge",
    "K8sClusterContainsK8sPersistentvolumeEdge",
    "K8sContainerRunsProcessEdge",
    "K8sNamespaceContainsK8sCronjobEdge",
    "K8sNamespaceContainsK8sDaemonsetEdge",
    "K8sNamespaceContainsK8sDeploymentEdge",
    "K8sNamespaceContainsK8sHpaEdge",
    "K8sNamespaceContainsK8sJobEdge",
    "K8sNamespaceContainsK8sPersistentvolumeclaimEdge",
    "K8sNamespaceContainsK8sPodEdge",
    "K8sNamespaceContainsK8sReplicasetEdge",
    "K8sNamespaceContainsK8sReplicationcontrollerEdge",
    "K8sNamespaceContainsK8sResourcequotaEdge",
    "K8sNamespaceContainsK8sServiceEdge",
    "K8sNamespaceContainsK8sStatefulsetEdge",
    "K8sNodeRunsK8sPodEdge",
    "K8sPodContainsK8sContainerEdge",
    "K8sPodRunsServiceEdge",
    "K8sPodRunsServiceInstanceEdge",
    "ProcessUsesProcessExecutableEdge",
    "ProcessUsesProcessRuntimeEdge",
    "ServiceBuiltFromVcsRepositoryEdge",
    "ServiceCallsServiceEdge",
    "ServiceContainsServiceInstanceEdge",
    "ServiceExposesAppEndpointEdge",
    "ServiceInstrumentedByTelemetryDistroEdge",
    "ServiceInstrumentedByTelemetrySdkEdge",
    "ServiceNamespaceContainsServiceEdge",
    "ServicePublishesToServiceEdge",
    "ServiceQueriesServiceEdge",
    "VcsRepositoryContainsVcsRefEdge",
    "SemanticEdge",
]
