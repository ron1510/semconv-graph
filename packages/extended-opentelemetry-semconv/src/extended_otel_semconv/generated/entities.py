"""Generated public semantic entity classes."""

from types import MappingProxyType
from typing import ClassVar

from extended_otel_semconv.entities import RawAttributes, SemanticEntity
from extended_otel_semconv.generated._models import AppFields as _AppFields
from extended_otel_semconv.generated._models import AppEndpointFields as _AppEndpointFields
from extended_otel_semconv.generated._models import BrowserDocumentFields as _BrowserDocumentFields
from extended_otel_semconv.generated._models import CicdPipelineFields as _CicdPipelineFields
from extended_otel_semconv.generated._models import CicdPipelineRunFields as _CicdPipelineRunFields
from extended_otel_semconv.generated._models import CicdWorkerFields as _CicdWorkerFields
from extended_otel_semconv.generated._models import ContainerRuntimeFields as _ContainerRuntimeFields
from extended_otel_semconv.generated._models import EtlPartRunFields as _EtlPartRunFields
from extended_otel_semconv.generated._models import EtlPipelineFields as _EtlPipelineFields
from extended_otel_semconv.generated._models import EtlRunFields as _EtlRunFields
from extended_otel_semconv.generated._models import GcpGceInstanceGroupManagerFields as _GcpGceInstanceGroupManagerFields
from extended_otel_semconv.generated._models import K8sClusterFields as _K8sClusterFields
from extended_otel_semconv.generated._models import K8sContainerFields as _K8sContainerFields
from extended_otel_semconv.generated._models import K8sCronjobFields as _K8sCronjobFields
from extended_otel_semconv.generated._models import K8sDaemonsetFields as _K8sDaemonsetFields
from extended_otel_semconv.generated._models import K8sDeploymentFields as _K8sDeploymentFields
from extended_otel_semconv.generated._models import K8sHpaFields as _K8sHpaFields
from extended_otel_semconv.generated._models import K8sJobFields as _K8sJobFields
from extended_otel_semconv.generated._models import K8sNamespaceFields as _K8sNamespaceFields
from extended_otel_semconv.generated._models import K8sNodeFields as _K8sNodeFields
from extended_otel_semconv.generated._models import K8sNodeSystemContainerFields as _K8sNodeSystemContainerFields
from extended_otel_semconv.generated._models import K8sPersistentvolumeFields as _K8sPersistentvolumeFields
from extended_otel_semconv.generated._models import K8sPersistentvolumeclaimFields as _K8sPersistentvolumeclaimFields
from extended_otel_semconv.generated._models import K8sPodFields as _K8sPodFields
from extended_otel_semconv.generated._models import K8sReplicasetFields as _K8sReplicasetFields
from extended_otel_semconv.generated._models import K8sReplicationcontrollerFields as _K8sReplicationcontrollerFields
from extended_otel_semconv.generated._models import K8sResourcequotaFields as _K8sResourcequotaFields
from extended_otel_semconv.generated._models import K8sServiceFields as _K8sServiceFields
from extended_otel_semconv.generated._models import K8sStatefulsetFields as _K8sStatefulsetFields
from extended_otel_semconv.generated._models import OpenshiftClusterquotaFields as _OpenshiftClusterquotaFields
from extended_otel_semconv.generated._models import ProcessFields as _ProcessFields
from extended_otel_semconv.generated._models import ProcessExecutableFields as _ProcessExecutableFields
from extended_otel_semconv.generated._models import ProcessRuntimeFields as _ProcessRuntimeFields
from extended_otel_semconv.generated._models import ServiceFields as _ServiceFields
from extended_otel_semconv.generated._models import ServiceInstanceFields as _ServiceInstanceFields
from extended_otel_semconv.generated._models import ServiceNamespaceFields as _ServiceNamespaceFields
from extended_otel_semconv.generated._models import TelemetryDistroFields as _TelemetryDistroFields
from extended_otel_semconv.generated._models import TelemetrySdkFields as _TelemetrySdkFields
from extended_otel_semconv.generated._models import VcsRefFields as _VcsRefFields
from extended_otel_semconv.generated._models import VcsRepositoryFields as _VcsRepositoryFields

class App(_AppFields):
    entity_type: ClassVar[str] = "app"
    identity_fields: ClassVar[tuple[str, ...]] = ('app.build_id',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class AppEndpoint(_AppEndpointFields):
    entity_type: ClassVar[str] = "app.endpoint"
    identity_fields: ClassVar[tuple[str, ...]] = ('service.name', 'service.namespace', 'http.request.method', 'http.route')
    template_fields: ClassVar[tuple[str, ...]] = ()


class BrowserDocument(_BrowserDocumentFields):
    entity_type: ClassVar[str] = "browser.document"
    identity_fields: ClassVar[tuple[str, ...]] = ('browser.document.url.full',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class CicdPipeline(_CicdPipelineFields):
    entity_type: ClassVar[str] = "cicd.pipeline"
    identity_fields: ClassVar[tuple[str, ...]] = ('cicd.pipeline.name',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class CicdPipelineRun(_CicdPipelineRunFields):
    entity_type: ClassVar[str] = "cicd.pipeline.run"
    identity_fields: ClassVar[tuple[str, ...]] = ('cicd.pipeline.run.id',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class CicdWorker(_CicdWorkerFields):
    entity_type: ClassVar[str] = "cicd.worker"
    identity_fields: ClassVar[tuple[str, ...]] = ('cicd.worker.id',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class ContainerRuntime(_ContainerRuntimeFields):
    entity_type: ClassVar[str] = "container.runtime"
    identity_fields: ClassVar[tuple[str, ...]] = ('container.runtime.name', 'container.runtime.version')
    template_fields: ClassVar[tuple[str, ...]] = ()


class EtlPartRun(_EtlPartRunFields):
    entity_type: ClassVar[str] = "etl.part.run"
    identity_fields: ClassVar[tuple[str, ...]] = ('etl.pipeline.id', 'etl.run.id', 'etl.part.run.id')
    template_fields: ClassVar[tuple[str, ...]] = ()


class EtlPipeline(_EtlPipelineFields):
    entity_type: ClassVar[str] = "etl.pipeline"
    identity_fields: ClassVar[tuple[str, ...]] = ('etl.pipeline.id',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class EtlRun(_EtlRunFields):
    entity_type: ClassVar[str] = "etl.run"
    identity_fields: ClassVar[tuple[str, ...]] = ('etl.pipeline.id', 'etl.run.id')
    template_fields: ClassVar[tuple[str, ...]] = ()


class GcpGceInstanceGroupManager(_GcpGceInstanceGroupManagerFields):
    entity_type: ClassVar[str] = "gcp.gce.instance_group_manager"
    identity_fields: ClassVar[tuple[str, ...]] = ('gcp.gce.instance_group_manager.name', 'gcp.gce.instance_group_manager.zone', 'gcp.gce.instance_group_manager.region')
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sCluster(_K8sClusterFields):
    entity_type: ClassVar[str] = "k8s.cluster"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.cluster.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sContainer(_K8sContainerFields):
    entity_type: ClassVar[str] = "k8s.container"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.container.name',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sCronjob(_K8sCronjobFields):
    entity_type: ClassVar[str] = "k8s.cronjob"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.cronjob.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.cronjob.label', 'k8s.cronjob.annotation')


class K8sDaemonset(_K8sDaemonsetFields):
    entity_type: ClassVar[str] = "k8s.daemonset"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.daemonset.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.daemonset.label', 'k8s.daemonset.annotation')


class K8sDeployment(_K8sDeploymentFields):
    entity_type: ClassVar[str] = "k8s.deployment"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.deployment.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.deployment.label', 'k8s.deployment.annotation')


class K8sHpa(_K8sHpaFields):
    entity_type: ClassVar[str] = "k8s.hpa"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.hpa.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sJob(_K8sJobFields):
    entity_type: ClassVar[str] = "k8s.job"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.job.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.job.label', 'k8s.job.annotation')


class K8sNamespace(_K8sNamespaceFields):
    entity_type: ClassVar[str] = "k8s.namespace"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.namespace.name',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.namespace.label', 'k8s.namespace.annotation')


class K8sNode(_K8sNodeFields):
    entity_type: ClassVar[str] = "k8s.node"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.node.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.node.label', 'k8s.node.annotation')


class K8sNodeSystemContainer(_K8sNodeSystemContainerFields):
    entity_type: ClassVar[str] = "k8s.node.system_container"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.node.system_container.name',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sPersistentvolume(_K8sPersistentvolumeFields):
    entity_type: ClassVar[str] = "k8s.persistentvolume"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.persistentvolume.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.persistentvolume.label', 'k8s.persistentvolume.annotation')


class K8sPersistentvolumeclaim(_K8sPersistentvolumeclaimFields):
    entity_type: ClassVar[str] = "k8s.persistentvolumeclaim"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.persistentvolumeclaim.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.persistentvolumeclaim.label', 'k8s.persistentvolumeclaim.annotation')


class K8sPod(_K8sPodFields):
    entity_type: ClassVar[str] = "k8s.pod"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.pod.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.pod.label', 'k8s.pod.annotation')


class K8sReplicaset(_K8sReplicasetFields):
    entity_type: ClassVar[str] = "k8s.replicaset"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.replicaset.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.replicaset.label', 'k8s.replicaset.annotation')


class K8sReplicationcontroller(_K8sReplicationcontrollerFields):
    entity_type: ClassVar[str] = "k8s.replicationcontroller"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.replicationcontroller.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sResourcequota(_K8sResourcequotaFields):
    entity_type: ClassVar[str] = "k8s.resourcequota"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.resourcequota.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class K8sService(_K8sServiceFields):
    entity_type: ClassVar[str] = "k8s.service"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.service.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.service.selector', 'k8s.service.label', 'k8s.service.annotation')


class K8sStatefulset(_K8sStatefulsetFields):
    entity_type: ClassVar[str] = "k8s.statefulset"
    identity_fields: ClassVar[tuple[str, ...]] = ('k8s.statefulset.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ('k8s.statefulset.label', 'k8s.statefulset.annotation')


class OpenshiftClusterquota(_OpenshiftClusterquotaFields):
    entity_type: ClassVar[str] = "openshift.clusterquota"
    identity_fields: ClassVar[tuple[str, ...]] = ('openshift.clusterquota.uid',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class Process(_ProcessFields):
    entity_type: ClassVar[str] = "process"
    identity_fields: ClassVar[tuple[str, ...]] = ('process.pid', 'process.creation.time')
    template_fields: ClassVar[tuple[str, ...]] = ()


class ProcessExecutable(_ProcessExecutableFields):
    entity_type: ClassVar[str] = "process.executable"
    identity_fields: ClassVar[tuple[str, ...]] = ('process.executable.build_id.htlhash',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class ProcessRuntime(_ProcessRuntimeFields):
    entity_type: ClassVar[str] = "process.runtime"
    identity_fields: ClassVar[tuple[str, ...]] = ('process.runtime.name', 'process.runtime.version')
    template_fields: ClassVar[tuple[str, ...]] = ()


class Service(_ServiceFields):
    entity_type: ClassVar[str] = "service"
    identity_fields: ClassVar[tuple[str, ...]] = ('service.name',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class ServiceInstance(_ServiceInstanceFields):
    entity_type: ClassVar[str] = "service.instance"
    identity_fields: ClassVar[tuple[str, ...]] = ('service.instance.id',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class ServiceNamespace(_ServiceNamespaceFields):
    entity_type: ClassVar[str] = "service.namespace"
    identity_fields: ClassVar[tuple[str, ...]] = ('service.namespace',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class TelemetryDistro(_TelemetryDistroFields):
    entity_type: ClassVar[str] = "telemetry.distro"
    identity_fields: ClassVar[tuple[str, ...]] = ('telemetry.distro.name',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class TelemetrySdk(_TelemetrySdkFields):
    entity_type: ClassVar[str] = "telemetry.sdk"
    identity_fields: ClassVar[tuple[str, ...]] = ('telemetry.sdk.name', 'telemetry.sdk.language')
    template_fields: ClassVar[tuple[str, ...]] = ()


class VcsRef(_VcsRefFields):
    entity_type: ClassVar[str] = "vcs.ref"
    identity_fields: ClassVar[tuple[str, ...]] = ('vcs.ref.head.revision',)
    template_fields: ClassVar[tuple[str, ...]] = ()


class VcsRepository(_VcsRepositoryFields):
    entity_type: ClassVar[str] = "vcs.repository"
    identity_fields: ClassVar[tuple[str, ...]] = ('vcs.repository.url.full',)
    template_fields: ClassVar[tuple[str, ...]] = ()


ENTITY_MODELS = MappingProxyType({
    "app": App,
    "app.endpoint": AppEndpoint,
    "browser.document": BrowserDocument,
    "cicd.pipeline": CicdPipeline,
    "cicd.pipeline.run": CicdPipelineRun,
    "cicd.worker": CicdWorker,
    "container.runtime": ContainerRuntime,
    "etl.part.run": EtlPartRun,
    "etl.pipeline": EtlPipeline,
    "etl.run": EtlRun,
    "gcp.gce.instance_group_manager": GcpGceInstanceGroupManager,
    "k8s.cluster": K8sCluster,
    "k8s.container": K8sContainer,
    "k8s.cronjob": K8sCronjob,
    "k8s.daemonset": K8sDaemonset,
    "k8s.deployment": K8sDeployment,
    "k8s.hpa": K8sHpa,
    "k8s.job": K8sJob,
    "k8s.namespace": K8sNamespace,
    "k8s.node": K8sNode,
    "k8s.node.system_container": K8sNodeSystemContainer,
    "k8s.persistentvolume": K8sPersistentvolume,
    "k8s.persistentvolumeclaim": K8sPersistentvolumeclaim,
    "k8s.pod": K8sPod,
    "k8s.replicaset": K8sReplicaset,
    "k8s.replicationcontroller": K8sReplicationcontroller,
    "k8s.resourcequota": K8sResourcequota,
    "k8s.service": K8sService,
    "k8s.statefulset": K8sStatefulset,
    "openshift.clusterquota": OpenshiftClusterquota,
    "process": Process,
    "process.executable": ProcessExecutable,
    "process.runtime": ProcessRuntime,
    "service": Service,
    "service.instance": ServiceInstance,
    "service.namespace": ServiceNamespace,
    "telemetry.distro": TelemetryDistro,
    "telemetry.sdk": TelemetrySdk,
    "vcs.ref": VcsRef,
    "vcs.repository": VcsRepository,
})


def entities_from_attributes(attributes: RawAttributes) -> list[SemanticEntity]:
    entities: list[SemanticEntity] = []
    for entity_class in ENTITY_MODELS.values():
        entity = entity_class.from_attributes(attributes)
        if entity is not None:
            entities.append(entity)
    return entities
