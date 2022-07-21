package reportingtask

import (
	"strconv"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

var log = common.CustomLogger().Named("reportingtask-method")

const (
	reportingTaskName                = "managed-prometheus"
	reportingTaskType_               = "org.apache.nifi.reporting.prometheus.PrometheusReportingTask"
	reportingTaskEnpointPortProperty = "prometheus-reporting-task-metrics-endpoint-port"
	reportingTaskStrategyProperty    = "prometheus-reporting-task-metrics-strategy"
	reportingTaskStrategy            = "All Components"
	reportingTaskSendJVMProperty     = "prometheus-reporting-task-metrics-send-jvm"
	reportingTaskSendJVM             = "true"
)

func CreateIfNotExists(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster) (*v1alpha1.PrometheusReportingTaskStatus, error) {
	// set the seed header if it exists in the reporting task status
	if cluster.Status.PrometheusReportingTask.IdSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = cluster.Status.PrometheusReportingTask.IdSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if cluster.Status.PrometheusReportingTask.Id == "" {
		status, err := CreateReportingTask(config, cluster)
		if err != nil {
			return nil, err
		}
		return status, nil
	}

	// double check that the flow exists at each configured client
	entities, err := nClient.GetReportingTask(cluster.Status.PrometheusReportingTask.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "get reporting task"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.PrometheusReportingTaskStatus
	for _, entity := range entities {
		if entity == nil || entity.Entity == nil {
			status, err := CreateReportingTaskWithClient(config, cluster, entity.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func CreateReportingTask(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster) (*v1alpha1.PrometheusReportingTaskStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.ReportingTaskEntity{}
	updateReportingTaskEntity(cluster, &scratchEntity)

	entities, err := nClient.CreateReportingTask(scratchEntity)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create reporting-task"); err != nil {
		return nil, err
	}

	return &v1alpha1.PrometheusReportingTaskStatus{
		Id:      entities[0].Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entities[0].Entity.Revision.Version,
	}, nil
}

func CreateReportingTaskWithClient(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster, client nificlient.ClientContextPair) (*v1alpha1.PrometheusReportingTaskStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}
	log.Info("Creating Prometheus Reporting task with client",
		zap.String("idGenerationHeader", config.GetIDSeedHeader()),
		zap.String("clusterName", cluster.Name))

	scratchEntity := nigoapi.ReportingTaskEntity{}
	updateReportingTaskEntity(cluster, &scratchEntity)

	entity, err := nClient.CreateReportingTaskWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create reporting-task"); err != nil {
		return nil, err
	}

	return &v1alpha1.PrometheusReportingTaskStatus{
		Id:      entity.Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity.Entity.Revision.Version,
	}, nil
}

func SyncReportingTask(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster) (*v1alpha1.PrometheusReportingTaskStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetReportingTask(cluster.Status.PrometheusReportingTask.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get reporting-task"); err != nil {
		return nil, err
	}

	var status v1alpha1.PrometheusReportingTaskStatus
	for _, entityPair := range entities {
		// If the reporting task is not returned, then either it hasn't been fully created or nifikop doesn't yet have permission to view it
		if entityPair == nil || entityPair.Entity == nil {
			return nil, errorfactory.NifiReportingTaskSyncing{}
		}
		entity := entityPair.Entity

		var updatedEntity *nificlient.ClientEntityPair[nigoapi.ReportingTaskEntity]

		if !reportingTaskIsSync(cluster, entity) {
			statusDto := entity.Status

			if statusDto.ValidationStatus == "VALIDATING" {
				return nil, errorfactory.NifiReportingTasksValidating{}
			}

			if statusDto.RunStatus == "RUNNING" {
				updatedEntity, err = nClient.UpdateRunStatusReportingTaskWithClient(entity.Id, nigoapi.ReportingTaskRunStatusEntity{
					Revision: entity.Revision,
					State:    "STOPPED",
				}, entityPair.Client)
				if err := clientwrappers.ErrorUpdateOperation(log, err, "Update reporting-task status"); err != nil {
					return nil, err
				}
				entity = updatedEntity.Entity
			}

			updateReportingTaskEntity(cluster, entity)
			updatedEntityPair, err := nClient.UpdateReportingTaskWithClient(*entity, entityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update reporting-task"); err != nil {
				return nil, err
			}
			entity = updatedEntityPair.Entity
		}

		if entity.Status.ValidationStatus == "INVALID" {
			return nil, errorfactory.NifiReportingTasksInvalid{}
		}

		if entity.Status.RunStatus == "STOPPED" || entity.Status.RunStatus == "DISABLED" {
			log.Info("Starting Prometheus reporting task",
				zap.String("clusterName", cluster.Name))
			updatedEntity, err = nClient.UpdateRunStatusReportingTaskWithClient(entity.Id, nigoapi.ReportingTaskRunStatusEntity{
				Revision: entity.Revision,
				State:    "RUNNING",
			}, entityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update reporting-task status"); err != nil {
				return nil, err
			}
			entity = updatedEntity.Entity
		}

		status = cluster.Status.PrometheusReportingTask
		status.Version = *entity.Revision.Version
		status.Id = entity.Id
	}

	return &status, nil
}

func RemoveReportingTask(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetReportingTask(cluster.Status.PrometheusReportingTask.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get reporting-task"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil
		}
		return err
	}

	for _, entityPair := range entities {
		if entityPair != nil && entityPair.Entity != nil {
			entity := entityPair.Entity
			updateReportingTaskEntity(cluster, entity)
			err = nClient.RemoveReportingTaskWithClient(*entity, entityPair.Client)
			if err != nil {
				return clientwrappers.ErrorRemoveOperation(log, err, "Remove registry-client")
			}
		}
	}

	return nil
}

func reportingTaskIsSync(cluster *v1alpha1.NifiCluster, entity *nigoapi.ReportingTaskEntity) bool {
	return reportingTaskName == entity.Component.Name &&
		strconv.Itoa(*cluster.Spec.GetMetricPort()) == entity.Component.Properties[reportingTaskEnpointPortProperty] &&
		reportingTaskStrategy == entity.Component.Properties[reportingTaskStrategyProperty] &&
		reportingTaskSendJVM == entity.Component.Properties[reportingTaskSendJVMProperty]
}

func updateReportingTaskEntity(cluster *v1alpha1.NifiCluster, entity *nigoapi.ReportingTaskEntity) {

	var defaultVersion int64 = 0

	if entity == nil {
		entity = &nigoapi.ReportingTaskEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.ReportingTaskDto{}
	}

	entity.Component.Name = "managed-prometheus"
	entity.Component.Type_ = "org.apache.nifi.reporting.prometheus.PrometheusReportingTask"
	entity.Component.Properties = map[string]string{
		reportingTaskEnpointPortProperty: strconv.Itoa(*cluster.Spec.GetMetricPort()),
		reportingTaskStrategyProperty:    reportingTaskStrategy,
		reportingTaskSendJVMProperty:     reportingTaskSendJVM,
	}
}
