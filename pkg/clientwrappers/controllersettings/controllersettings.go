package controllersettings

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
)

var log = common.CustomLogger().Named("controllersettings-method")

func controllerConfigIsSync(cluster *v1alpha1.NifiCluster, entity *nigoapi.ControllerConfigurationEntity) bool {
	return cluster.Spec.ReadOnlyConfig.GetMaximumTimerDrivenThreadCount() == entity.Component.MaxTimerDrivenThreadCount &&
		cluster.Spec.ReadOnlyConfig.GetMaximumEventDrivenThreadCount() == entity.Component.MaxEventDrivenThreadCount
}

func SyncConfiguration(config *clientconfig.NifiConfig, cluster *v1alpha1.NifiCluster) error {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetControllerConfig()
	if err := clientwrappers.ErrorGetOperation(log, err, "Get controller config"); err != nil {
		return err
	}

	for _, entity := range entities {
		if !controllerConfigIsSync(cluster, entity.Entity) {
			updateControllerConfigEntity(cluster, entity)
			updatedEntity, err := nClient.UpdateControllerConfigWithClient(*entity.Entity, entity.Client)
			entity.Entity = updatedEntity.Entity
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Failed to update controller config"); err != nil {
				return err
			}
		}
	}
	return nil
}

func updateControllerConfigEntity(cluster *v1alpha1.NifiCluster, entity *nificlient.ClientEntityPair[nigoapi.ControllerConfigurationEntity]) {
	if entity.Entity == nil {
		entity.Entity = &nigoapi.ControllerConfigurationEntity{}
	}

	if entity.Entity.Component == nil {
		entity.Entity.Revision = &nigoapi.RevisionDto{}
	}

	if entity.Entity.Component == nil {
		entity.Entity.Component = &nigoapi.ControllerConfigurationDto{}
	}
	entity.Entity.Component.MaxTimerDrivenThreadCount = cluster.Spec.ReadOnlyConfig.GetMaximumTimerDrivenThreadCount()
	entity.Entity.Component.MaxEventDrivenThreadCount = cluster.Spec.ReadOnlyConfig.GetMaximumEventDrivenThreadCount()
}
