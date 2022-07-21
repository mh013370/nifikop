package registryclient

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

var log = common.CustomLogger().Named("registryclient-method")

func CreateIfNotExists(registryClient *v1alpha1.NifiRegistryClient, config *clientconfig.NifiConfig) (*v1alpha1.NifiRegistryClientStatus, error) {
	// set the seed header if it exists in the parameter context status
	if registryClient.Status.IdSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = registryClient.Status.IdSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if registryClient.Status.Id == "" {
		status, err := CreateRegistryClient(registryClient, config)
		if err != nil {
			return nil, err
		}
		return status, nil
	}

	// double check that the flow exists at each configured client
	entities, err := nClient.GetRegistryClient(registryClient.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "get registry client"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.NifiRegistryClientStatus
	for _, entity := range entities {
		if entity == nil || entity.Entity == nil {
			status, err := CreateRegistryClientWithClient(registryClient, config, entity.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func CreateRegistryClient(registryClient *v1alpha1.NifiRegistryClient,
	config *clientconfig.NifiConfig) (*v1alpha1.NifiRegistryClientStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.RegistryClientEntity{}
	updateRegistryClientEntity(registryClient, &scratchEntity)

	entities, err := nClient.CreateRegistryClient(scratchEntity)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Failed to create registry-client "+registryClient.Name); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiRegistryClientStatus{
		Id:      entities[0].Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entities[0].Entity.Revision.Version,
	}, nil
}

func CreateRegistryClientWithClient(registryClient *v1alpha1.NifiRegistryClient,
	config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (*v1alpha1.NifiRegistryClientStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.RegistryClientEntity{}
	updateRegistryClientEntity(registryClient, &scratchEntity)

	entity, err := nClient.CreateRegistryClientWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Failed to create registry-client "+registryClient.Name); err != nil {
		return nil, err
	}

	return &v1alpha1.NifiRegistryClientStatus{
		Id:      entity.Entity.Id,
		IdSeed:  config.GetIDSeedHeader(),
		Version: *entity.Entity.Revision.Version,
	}, nil
}

func SyncRegistryClient(registryClient *v1alpha1.NifiRegistryClient,
	config *clientconfig.NifiConfig) (*v1alpha1.NifiRegistryClientStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetRegistryClient(registryClient.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get registry-client"); err != nil {
		return nil, err
	}

	var status v1alpha1.NifiRegistryClientStatus
	var updatedEntity *nificlient.ClientEntityPair[nigoapi.RegistryClientEntity]
	for _, entityPair := range entities {

		// If the registry client is not returned, then either it hasn't been fully created or nifikop doesn't yet have permission to view the client
		if entityPair == nil || entityPair.Entity == nil {
			return nil, errorfactory.NifiRegistryClientSyncing{}
		}
		entity := entityPair.Entity

		if !registryClientIsSync(registryClient, entity) {
			log.Info("Registry client is out of sync. Updating.",
				zap.String("clusterName", registryClient.Spec.ClusterRef.Name),
				zap.String("registryClientName", registryClient.Name))
			updateRegistryClientEntity(registryClient, entity)
			updatedEntity, err = nClient.UpdateRegistryClientWithClient(*entity, entityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update registry-client"); err != nil {
				return nil, err
			}
			entity = updatedEntity.Entity
		}

		status = registryClient.Status
		status.Version = *entity.Revision.Version
		status.Id = entity.Id
	}

	return &status, nil
}

func RemoveRegistryClient(registryClient *v1alpha1.NifiRegistryClient, config *clientconfig.NifiConfig) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetRegistryClient(registryClient.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get registry-client"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil
		}
		return err
	}

	for _, entityPair := range entities {
		if entityPair != nil && entityPair.Entity != nil {
			entity := entityPair.Entity
			updateRegistryClientEntity(registryClient, entity)
			err = nClient.RemoveRegistryClientWithClient(*entity, entityPair.Client)
			if err != nil {
				return clientwrappers.ErrorRemoveOperation(log, err, "Remove registry-client")
			}
		}
	}

	return nil
}

func registryClientIsSync(registryClient *v1alpha1.NifiRegistryClient, entity *nigoapi.RegistryClientEntity) bool {
	return registryClient.Name == entity.Component.Name &&
		registryClient.Spec.Description == entity.Component.Description &&
		registryClient.Spec.Uri == entity.Component.Uri
}

func updateRegistryClientEntity(registryClient *v1alpha1.NifiRegistryClient, entity *nigoapi.RegistryClientEntity) {

	var defaultVersion int64 = 0

	if entity == nil {
		entity = &nigoapi.RegistryClientEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.RegistryDto{}
	}

	entity.Component.Name = registryClient.Name
	entity.Component.Description = registryClient.Spec.Description
	entity.Component.Uri = registryClient.Spec.Uri
}
