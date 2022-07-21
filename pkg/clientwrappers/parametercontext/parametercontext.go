package parametercontext

import (
	"fmt"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
)

var log = common.CustomLogger().Named("parametercontext-method")

func CreateIfNotExists(parameterContext *v1alpha1.NifiParameterContext, parameterSecrets []*corev1.Secret, parameterContextRefs []*v1alpha1.NifiParameterContext, config *clientconfig.NifiConfig) (*v1alpha1.NifiParameterContextStatus, error) {
	// set the seed header if it exists in the parameter context status
	if parameterContext.Status.IdSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = parameterContext.Status.IdSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	status, err := findParameterContextByName(parameterContext, config)
	if err := clientwrappers.ErrorGetOperation(log, err, "find parameter context by name"); err != nil {
		return nil, err
	}
	// TakeOver disabled
	if status != nil && !parameterContext.Spec.IsTakeOverEnabled() {
		return nil, fmt.Errorf("parameter context name %s already used and takeOver disabled", parameterContext.Name)
	}

	// if the parameter context id hasn't been set, then it hasn't been created. Create it.
	if parameterContext.Status.Id == "" {
		status, err := CreateParameterContext(parameterContext, parameterSecrets, parameterContextRefs, config)
		if err != nil {
			return nil, err
		}
		return status, nil
	}

	// double check that the parameter context exists at each configured client
	entities, err := nClient.GetParameterContext(parameterContext.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "get parameter context"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.NifiParameterContextStatus
	for _, entity := range entities {
		if entity == nil || entity.Entity == nil {
			status, err := CreateParameterContextWithClient(parameterContext, parameterSecrets, parameterContextRefs, config, entity.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func findParameterContextByName(parameterContext *v1alpha1.NifiParameterContext, config *clientconfig.NifiConfig) (*v1alpha1.NifiParameterContextStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetParameterContexts()
	if err := clientwrappers.ErrorGetOperation(log, err, "Get parameter-contexts"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil, nil
		}
		return nil, err
	}

	for _, entityPair := range entities {
		paramContexts := entityPair.Entity
		for _, entity := range *paramContexts {
			if parameterContext.GetName() == entity.Component.Name {
				return &v1alpha1.NifiParameterContextStatus{
					Id:      entity.Id,
					Version: *entity.Revision.Version,
				}, nil
			}
		}
	}

	return nil, nil
}

func CreateParameterContext(
	parameterContext *v1alpha1.NifiParameterContext,
	parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext,
	config *clientconfig.NifiConfig) (*v1alpha1.NifiParameterContextStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.ParameterContextEntity{}
	updateParameterContextEntity(parameterContext, parameterSecrets, parameterContextRefs, &scratchEntity)

	entities, err := nClient.CreateParameterContext(scratchEntity)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create parameter-context"); err != nil {
		return nil, err
	}

	// all paramter contexts will be the same, so just grab the first status
	parameterContext.Status.Id = entities[0].Entity.Id
	parameterContext.Status.IdSeed = config.GetIDSeedHeader()
	parameterContext.Status.Version = *entities[0].Entity.Revision.Version

	return &parameterContext.Status, nil
}

func CreateParameterContextWithClient(parameterContext *v1alpha1.NifiParameterContext, parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext, config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (*v1alpha1.NifiParameterContextStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.ParameterContextEntity{}
	updateParameterContextEntity(parameterContext, parameterSecrets, parameterContextRefs, &scratchEntity)

	entity, err := nClient.CreateParameterContextWithClient(scratchEntity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create parameter-context"); err != nil {
		return nil, err
	}

	parameterContext.Status.Id = entity.Entity.Id
	parameterContext.Status.IdSeed = config.GetIDSeedHeader()
	parameterContext.Status.Version = *entity.Entity.Revision.Version

	return &parameterContext.Status, nil
}

func SyncParameterContext(
	parameterContext *v1alpha1.NifiParameterContext,
	parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext,
	config *clientconfig.NifiConfig) (*v1alpha1.NifiParameterContextStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	entities, err := nClient.GetParameterContext(parameterContext.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get parameter-context"); err != nil {
		return nil, err
	}

	var status *v1alpha1.NifiParameterContextStatus
	for _, entityPair := range entities {
		// If the parameter context is not returned, then either it hasn't been fully created or nifikop doesn't yet have permission to view the PC
		if entityPair == nil || entityPair.Entity == nil {
			return nil, errorfactory.NifiParameterContextSyncing{}
		}

		latestUpdateRequest := parameterContext.Status.LatestUpdateRequest
		if latestUpdateRequest != nil && !latestUpdateRequest.Complete {
			updateRequest, err := nClient.GetParameterContextUpdateRequestWithClient(parameterContext.Status.Id, latestUpdateRequest.Id, entityPair.Client)
			if updateRequest.Entity != nil {
				parameterContext.Status.LatestUpdateRequest = updateRequest2Status(updateRequest.Entity)
			}

			if err := clientwrappers.ErrorGetOperation(log, err, "Get update-request"); err != nificlient.ErrNifiClusterReturned404 {
				if err != nil {
					return &parameterContext.Status, err
				}
				return &parameterContext.Status, errorfactory.NifiParameterContextUpdateRequestRunning{}
			}
		}

		entity := entityPair.Entity
		if !parameterContextIsSync(parameterContext, parameterSecrets, parameterContextRefs, entity) {
			entity.Component.Parameters = updateRequestPrepare(parameterContext, parameterSecrets, parameterContextRefs, entity)

			updateRequest, err := nClient.CreateParameterContextUpdateRequestWithClient(entity.Id, *entity, entityPair.Client)
			if err := clientwrappers.ErrorCreateOperation(log, err, "Create parameter-context update-request"); err != nil {
				return nil, err
			}

			parameterContext.Status.LatestUpdateRequest = updateRequest2Status(updateRequest.Entity)
			return &parameterContext.Status, errorfactory.NifiParameterContextUpdateRequestRunning{}
		}

		if parameterContext.Status.Version != *entity.Revision.Version || parameterContext.Status.Id != entity.Id {
			status = &parameterContext.Status
			status.Version = *entity.Revision.Version
			status.Id = entity.Id
		}
	}

	return status, nil
}

func RemoveParameterContext(
	parameterContext *v1alpha1.NifiParameterContext,
	parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext,
	config *clientconfig.NifiConfig) error {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	entities, err := nClient.GetParameterContext(parameterContext.Status.Id)
	if err := clientwrappers.ErrorGetOperation(log, err, "Failed to fetch parameter-context for removal: "+parameterContext.Name); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil
		}
		return err
	}

	for _, entityPair := range entities {
		if entityPair != nil && entityPair.Entity != nil {
			entity := entityPair.Entity
			// if there's not a parameter context for this specific client, just skip
			if entity == nil {
				continue
			}
			updateParameterContextEntity(parameterContext, parameterSecrets, parameterContextRefs, entity)
			err = nClient.RemoveParameterContextWithClient(*entity, entityPair.Client)
			if err != nil {
				return clientwrappers.ErrorRemoveOperation(log, err, "Failed to remove parameter-context "+parameterContext.Name)
			}
		}
	}

	return nil
}

func parameterContextIsSync(parameterContext *v1alpha1.NifiParameterContext, parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext, entity *nigoapi.ParameterContextEntity) bool {

	e := nigoapi.ParameterContextEntity{}
	updateParameterContextEntity(parameterContext, parameterSecrets, parameterContextRefs, &e)

	if len(e.Component.Parameters) != len(entity.Component.Parameters) {
		return false
	}

	for _, expected := range e.Component.Parameters {
		notFound := true
		for _, param := range entity.Component.Parameters {
			if expected.Parameter.Name == param.Parameter.Name {
				notFound = false

				if (!param.Parameter.Sensitive &&
					!((expected.Parameter.Value == nil && param.Parameter.Value == nil) ||
						((expected.Parameter.Value != nil && param.Parameter.Value != nil) &&
							(*expected.Parameter.Value == *param.Parameter.Value)))) ||
					!((expected.Parameter.Description == nil && param.Parameter.Description == nil) ||
						((expected.Parameter.Description != nil && param.Parameter.Description != nil) &&
							(*expected.Parameter.Description == *param.Parameter.Description))) {

					return false
				}
			}
		}
		if notFound {
			return false
		}
	}

	if len(e.Component.InheritedParameterContexts) != len(entity.Component.InheritedParameterContexts) {
		return false
	}

	for idx, expected := range e.Component.InheritedParameterContexts {
		if expected.Id != entity.Component.InheritedParameterContexts[idx].Id {
			return false
		}
	}

	return e.Component.Description == entity.Component.Description && e.Component.Name == entity.Component.Name
}

func updateRequestPrepare(
	parameterContext *v1alpha1.NifiParameterContext,
	parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext,
	entity *nigoapi.ParameterContextEntity) []nigoapi.ParameterEntity {

	tmp := entity.Component.Parameters
	updateParameterContextEntity(parameterContext, parameterSecrets, parameterContextRefs, entity)

	// List all parameter to remove
	var toRemove []string
	for _, toFind := range tmp {
		notFound := true
		for _, p := range entity.Component.Parameters {
			if p.Parameter.Name == toFind.Parameter.Name {
				notFound = false
				break
			}
		}

		if notFound {
			toRemove = append(toRemove, toFind.Parameter.Name)
		}
	}

	// List all parameter to upsert
	parameters := make([]nigoapi.ParameterEntity, 0)
	for _, expected := range entity.Component.Parameters {
		notFound := true
		for _, param := range tmp {
			if expected.Parameter.Name == param.Parameter.Name {
				notFound = false
				if (!param.Parameter.Sensitive &&
					!((expected.Parameter.Value == nil && param.Parameter.Value == nil) ||
						((expected.Parameter.Value != nil && param.Parameter.Value != nil) &&
							(*expected.Parameter.Value == *param.Parameter.Value)))) ||
					!((expected.Parameter.Description == nil && param.Parameter.Description == nil) ||
						((expected.Parameter.Description != nil && param.Parameter.Description != nil) &&
							(*expected.Parameter.Description == *param.Parameter.Description))) {

					notFound = false
					if expected.Parameter.Value == nil && param.Parameter.Value != nil {
						toRemove = append(toRemove, expected.Parameter.Name)
						break
					}
					parameters = append(parameters, expected)
					break
				}
			}
		}
		if notFound {
			parameters = append(parameters, expected)
		}
	}

	for _, name := range toRemove {
		parameters = append(parameters, nigoapi.ParameterEntity{
			Parameter: &nigoapi.ParameterDto{
				Name: name,
			},
		})
	}

	return parameters
}

func updateParameterContextEntity(
	parameterContext *v1alpha1.NifiParameterContext,
	parameterSecrets []*corev1.Secret,
	parameterContextRefs []*v1alpha1.NifiParameterContext,
	entity *nigoapi.ParameterContextEntity) {

	var defaultVersion int64 = 0
	if entity == nil {
		entity = &nigoapi.ParameterContextEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.ParameterContextDto{}
	}

	parameters := make([]nigoapi.ParameterEntity, 0)

	emptyString := ""
	for _, secret := range parameterSecrets {
		for k, v := range secret.Data {
			value := string(v)
			parameters = append(parameters, nigoapi.ParameterEntity{
				Parameter: &nigoapi.ParameterDto{
					Name:        k,
					Description: &emptyString,
					Sensitive:   true,
					Value:       &value,
				},
			})
		}
	}

	for _, parameter := range parameterContext.Spec.Parameters {
		desc := parameter.Description
		parameters = append(parameters, nigoapi.ParameterEntity{
			Parameter: &nigoapi.ParameterDto{
				Name:        parameter.Name,
				Description: &desc,
				Sensitive:   parameter.Sensitive,
				Value:       parameter.Value,
			},
		})
	}

	inheritedParameterContexts := make([]nigoapi.ParameterContextReferenceEntity, 0)
	for _, parameterContextRef := range parameterContextRefs {
		inheritedParameterContexts = append(inheritedParameterContexts,
			nigoapi.ParameterContextReferenceEntity{
				Id: parameterContextRef.Status.Id,
				Component: &nigoapi.ParameterContextReferenceDto{
					Id:   parameterContextRef.Status.Id,
					Name: parameterContextRef.Name,
				},
			})
	}

	entity.Component.Name = parameterContext.Name
	entity.Component.Description = parameterContext.Spec.Description
	entity.Component.Parameters = parameters
	entity.Component.InheritedParameterContexts = inheritedParameterContexts
}

func updateRequest2Status(updateRequest *nigoapi.ParameterContextUpdateRequestEntity) *v1alpha1.ParameterContextUpdateRequest {
	ur := updateRequest.Request
	return &v1alpha1.ParameterContextUpdateRequest{
		Id:               ur.RequestId,
		Uri:              ur.Uri,
		SubmissionTime:   ur.SubmissionTime,
		LastUpdated:      ur.LastUpdated,
		Complete:         ur.Complete,
		FailureReason:    ur.FailureReason,
		PercentCompleted: ur.PercentCompleted,
		State:            ur.State,
	}
}
