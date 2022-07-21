package dataflow

import (
	"strings"

	"github.com/google/uuid"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	"go.uber.org/zap"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/clientwrappers"
	"github.com/konpyutaika/nifikop/pkg/common"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/nificlient"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
)

var log = common.CustomLogger().Named("dataflow-method")

func CreateIfNotExists(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig, registry *v1alpha1.NifiRegistryClient) (*v1alpha1.NifiDataflowStatus, error) {
	// set the seed header if it exists in the flow status
	if flow.Status.ProcessGroupIDSeed != "" {
		config.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = flow.Status.ProcessGroupIDSeed
	}

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	// if the process group hasn't been set, then the flow hasn't been created. Create it.
	if flow.Status.ProcessGroupID == "" {
		status, err := CreateDataflow(flow, config, registry)
		if err != nil {
			return nil, err
		}
		return status, nil
	}

	// double check that the flow exists at each configured client
	entities, err := nClient.GetFlow(flow.Status.ProcessGroupID)
	if err := clientwrappers.ErrorGetOperation(log, err, "get flow"); err != nil {
		return nil, err
	}
	var retStatus *v1alpha1.NifiDataflowStatus
	for _, entity := range entities {
		if entity == nil || entity.Entity == nil {
			status, err := CreateDataflowWithClient(flow, config, registry, entity.Client)
			if err != nil {
				return nil, err
			}
			retStatus = status
		}
	}

	return retStatus, nil
}

func RootProcessGroup(config *clientconfig.NifiConfig) (string, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return "", err
	}

	rootPg, err := nClient.GetFlow(clientconfig.ROOT_PROCESS_GROUP_ALIAS)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return "", nil
		}
		return "", err
	}
	var rootPgId string
	for _, pg := range rootPg {
		if pg.Entity != nil && pg.Entity.ProcessGroupFlow != nil {
			rootPgId = pg.Entity.ProcessGroupFlow.Id
			break
		}
	}

	return rootPgId, nil
}

func RootProcessGroupWithClient(config *clientconfig.NifiConfig, client nificlient.ClientContextPair) (string, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return "", err
	}

	rootPg, err := nClient.GetFlowWithClient(clientconfig.ROOT_PROCESS_GROUP_ALIAS, client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return "", nil
		}
		return "", err
	}
	var rootPgId string
	if rootPg != nil && rootPg.Entity != nil && rootPg.Entity.ProcessGroupFlow != nil {
		rootPgId = rootPg.Entity.ProcessGroupFlow.Id
	}

	return rootPgId, nil
}

// CreateDataflow will deploy the NifiDataflow on NiFi Cluster
func CreateDataflow(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig, registry *v1alpha1.NifiRegistryClient) (*v1alpha1.NifiDataflowStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.ProcessGroupEntity{}
	updateProcessGroupEntity(flow, registry, config, &scratchEntity)

	entities, err := nClient.CreateProcessGroup(scratchEntity, flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId))

	if err := clientwrappers.ErrorCreateOperation(log, err, "Create process-group"); err != nil {
		return nil, err
	}

	// All of the flows will have the same ID, so just grab the first
	flow.Status.ProcessGroupID = entities[0].Entity.Id
	flow.Status.ProcessGroupIDSeed = config.GetIDSeedHeader()
	return &flow.Status, nil
}

// CreateDataflow will deploy the NifiDataflow on NiFi Cluster
func CreateDataflowWithClient(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig, registry *v1alpha1.NifiRegistryClient, client nificlient.ClientContextPair) (*v1alpha1.NifiDataflowStatus, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	scratchEntity := nigoapi.ProcessGroupEntity{}
	updateProcessGroupEntity(flow, registry, config, &scratchEntity)

	entity, err := nClient.CreateProcessGroupWithClient(scratchEntity, flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId), client)

	if err := clientwrappers.ErrorCreateOperation(log, err, "Create process-group"); err != nil {
		return nil, err
	}

	flow.Status.ProcessGroupID = entity.Entity.Id
	flow.Status.ProcessGroupIDSeed = config.GetIDSeedHeader()
	return &flow.Status, nil
}

// ScheduleDataflow will schedule the controller services and components of the NifiDataflow.
func ScheduleDataflow(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	// Schedule controller services
	_, err = nClient.UpdateFlowControllerServices(nigoapi.ActivateControllerServicesEntity{
		Id:    flow.Status.ProcessGroupID,
		State: "ENABLED",
	})
	if err := clientwrappers.ErrorUpdateOperation(log, err, "Schedule flow's controller services"); err != nil {
		return err
	}

	// Check all controller services are enabled
	csEntities, err := nClient.GetFlowControllerServices(flow.Status.ProcessGroupID)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow controller services"); err != nil {
		return err
	}
	for _, csEntity := range csEntities {
		for _, contServices := range csEntity.Entity.ControllerServices {
			if contServices.Status.RunStatus != "ENABLED" &&
				!(flow.Spec.SkipInvalidControllerService && contServices.Status.ValidationStatus == "INVALID") {
				return errorfactory.NifiFlowControllerServiceScheduling{}
			}
		}
	}

	// Schedule flow
	_, err = nClient.UpdateFlowProcessGroup(nigoapi.ScheduleComponentsEntity{
		Id:    flow.Status.ProcessGroupID,
		State: "RUNNING",
	})
	if err := clientwrappers.ErrorUpdateOperation(log, err, "Schedule flow"); err != nil {
		return err
	}

	// Check all components are ok
	componentGroups, err := listComponents(config, flow.Status.ProcessGroupID, nificlient.ClientContextPair{})
	if err != nil {
		return err
	}
	for _, componentGroup := range componentGroups {
		processGroups := componentGroup.Entity.processGroups
		pGEntities, err := nClient.GetProcessGroupWithClient(flow.Status.ProcessGroupID, componentGroup.Client)
		if err := clientwrappers.ErrorGetOperation(log, err, "Get process group"); err != nil {
			return err
		}
		processGroups = append(processGroups, *pGEntities.Entity)

		for _, pgEntity := range processGroups {
			if pgEntity.StoppedCount > 0 || (!flow.Spec.SkipInvalidComponent && pgEntity.InvalidCount > 0) {
				return errorfactory.NifiFlowScheduling{}
			}
		}
	}

	log.Debug("Successfully scheduled flow.",
		zap.String("clusterName", flow.Spec.ClusterRef.Name),
		zap.String("flowId", flow.Spec.FlowId),
		zap.String("flowName", flow.Name))

	return nil
}

// IsOutOfSyncDataflowWithClient control if the deployed dataflow is out of sync with the NifiDataflow resource
func IsOutOfSyncDataflow(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig,
	registry *v1alpha1.NifiRegistryClient, parameterContext *v1alpha1.NifiParameterContext) (bool, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return false, err
	}

	flowEntities, err := nClient.GetFlow(flow.Status.ProcessGroupID)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return false, nil
		}
		return false, err
	}

	var status bool
	for _, flowEntity := range flowEntities {
		status, err = IsOutOfSyncDataflowWithClient(flowEntity.Client, flow, config, registry, parameterContext)
		if err != nil {
			return false, err
		}
	}
	return status, nil
}

// IsOutOfSyncDataflowWithClient control if the deployed dataflow is out of sync with the NifiDataflow resource
func IsOutOfSyncDataflowWithClient(client nificlient.ClientContextPair, flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig,
	registry *v1alpha1.NifiRegistryClient, parameterContext *v1alpha1.NifiParameterContext) (bool, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return false, err
	}

	entity, err := nClient.GetProcessGroupWithClient(flow.Status.ProcessGroupID, client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get process group"); err != nil {
		return false, err
	}

	componentGroups, err := listComponents(config, flow.Status.ProcessGroupID, entity.Client)
	// since we provided a client, there will be only 1 item in the returned list.
	processGroups := componentGroups[0].Entity.processGroups
	if err != nil {
		return false, err
	}

	processGroups = append(processGroups, *entity.Entity)

	return isParameterContextChanged(parameterContext, processGroups) ||
		isVersioningChanged(flow, registry, entity.Entity) || !isVersionSync(flow, entity.Entity) || localChanged(entity.Entity) ||
		isParentProcessGroupChanged(flow, config, entity.Entity, entity.Client) || isNameChanged(flow, entity.Entity) || isPostionChanged(flow, entity.Entity), nil
}

func isParameterContextChanged(parameterContext *v1alpha1.NifiParameterContext, processGroups []nigoapi.ProcessGroupEntity) bool {
	for _, processGroup := range processGroups {
		pgParameterContext := processGroup.ParameterContext

		if pgParameterContext == nil && parameterContext == nil {
			continue
		}

		if (pgParameterContext == nil && parameterContext != nil) ||
			(pgParameterContext != nil && parameterContext == nil) ||
			processGroup.ParameterContext.Id != parameterContext.Status.Id {
			return true
		}
	}
	return false
}

func isParentProcessGroupChanged(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig, pgFlowEntity *nigoapi.ProcessGroupEntity, client nificlient.ClientContextPair) bool {
	flowParentId := flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId)
	// If the cluster is standalone, then each node will have a different root process group ID. All other PGs will have the same ID across all nodes, so the rules are:
	// If the flow is configured to be at the root level, then check if the flow from NiFi has a parent ID different than the root. If it does, then the parent PG changed.
	// If the flow is configured to be at some other level, then check if the parent PG IDs changed just like in the normal case.
	if config.IsStandalone {
		rootPgId, err := RootProcessGroupWithClient(config, client)
		if err != nil {
			log.Warn("Failed to fetch root process group for node. Not taking any action to prevent unnecessary issues.",
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowName", flow.Name))
			return false
		}

		if flowParentId == clientconfig.ROOT_PROCESS_GROUP_ALIAS {
			return rootPgId != pgFlowEntity.Component.ParentGroupId
		} else {
			return flowParentId != pgFlowEntity.Component.ParentGroupId
		}
	}

	return flowParentId != pgFlowEntity.Component.ParentGroupId
}

func isNameChanged(flow *v1alpha1.NifiDataflow, pgFlowEntity *nigoapi.ProcessGroupEntity) bool {
	return flow.Name != pgFlowEntity.Component.Name
}

// isVersionSync check if the flow version is out of sync.
func isVersionSync(flow *v1alpha1.NifiDataflow, pgFlowEntity *nigoapi.ProcessGroupEntity) bool {
	return *flow.Spec.FlowVersion == pgFlowEntity.Component.VersionControlInformation.Version
}

func localChanged(pgFlowEntity *nigoapi.ProcessGroupEntity) bool {
	return strings.Contains(pgFlowEntity.Component.VersionControlInformation.State, "LOCALLY_MODIFIED")
}

// isVersioningChanged check if the versioning configuration is out of sync on process group.
func isVersioningChanged(flow *v1alpha1.NifiDataflow, registry *v1alpha1.NifiRegistryClient, pgFlowEntity *nigoapi.ProcessGroupEntity) bool {

	return pgFlowEntity.Component.VersionControlInformation == nil ||
		flow.Spec.FlowId != pgFlowEntity.Component.VersionControlInformation.FlowId ||
		flow.Spec.BucketId != pgFlowEntity.Component.VersionControlInformation.BucketId ||
		registry.Status.Id != pgFlowEntity.Component.VersionControlInformation.RegistryId
}

// isPostionChanged check if the position of the process group is out of sync.
func isPostionChanged(flow *v1alpha1.NifiDataflow, pgFlowEntity *nigoapi.ProcessGroupEntity) bool {
	return flow.Spec.FlowPosition != nil &&
		((flow.Spec.FlowPosition.X != nil && float64(flow.Spec.FlowPosition.GetX()) != pgFlowEntity.Component.Position.X) ||
			(flow.Spec.FlowPosition.Y != nil && float64(flow.Spec.FlowPosition.GetY()) != pgFlowEntity.Component.Position.Y))
}

// SyncDataflow implements the logic to sync a NifiDataflow with the deployed flow.
func SyncDataflow(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig, registry *v1alpha1.NifiRegistryClient, parameterContext *v1alpha1.NifiParameterContext) (*v1alpha1.NifiDataflowStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	pGEntities, err := nClient.GetProcessGroup(flow.Status.ProcessGroupID)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get process group"); err != nil {
		return nil, err
	}

	for _, pgEntityPair := range pGEntities {
		pGEntity := pgEntityPair.Entity
		// If the flow is not returned, then either it hasn't been fully created or nifikop doesn't yet have permission to view the PG
		if pGEntity == nil {
			return nil, errorfactory.NifiFlowSyncing{}
		}
		componentGroups, err := listComponents(config, flow.Status.ProcessGroupID, pgEntityPair.Client)
		if err != nil {
			return nil, err
		}
		// since we provided a client, there will be only 1 item in the returned list.
		processGroups := componentGroups[0].Entity.processGroups
		processGroups = append(processGroups, *pgEntityPair.Entity)

		if isParameterContextChanged(parameterContext, processGroups) {
			log.Info("Parameter context changed in flow. Unscheduling processors.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowName", flow.Name))
			// unschedule processors
			_, err := nClient.UpdateFlowProcessGroupWithClient(nigoapi.ScheduleComponentsEntity{
				Id:    flow.Status.ProcessGroupID,
				State: "STOPPED",
			}, pgEntityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Stop flow"); err != nil {
				return nil, err
			}

			for _, pg := range processGroups {
				if parameterContext == nil {
					pg.Component.ParameterContext = &nigoapi.ParameterContextReferenceEntity{}
				} else {
					pg.Component.ParameterContext = &nigoapi.ParameterContextReferenceEntity{
						Id: parameterContext.Status.Id,
					}
				}
				_, err := nClient.UpdateProcessGroupWithClient(pg, pgEntityPair.Client)
				if err := clientwrappers.ErrorUpdateOperation(log, err, "Set parameter-context"); err != nil {
					return nil, err
				}
			}
			return &flow.Status, errorfactory.NifiFlowSyncing{}
		}

		if isVersioningChanged(flow, registry, pGEntity) {
			log.Info("Versioning changed in flow. Removing old flow.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowName", flow.Name))
			return RemoveDataflowWithClient(pgEntityPair.Client, flow, config)
		}

		if isNameChanged(flow, pGEntity) || isPostionChanged(flow, pGEntity) {
			log.Info("Name and/or position of flow changed. Updating.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("oldFlowName", pGEntity.Component.Name),
				zap.String("newFlowName", flow.Name))
			pGEntity.Component.ParentGroupId = flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId)
			pGEntity.Component.Name = flow.Name

			var xPos, yPos float64
			if flow.Spec.FlowPosition == nil || flow.Spec.FlowPosition.X == nil {
				xPos = pGEntity.Component.Position.X
			} else {
				xPos = float64(flow.Spec.FlowPosition.GetX())
			}

			if flow.Spec.FlowPosition == nil || flow.Spec.FlowPosition.Y == nil {
				yPos = pGEntity.Component.Position.Y
			} else {
				yPos = float64(flow.Spec.FlowPosition.GetY())
			}

			pGEntity.Component.Position = &nigoapi.PositionDto{
				X: xPos,
				Y: yPos,
			}
			response, err := nClient.UpdateProcessGroupWithClient(*pGEntity, pgEntityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Stop flow"); err != nil {
				return nil, err
			}
			log.Info("After update",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("oldId", flow.Status.ProcessGroupID),
				zap.String("newId", response.Entity.Id),
			)
			return &flow.Status, errorfactory.NifiFlowSyncing{}
		}

		if isParentProcessGroupChanged(flow, config, pGEntity, pgEntityPair.Client) {
			log.Info("Parent process group of flow changed. Updating.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("oldParentId", flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId)),
				zap.String("newParentId", pGEntity.Component.ParentGroupId),
				zap.String("flowName", flow.Name))

			snippet, err := nClient.CreateSnippetWithClient(nigoapi.SnippetEntity{
				Snippet: &nigoapi.SnippetDto{
					ParentGroupId: pGEntity.Component.ParentGroupId,
					ProcessGroups: map[string]nigoapi.RevisionDto{pGEntity.Id: *pGEntity.Revision},
				},
			}, pgEntityPair.Client)
			if err := clientwrappers.ErrorCreateOperation(log, err, "Create snippet"); err != nil {
				return nil, err
			}

			_, err = nClient.UpdateSnippetWithClient(nigoapi.SnippetEntity{
				Snippet: &nigoapi.SnippetDto{
					Id:            snippet.Entity.Snippet.Id,
					ParentGroupId: flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId),
				},
			}, pgEntityPair.Client)
			if err := clientwrappers.ErrorUpdateOperation(log, err, "Update snippet"); err != nil {
				return nil, err
			}
			return &flow.Status, errorfactory.NifiFlowSyncing{}
		}

		latestUpdateRequest := flow.Status.LatestUpdateRequest
		if latestUpdateRequest != nil && !latestUpdateRequest.Complete {
			log.Info("Update request currently in progress. Requeuing.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("requestId", latestUpdateRequest.Id),
				zap.String("requestIdSeed", latestUpdateRequest.IdSeed),
				zap.String("flowName", flow.Name))
			var t v1alpha1.DataflowUpdateRequestType
			var err error
			var updateRequest *nificlient.ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]
			if latestUpdateRequest.Type == v1alpha1.UpdateRequestType {
				t = v1alpha1.UpdateRequestType
				updateRequest, err = nClient.GetVersionUpdateRequestWithClient(latestUpdateRequest.Id, pgEntityPair.Client)
			} else {
				t = v1alpha1.RevertRequestType
				updateRequest, err = nClient.GetVersionRevertRequestWithClient(latestUpdateRequest.Id, pgEntityPair.Client)
			}
			if updateRequest.Entity != nil {
				flow.Status.LatestUpdateRequest = updateRequest2Status(updateRequest.Entity, t, config, latestUpdateRequest.IdSeed)
			}

			if err := clientwrappers.ErrorGetOperation(log, err, "Get version-request"); err != nificlient.ErrNifiClusterReturned404 ||
				(updateRequest.Entity != nil && updateRequest.Entity.Request != nil && !updateRequest.Entity.Request.Complete) {
				if err != nil {
					return &flow.Status, err
				}
				return &flow.Status, errorfactory.NifiFlowUpdateRequestRunning{}
			}
		}

		isOutOfSync, err := IsOutOfSyncDataflowWithClient(pgEntityPair.Client, flow, config, registry, parameterContext)
		if err != nil {
			return &flow.Status, err
		}
		if isOutOfSync {
			log.Info("Flow is out of sync. Preparing update.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowName", flow.Name))
			status, err := prepareUpdatePG(pgEntityPair.Client, flow, config)
			if err != nil {
				return status, err
			}
			flow.Status = *status

			if err := UnscheduleDataflow(pgEntityPair.Client, flow, config); err != nil {
				return &flow.Status, err
			}
		}

		updatedPGEntity, err := nClient.GetProcessGroupWithClient(flow.Status.ProcessGroupID, pgEntityPair.Client)
		if err := clientwrappers.ErrorGetOperation(log, err, "Get process group"); err != nil {
			return nil, err
		}

		pGEntity = updatedPGEntity.Entity
		if localChanged(pGEntity) {
			log.Info("Flow has changed locally. Reverting to versioned flow.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowId", flow.Spec.FlowId),
				zap.String("flowName", flow.Name))
			vInfo := pGEntity.Component.VersionControlInformation
			status, err := createVersionRevertRequest(
				flow.Status,
				nigoapi.VersionControlInformationEntity{
					ProcessGroupRevision: pGEntity.Revision,
					VersionControlInformation: &nigoapi.VersionControlInformationDto{
						GroupId:    pGEntity.Id,
						RegistryId: vInfo.RegistryId,
						BucketId:   vInfo.BucketId,
						FlowId:     vInfo.FlowId,
						Version:    vInfo.Version,
					},
				},
				pgEntityPair.Client,
				config,
			)
			if err != nil {
				return nil, err
			}

			flow.Status.LatestUpdateRequest = status
			return &flow.Status, errorfactory.NifiFlowUpdateRequestRunning{}
		}

		if !isVersionSync(flow, pGEntity) {
			log.Info("Flow version has changed. Updating.",
				zap.Int32("nodeId", pgEntityPair.Client.NodeId),
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("flowId", flow.Spec.FlowId),
				zap.String("flowName", flow.Name))
			status, err := createVersionUpdateRequest(
				flow.Status,
				nigoapi.VersionControlInformationEntity{
					ProcessGroupRevision: pGEntity.Revision,
					VersionControlInformation: &nigoapi.VersionControlInformationDto{
						GroupId:    pGEntity.Id,
						RegistryId: registry.Status.Id,
						BucketId:   flow.Spec.BucketId,
						FlowId:     flow.Spec.FlowId,
						Version:    *flow.Spec.FlowVersion,
					},
				},
				pgEntityPair.Client,
				config,
			)
			if err != nil {
				return nil, err
			}

			flow.Status.LatestUpdateRequest = status
			return &flow.Status, errorfactory.NifiFlowUpdateRequestRunning{}
		}
		log.Info("Successfully synced flow on node.",
			zap.Int32("nodeId", pgEntityPair.Client.NodeId),
			zap.String("clusterName", flow.Spec.ClusterRef.Name),
			zap.String("flowId", flow.Spec.FlowId),
			zap.String("flowName", flow.Name))
	}

	log.Info("Successfully synced flow for whole cluster.",
		zap.String("clusterName", flow.Spec.ClusterRef.Name),
		zap.String("flowId", flow.Spec.FlowId),
		zap.String("flowName", flow.Name))

	return &flow.Status, nil
}

func createVersionRevertRequest(flowStatus v1alpha1.NifiDataflowStatus, entity nigoapi.VersionControlInformationEntity, client nificlient.ClientContextPair, config *clientconfig.NifiConfig) (*v1alpha1.UpdateRequest, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	// set the update request seed header appropriately
	seed := uuid.NewString()
	client.Client.AddDefaultHeader(clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER, seed)

	updateRequest, err := nClient.CreateVersionRevertRequestWithClient(flowStatus.ProcessGroupID, entity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create version revert-request"); err != nil {
		return nil, err
	}

	return updateRequest2Status(updateRequest.Entity, v1alpha1.RevertRequestType, config, seed), nil
}

func createVersionUpdateRequest(flowStatus v1alpha1.NifiDataflowStatus, entity nigoapi.VersionControlInformationEntity, client nificlient.ClientContextPair, config *clientconfig.NifiConfig) (*v1alpha1.UpdateRequest, error) {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	// set the update request seed header appropriately
	seed := uuid.NewString()
	client.Client.AddDefaultHeader(clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER, seed)

	updateRequest, err := nClient.CreateVersionUpdateRequestWithClient(flowStatus.ProcessGroupID, entity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Create version update-request"); err != nil {
		return nil, err
	}

	return updateRequest2Status(updateRequest.Entity, v1alpha1.UpdateRequestType, config, seed), nil
}

// prepareUpdatePG ensure drain or drop logic
func prepareUpdatePG(client nificlient.ClientContextPair, flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig) (*v1alpha1.NifiDataflowStatus, error) {

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if flow.Spec.UpdateStrategy == v1alpha1.DropStrategy {
		log.Info("Flow update strategy is drop. Stopping process group & dropping FlowFiles.",
			zap.String("clusterName", flow.Spec.ClusterRef.Name),
			zap.String("flowName", flow.Name))
		// unschedule processors
		_, err := nClient.UpdateFlowProcessGroupWithClient(nigoapi.ScheduleComponentsEntity{
			Id:    flow.Status.ProcessGroupID,
			State: "STOPPED",
		}, client)
		if err := clientwrappers.ErrorUpdateOperation(log, err, "Stop flow"); err != nil {
			return nil, err
		}

		if flow.Status.LatestDropRequest != nil && !flow.Status.LatestDropRequest.Finished {
			log.Info("Drop request currently in progress. Requeuing.",
				zap.String("clusterName", flow.Spec.ClusterRef.Name),
				zap.String("requestId", flow.Status.LatestDropRequest.Id),
				zap.String("requestIdSeed", flow.Status.LatestDropRequest.IdSeed),
				zap.String("flowName", flow.Name))

			dropRequest, err :=
				nClient.GetDropRequestWithClient(flow.Status.LatestDropRequest.ConnectionId, flow.Status.LatestDropRequest.Id, client)
			if err := clientwrappers.ErrorGetOperation(log, err, "Get drop-request"); err != nificlient.ErrNifiClusterReturned404 {
				if err != nil {
					return nil, err
				}

				flow.Status.LatestDropRequest =
					dropRequest2Status(flow.Status.LatestDropRequest.ConnectionId, dropRequest.Entity, config, flow.Status.LatestDropRequest.IdSeed)
				if !dropRequest.Entity.DropRequest.Finished {
					return &flow.Status, errorfactory.NifiConnectionDropping{}
				}
			}
		}

		// Drop all events in connections
		componentGroups, err := listComponents(config, flow.Status.ProcessGroupID, client)
		if err := clientwrappers.ErrorGetOperation(log, err, "Get recursively flow components"); err != nil {
			return nil, err
		}
		// since we provided a client, there will be only 1 item in the returned list.
		connections := componentGroups[0].Entity.connections

		for _, connection := range connections {
			if connection.Status.AggregateSnapshot.FlowFilesQueued != 0 {
				log.Info("Dropping flowFiles from connection",
					zap.String("clusterName", flow.Spec.ClusterRef.Name),
					zap.String("requestId", flow.Status.LatestDropRequest.Id),
					zap.String("requestIdSeed", flow.Status.LatestDropRequest.IdSeed),
					zap.Int32("numFlowFiles", connection.Status.AggregateSnapshot.FlowFilesQueued),
					zap.String("connectionId", connection.Id),
					zap.String("flowName", flow.Name))
				// set the update request seed header appropriately
				seed := uuid.NewString()
				client.Client.AddDefaultHeader(clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER, seed)
				dropRequest, err := nClient.CreateDropRequestWithClient(connection.Id, client)
				if err := clientwrappers.ErrorCreateOperation(log, err, "Create drop-request"); err != nil {
					return nil, err
				}

				flow.Status.LatestDropRequest =
					dropRequest2Status(flow.Status.LatestDropRequest.ConnectionId, dropRequest.Entity, config, seed)

				return &flow.Status, errorfactory.NifiConnectionDropping{}
			}
		}
	} else {
		log.Info("Flow update strategy is drain. Waiting to ensure all connections have drained...",
			zap.String("clusterName", flow.Spec.ClusterRef.Name),
			zap.String("flowName", flow.Name))

		// Check all components are ok
		flowEntity, err := nClient.GetFlowWithClient(flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId), client)
		if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
			return nil, err
		}

		pgEntity := processGroupFromFlow(flowEntity.Entity, flow)
		if pgEntity == nil {
			return nil, errorfactory.NifiFlowDraining{}
		}

		// If flow is not fully drained
		if pgEntity.Status.AggregateSnapshot.FlowFilesQueued != 0 {
			componentGroups, err := listComponents(config, flow.Status.ProcessGroupID, client)
			if err := clientwrappers.ErrorGetOperation(log, err, "Get recursively flow components"); err != nil {
				return nil, err
			}
			// since we provided a client, there will only be 1 item in the returned list.
			connections := componentGroups[0].Entity.connections
			processors := componentGroups[0].Entity.processors
			inputPorts := componentGroups[0].Entity.ports

			// list input port
			for _, connection := range connections {
				processors = removeProcessor(processors, connection.DestinationId)
			}

			// Stop all input processor
			for _, processor := range processors {
				if processor.Status.RunStatus == "Running" {
					log.Info("Stopping input processor.",
						zap.String("clusterName", flow.Spec.ClusterRef.Name),
						zap.String("processorId", processor.Id),
						zap.String("flowName", flow.Name))
					_, err := nClient.UpdateProcessorRunStatusWithClient(processor.Id, nigoapi.ProcessorRunStatusEntity{
						Revision: processor.Revision,
						State:    "STOPPED",
					}, client)
					if err := clientwrappers.ErrorUpdateOperation(log, err, "Stop processor"); err != nil {
						return nil, err
					}
				}
			}

			// Stop all input remote
			for _, inputPort := range inputPorts {
				if inputPort.AllowRemoteAccess && inputPort.Status.RunStatus == "Running" {
					log.Info("Stopping input port.",
						zap.String("clusterName", flow.Spec.ClusterRef.Name),
						zap.String("inputPortId", inputPort.Id),
						zap.String("flowName", flow.Name))
					_, err := nClient.UpdateInputPortRunStatusWithClient(inputPort.Id, nigoapi.PortRunStatusEntity{
						Revision: inputPort.Revision,
						State:    "STOPPED",
					}, client)
					if err := clientwrappers.ErrorUpdateOperation(log, err, "Stop remote input-port"); err != nil {
						return nil, err
					}
				}
			}
			return nil, errorfactory.NifiFlowDraining{}
		}
	}

	return &flow.Status, nil
}

func RemoveDataflow(flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig) (*v1alpha1.NifiDataflowStatus, error) {
	log.Info("Removing dataflow",
		zap.String("flowName", flow.Name),
		zap.String("flowId", flow.Spec.FlowId),
		zap.String("bucketId", flow.Spec.BucketId),
		zap.String("clusterName", flow.Spec.ClusterRef.Name))
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	flowEntities, err := nClient.GetFlow(flow.Status.ProcessGroupID)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil, nil
		}
		return &flow.Status, err
	}

	var status *v1alpha1.NifiDataflowStatus
	for _, flowEntity := range flowEntities {
		if flowEntity.Entity != nil {
			status, err = RemoveDataflowWithClient(flowEntity.Client, flow, config)
			if err != nil {
				return nil, err
			}
		}
	}
	return status, nil
}

func RemoveDataflowWithClient(client nificlient.ClientContextPair, flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig) (*v1alpha1.NifiDataflowStatus, error) {
	// Prepare Dataflow
	status, err := prepareUpdatePG(client, flow, config)
	if err != nil {
		return status, err
	}
	flow.Status = *status

	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if err := UnscheduleDataflow(client, flow, config); err != nil {
		return &flow.Status, err
	}

	pGEntity, err := nClient.GetProcessGroupWithClient(flow.Status.ProcessGroupID, client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		if err == nificlient.ErrNifiClusterReturned404 {
			return nil, nil
		}
		return &flow.Status, err
	}

	err = nClient.RemoveProcessGroupWithClient(*pGEntity.Entity, client)
	if err := clientwrappers.ErrorCreateOperation(log, err, "Remove process-group"); err != nil {
		return &flow.Status, err
	}

	return nil, nil
}

func UnscheduleDataflow(client nificlient.ClientContextPair, flow *v1alpha1.NifiDataflow, config *clientconfig.NifiConfig) error {
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return err
	}

	// UnSchedule flow
	_, err = nClient.UpdateFlowProcessGroupWithClient(nigoapi.ScheduleComponentsEntity{
		Id:    flow.Status.ProcessGroupID,
		State: "STOPPED",
	}, client)
	if err := clientwrappers.ErrorUpdateOperation(log, err, "Unschedule flow"); err != nil {
		return err
	}

	// Schedule controller services
	_, err = nClient.UpdateFlowControllerServicesWithClient(nigoapi.ActivateControllerServicesEntity{
		Id:    flow.Status.ProcessGroupID,
		State: "DISABLED",
	}, client)
	if err := clientwrappers.ErrorUpdateOperation(log, err, "Unschedule flow's controller services"); err != nil {
		return err
	}

	// Check all controller services are enabled
	csEntities, err := nClient.GetFlowControllerServicesWithClient(flow.Status.ProcessGroupID, client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow controller services"); err != nil {
		return err
	}
	for _, csEntity := range csEntities.Entity.ControllerServices {
		if csEntity.Status.RunStatus != "DISABLED" &&
			!(flow.Spec.SkipInvalidControllerService && csEntity.Status.ValidationStatus == "INVALID") {
			return errorfactory.NifiFlowControllerServiceScheduling{}
		}
	}

	// Check all components are ok
	flowEntity, err := nClient.GetFlowWithClient(flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId), client)
	if err := clientwrappers.ErrorGetOperation(log, err, "Get flow"); err != nil {
		return err
	}

	pgEntity := processGroupFromFlow(flowEntity.Entity, flow)
	if pgEntity == nil {
		return errorfactory.NifiFlowScheduling{}
	}

	if pgEntity.RunningCount > 0 {
		return errorfactory.NifiFlowScheduling{}
	}

	return nil
}

// processGroupFromFlow convert a ProcessGroupFlowEntity to NifiDataflow
func processGroupFromFlow(flowEntity *nigoapi.ProcessGroupFlowEntity, flow *v1alpha1.NifiDataflow) *nigoapi.ProcessGroupEntity {

	for _, entity := range flowEntity.ProcessGroupFlow.Flow.ProcessGroups {
		if entity.Id == flow.Status.ProcessGroupID {
			return &entity
		}
	}

	return nil
}

type entityGroup struct {
	processGroups []nigoapi.ProcessGroupEntity
	processors    []nigoapi.ProcessorEntity
	connections   []nigoapi.ConnectionEntity
	ports         []nigoapi.PortEntity
}

// listComponents will get all ProcessGroups, Processors, Connections and Ports recursively
// If a client is provided, then components will only be listed for that specific client. Otherwise, components are listed for the cluster.
func listComponents(config *clientconfig.NifiConfig, processGroupID string, client nificlient.ClientContextPair) ([]nificlient.ClientEntityPair[entityGroup], error) {
	var processGroups []nigoapi.ProcessGroupEntity
	var processors []nigoapi.ProcessorEntity
	var connections []nigoapi.ConnectionEntity
	var inputPorts []nigoapi.PortEntity
	allComponentGroups := []nificlient.ClientEntityPair[entityGroup]{}

	var flowEntities []*nificlient.ClientEntityPair[nigoapi.ProcessGroupFlowEntity]
	nClient, err := common.NewClusterConnection(log, config)
	if err != nil {
		return nil, err
	}

	if client.Client == nil {
		flowEntities, _ = nClient.GetFlow(processGroupID)
	} else {
		flowEntity, _ := nClient.GetFlowWithClient(processGroupID, client)
		flowEntities = []*nificlient.ClientEntityPair[nigoapi.ProcessGroupFlowEntity]{
			flowEntity,
		}
	}

	for _, flowEntity := range flowEntities {
		if flowEntity == nil || flowEntity.Entity == nil {
			continue
		}
		flow := flowEntity.Entity.ProcessGroupFlow.Flow

		processGroups = flow.ProcessGroups
		processors = flow.Processors
		connections = flow.Connections
		inputPorts = flow.InputPorts

		for _, pg := range flow.ProcessGroups {
			componentGroups, err := listComponents(config, pg.Id, flowEntity.Client)
			if err != nil {
				return allComponentGroups, err
			}

			// since we provided a client, there will be only 1 item in the returned list.
			entity := componentGroups[0].Entity
			processGroups = append(processGroups, entity.processGroups...)
			processors = append(processors, entity.processors...)
			connections = append(connections, entity.connections...)
			inputPorts = append(inputPorts, entity.ports...)
		}
		allComponentGroups = append(allComponentGroups, nificlient.ClientEntityPair[entityGroup]{
			Client: flowEntity.Client,
			Entity: &entityGroup{
				processGroups: processGroups,
				processors:    processors,
				connections:   connections,
				ports:         inputPorts,
			},
		})
	}

	return allComponentGroups, nil
}

func dropRequest2Status(connectionId string, dropRequest *nigoapi.DropRequestEntity, config *clientconfig.NifiConfig, idSeed string) *v1alpha1.DropRequest {
	dr := dropRequest.DropRequest
	var seed string
	if idSeed != "" {
		seed = idSeed
	} else {
		seed = config.GetIDSeedHeader()
	}
	return &v1alpha1.DropRequest{
		ConnectionId:     connectionId,
		Id:               dr.Id,
		IdSeed:           seed,
		Uri:              dr.Uri,
		LastUpdated:      dr.LastUpdated,
		Finished:         dr.Finished,
		FailureReason:    dr.FailureReason,
		PercentCompleted: dr.PercentCompleted,
		CurrentCount:     dr.CurrentCount,
		CurrentSize:      dr.CurrentSize,
		Current:          dr.Current,
		OriginalCount:    dr.OriginalCount,
		OriginalSize:     dr.OriginalSize,
		Original:         dr.Original,
		DroppedCount:     dr.DroppedCount,
		DroppedSize:      dr.DroppedSize,
		Dropped:          dr.Dropped,
		State:            dr.State,
	}
}

func updateRequest2Status(updateRequest *nigoapi.VersionedFlowUpdateRequestEntity,
	updateType v1alpha1.DataflowUpdateRequestType, config *clientconfig.NifiConfig, idSeed string) *v1alpha1.UpdateRequest {
	ur := updateRequest.Request
	var seed string
	if idSeed != "" {
		seed = idSeed
	} else {
		seed = config.GetIDSeedHeader()
	}
	return &v1alpha1.UpdateRequest{
		Type:             updateType,
		Id:               ur.RequestId,
		IdSeed:           seed,
		Uri:              ur.Uri,
		LastUpdated:      ur.LastUpdated,
		Complete:         ur.Complete,
		FailureReason:    ur.FailureReason,
		PercentCompleted: ur.PercentCompleted,
		State:            ur.State,
	}
}

func updateProcessGroupEntity(flow *v1alpha1.NifiDataflow, registry *v1alpha1.NifiRegistryClient, config *clientconfig.NifiConfig, entity *nigoapi.ProcessGroupEntity) {

	stringFactory := func() string { return "" }

	var defaultVersion int64 = 0
	if entity == nil {
		entity = &nigoapi.ProcessGroupEntity{}
	}

	if entity.Component == nil {
		entity.Revision = &nigoapi.RevisionDto{
			Version: &defaultVersion,
		}
	}

	if entity.Component == nil {
		entity.Component = &nigoapi.ProcessGroupDto{}
	}

	entity.Component.Name = flow.Name
	entity.Component.ParentGroupId = flow.Spec.GetParentProcessGroupID(config.RootProcessGroupId)

	var xPos, yPos float64
	if entity.Component.Position != nil {
		xPos = entity.Component.Position.X
		yPos = entity.Component.Position.Y
	} else {
		if flow.Spec.FlowPosition == nil || flow.Spec.FlowPosition.X == nil {
			xPos = float64(1)
		} else {
			xPos = float64(flow.Spec.FlowPosition.GetX())
		}

		if flow.Spec.FlowPosition == nil || flow.Spec.FlowPosition.Y == nil {
			yPos = float64(1)
		} else {
			yPos = float64(flow.Spec.FlowPosition.GetY())
		}
	}

	entity.Component.Position = &nigoapi.PositionDto{
		X: xPos,
		Y: yPos,
	}

	entity.Component.VersionControlInformation = &nigoapi.VersionControlInformationDto{
		GroupId:          stringFactory(),
		RegistryName:     stringFactory(),
		BucketName:       stringFactory(),
		FlowName:         stringFactory(),
		FlowDescription:  stringFactory(),
		State:            stringFactory(),
		StateExplanation: stringFactory(),
		RegistryId:       registry.Status.Id,
		BucketId:         flow.Spec.BucketId,
		FlowId:           flow.Spec.FlowId,
		Version:          *flow.Spec.FlowVersion,
	}
}

func removeProcessor(processors []nigoapi.ProcessorEntity, toRemoveId string) []nigoapi.ProcessorEntity {
	var tmp []nigoapi.ProcessorEntity

	for _, processor := range processors {
		if processor.Id != toRemoveId {
			tmp = append(tmp, processor)
		}
	}

	return tmp
}
