package nificlient

import (
	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetFlow(id string) ([]*ClientEntityPair[nigoapi.ProcessGroupFlowEntity], error) {
	// Get nigoapi clients
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client",
			zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessGroupFlowEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the process group flow informations
		entityPair, err := n.GetFlowWithClient(id, clientPair)
		if err != nil {
			n.log.Warn("Unable to locate flow at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", entityPair.Client.NodeId),
				zap.String("flowId", id))
			entityPair = &ClientEntityPair[nigoapi.ProcessGroupFlowEntity]{
				Client: clientPair,
			}
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetFlowWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupFlowEntity], error) {
	// Request on Nifi Rest API to get the process group flow informations
	flowEntity, rsp, body, err := client.Client.FlowApi.GetFlow(client.Context, id, nil)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	var ret *nigoapi.ProcessGroupFlowEntity
	if flowEntity.ProcessGroupFlow == nil || flowEntity.ProcessGroupFlow.Id == "" {
		ret = nil
	} else {
		ret = &flowEntity
	}

	return &ClientEntityPair[nigoapi.ProcessGroupFlowEntity]{
		Client: client,
		Entity: ret,
	}, nil
}

func (n *nifiClient) GetFlowControllerServices(id string) ([]*ClientEntityPair[nigoapi.ControllerServicesEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ControllerServicesEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the process group flow's controller services informations
		entityPair, err := n.GetFlowControllerServicesWithClient(id, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetFlowControllerServicesWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ControllerServicesEntity], error) {
	// Request on Nifi Rest API to get the process group flow's controller services informations
	csEntity, rsp, body, err := client.Client.FlowApi.GetControllerServicesFromGroup(client.Context, id,
		&nigoapi.FlowApiGetControllerServicesFromGroupOpts{
			IncludeAncestorGroups:   optional.NewBool(false),
			IncludeDescendantGroups: optional.NewBool(true),
		})

	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}
	return &ClientEntityPair[nigoapi.ControllerServicesEntity]{
		Client: client,
		Entity: &csEntity,
	}, nil
}

func (n *nifiClient) UpdateFlowControllerServices(entity nigoapi.ActivateControllerServicesEntity) ([]*ClientEntityPair[nigoapi.ActivateControllerServicesEntity], error) {

	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client",
			zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ActivateControllerServicesEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to enable or disable the controller services
		entityPair, err := n.UpdateFlowControllerServicesWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateFlowControllerServicesWithClient(entity nigoapi.ActivateControllerServicesEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ActivateControllerServicesEntity], error) {
	// Request on Nifi Rest API to enable or disable the controller services
	csEntity, rsp, body, err := client.Client.FlowApi.ActivateControllerServices(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}
	return &ClientEntityPair[nigoapi.ActivateControllerServicesEntity]{
		Client: client,
		Entity: &csEntity,
	}, nil
}

func (n *nifiClient) UpdateFlowProcessGroup(entity nigoapi.ScheduleComponentsEntity) ([]*ClientEntityPair[nigoapi.ScheduleComponentsEntity], error) {

	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client",
			zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ScheduleComponentsEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to enable or disable the controller services
		entityPair, err := n.UpdateFlowProcessGroupWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateFlowProcessGroupWithClient(entity nigoapi.ScheduleComponentsEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ScheduleComponentsEntity], error) {
	// Request on Nifi Rest API to enable or disable the controller services
	csEntity, rsp, body, err := client.Client.FlowApi.ScheduleComponents(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ScheduleComponentsEntity]{
		Client: client,
		Entity: &csEntity,
	}, nil
}

// TODO : when last supported will be NiFi 1.12.X
//func (n *nifiClient) FlowDropRequest(connectionId, id string) (*nigoapi.DropRequestEntity, error) {
//	// Get nigoapi client, favoring the one associated to the coordinator node.
//	client, context := n.privilegeCoordinatorClient()
//	if client == nil {
//		log.Error(ErrNoNodeClientsAvailable, "Error during creating node client")
//		return nil, ErrNoNodeClientsAvailable
//	}
//
//	// Request on Nifi Rest API to get the drop request information
//	dropRequest, rsp, err := client.FlowfileQueuesApi.GetDropRequest(context, connectionId, id)
//	if err := errorGetOperation(rsp, err); err != nil {
//		return nil, err
//	}
//
//	return &dropRequest, nil
//}
