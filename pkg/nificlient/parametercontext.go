package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetParameterContexts() ([]*ClientEntityPair[[]nigoapi.ParameterContextEntity], error) {
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[[]nigoapi.ParameterContextEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the parameter contexts informations
		pcEntity, rsp, body, err := client.Client.FlowApi.GetParameterContexts(client.Context)
		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			return nil, err
		}
		entities = append(entities, &ClientEntityPair[[]nigoapi.ParameterContextEntity]{
			Client: client,
			Entity: &pcEntity.ParameterContexts,
		})
	}

	return entities, nil
}

func (n *nifiClient) GetParameterContext(id string) ([]*ClientEntityPair[nigoapi.ParameterContextEntity], error) {
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ParameterContextEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the parameter context informations
		entityPair, err := n.GetParameterContextWithClient(id, clientPair)
		if err != nil {
			n.log.Warn("Unable to locate parameter context at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", clientPair.NodeId),
				zap.String("parameterContextId", id))
			entityPair = &ClientEntityPair[nigoapi.ParameterContextEntity]{
				Client: clientPair,
			}
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetParameterContextWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextEntity], error) {

	// Request on Nifi Rest API to get the parameter context informations
	pcEntity, rsp, body, err := client.Client.ParameterContextsApi.GetParameterContext(client.Context, id, nil)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	var ret *nigoapi.ParameterContextEntity
	if pcEntity.Id == "" {
		ret = nil
	} else {
		ret = &pcEntity
	}

	return &ClientEntityPair[nigoapi.ParameterContextEntity]{
		Client: client,
		Entity: ret,
	}, nil
}

func (n *nifiClient) CreateParameterContext(entity nigoapi.ParameterContextEntity) ([]*ClientEntityPair[nigoapi.ParameterContextEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ParameterContextEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to create the parameter context
		entityPair, err := n.CreateParameterContextWithClient(entity, client)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateParameterContextWithClient(entity nigoapi.ParameterContextEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextEntity], error) {
	// Request on Nifi Rest API to create the parameter context
	pcEntity, rsp, body, err := client.Client.ParameterContextsApi.CreateParameterContext(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ParameterContextEntity]{
		Client: client,
		Entity: &pcEntity,
	}, nil
}

func (n *nifiClient) RemoveParameterContext(entity nigoapi.ParameterContextEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		// Request on Nifi Rest API to remove the parameter context
		err := n.RemoveParameterContextWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *nifiClient) RemoveParameterContextWithClient(entity nigoapi.ParameterContextEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the parameter context
	_, rsp, body, err := client.Client.ParameterContextsApi.DeleteParameterContext(client.Context, entity.Id,
		&nigoapi.ParameterContextsApiDeleteParameterContextOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}

func (n *nifiClient) CreateParameterContextUpdateRequest(contextId string, entity nigoapi.ParameterContextEntity) ([]*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the parameter context update request
		entityPair, err := n.CreateParameterContextUpdateRequestWithClient(contextId, entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateParameterContextUpdateRequestWithClient(contextId string, entity nigoapi.ParameterContextEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error) {
	// Request on Nifi Rest API to create the parameter context update request
	request, rsp, body, err := client.Client.ParameterContextsApi.SubmitParameterContextUpdate(client.Context, contextId, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity]{
		Client: client,
		Entity: &request,
	}, nil
}

func (n *nifiClient) GetParameterContextUpdateRequest(contextId, id string) ([]*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the parameter context update request information
		entityPair, err := n.GetParameterContextUpdateRequestWithClient(contextId, id, client)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetParameterContextUpdateRequestWithClient(contextId, id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error) {
	// Request on Nifi Rest API to get the parameter context update request information
	request, rsp, body, err := client.Client.ParameterContextsApi.GetParameterContextUpdate(client.Context, contextId, id)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}
	var ret *nigoapi.ParameterContextUpdateRequestEntity
	if request.ParameterContextRevision == nil || request.ParameterContextRevision.Version == nil {
		ret = nil
	} else {
		ret = &request
	}
	return &ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity]{
		Client: client,
		Entity: ret,
	}, nil
}
