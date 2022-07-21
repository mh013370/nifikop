package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) CreateVersionUpdateRequest(pgId string, entity nigoapi.VersionControlInformationEntity) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to create the version update request
		entityPair, err := n.CreateVersionUpdateRequestWithClient(pgId, entity, client)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateVersionUpdateRequestWithClient(pgId string, entity nigoapi.VersionControlInformationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Request on Nifi Rest API to create the version update request
	request, rsp, body, err := client.Client.VersionsApi.InitiateVersionControlUpdate(client.Context, pgId, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{
		Client: client,
		Entity: &request,
	}, nil
}

func (n *nifiClient) GetVersionUpdateRequest(id string) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the update request information
		entityPair, err := n.GetVersionUpdateRequestWithClient(id, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetVersionUpdateRequestWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Request on Nifi Rest API to get the update request information
	request, rsp, body, err := client.Client.VersionsApi.GetUpdateRequest(client.Context, id)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	var ret *nigoapi.VersionedFlowUpdateRequestEntity
	if request.Request == nil || request.Request.RequestId == "" {
		ret = nil
	} else {
		ret = &request
	}
	return &ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{
		Client: client,
		Entity: ret,
	}, nil
}

func (n *nifiClient) CreateVersionRevertRequest(pgId string, entity nigoapi.VersionControlInformationEntity) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the version revert request
		entityPair, err := n.CreateVersionRevertRequestWithClient(pgId, entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateVersionRevertRequestWithClient(pgId string, entity nigoapi.VersionControlInformationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Request on Nifi Rest API to create the version revert request
	request, rsp, body, err := client.Client.VersionsApi.InitiateRevertFlowVersion(client.Context, pgId, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{
		Client: client,
		Entity: &request,
	}, nil
}

func (n *nifiClient) GetVersionRevertRequest(id string) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the revert request information
		entityPair, err := n.GetVersionRevertRequestWithClient(id, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetVersionRevertRequestWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error) {
	// Request on Nifi Rest API to get the revert request information
	request, rsp, body, err := client.Client.VersionsApi.GetRevertRequest(client.Context, id)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	var ret *nigoapi.VersionedFlowUpdateRequestEntity
	if request.Request == nil || request.Request.RequestId == "" {
		ret = nil
	} else {
		ret = &request
	}
	return &ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity]{
		Client: client,
		Entity: ret,
	}, nil
}
