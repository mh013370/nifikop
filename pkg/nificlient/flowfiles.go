package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetDropRequest(connectionId, id string) ([]*ClientEntityPair[nigoapi.DropRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.DropRequestEntity]{}
	for _, clientPair := range clients {
		entityPair, err := n.GetDropRequestWithClient(connectionId, id, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetDropRequestWithClient(connectionId, id string, client ClientContextPair) (*ClientEntityPair[nigoapi.DropRequestEntity], error) {
	// Request on Nifi Rest API to get the drop request information
	dropRequest, rsp, body, err := client.Client.FlowfileQueuesApi.GetDropRequest(client.Context, connectionId, id)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.DropRequestEntity]{
		Client: client,
		Entity: &dropRequest,
	}, nil
}

func (n *nifiClient) CreateDropRequest(connectionId string) ([]*ClientEntityPair[nigoapi.DropRequestEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.DropRequestEntity]{}
	for _, clientPair := range clients {
		entityPair, err := n.CreateDropRequestWithClient(connectionId, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}
	return entities, nil
}

func (n *nifiClient) CreateDropRequestWithClient(connectionId string, client ClientContextPair) (*ClientEntityPair[nigoapi.DropRequestEntity], error) {
	// Request on Nifi Rest API to create the drop Request
	entity, rsp, body, err := client.Client.FlowfileQueuesApi.CreateDropRequest(client.Context, connectionId)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.DropRequestEntity]{
		Client: client,
		Entity: &entity,
	}, nil
}

// TODO : when last supported will be NiFi 1.12.X
//func (n *nifiClient) CreateDropRequest(pgId string)(*nigoapi.ProcessGroupEntity, error) {
//	// Get nigoapi client, favoring the one associated to the coordinator node.
//	client, context := n.privilegeCoordinatorClient()
//	if client == nil {
//		log.Error(ErrNoNodeClientsAvailable, "Error during creating node client")
//		return nil, ErrNoNodeClientsAvailable
//	}
//
//	// Request on Nifi Rest API to create the registry client
//	entity, rsp, err := client.ProcessGroupsApi.CreateEmptyAllConnectionsRequest(context, pgId)
//	if err := errorCreateOperation(rsp, err); err != nil {
//		return nil, err
//	}
//
//	return &entity, nil
//}
