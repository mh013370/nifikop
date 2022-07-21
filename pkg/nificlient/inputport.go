package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) UpdateInputPortRunStatus(id string, entity nigoapi.PortRunStatusEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessorEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the input port run status
		entityPair, err := n.UpdateInputPortRunStatusWithClient(id, entity, clientPair)
		if err != nil {
			return nil, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateInputPortRunStatusWithClient(id string, entity nigoapi.PortRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error) {
	// Request on Nifi Rest API to update the input port run status
	processor, rsp, body, err := client.Client.InputPortsApi.UpdateRunStatus(client.Context, id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ProcessorEntity]{
		Client: client,
		Entity: &processor,
	}, nil
}
