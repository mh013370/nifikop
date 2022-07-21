package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) UpdateProcessor(entity nigoapi.ProcessorEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessorEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the processor
		entityPair, err := n.UpdateProcessorWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateProcessorWithClient(entity nigoapi.ProcessorEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error) {
	// Request on Nifi Rest API to update the processor
	processor, rsp, body, err := client.Client.ProcessorsApi.UpdateProcessor(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ProcessorEntity]{
		Client: client,
		Entity: &processor,
	}, nil
}

func (n *nifiClient) UpdateProcessorRunStatus(id string, entity nigoapi.ProcessorRunStatusEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error) {

	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessorEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the processor run status
		entityPair, err := n.UpdateProcessorRunStatusWithClient(id, entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateProcessorRunStatusWithClient(id string, entity nigoapi.ProcessorRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error) {

	// Request on Nifi Rest API to update the processor run status
	processor, rsp, body, err := client.Client.ProcessorsApi.UpdateRunStatus(client.Context, id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ProcessorEntity]{
		Client: client,
		Entity: &processor,
	}, nil
}
