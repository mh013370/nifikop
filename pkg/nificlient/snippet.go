package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) CreateSnippet(entity nigoapi.SnippetEntity) ([]*ClientEntityPair[nigoapi.SnippetEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.SnippetEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the snippet
		entityPair, err := n.CreateSnippetWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateSnippetWithClient(entity nigoapi.SnippetEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.SnippetEntity], error) {
	// Request on Nifi Rest API to create the snippet
	snippetEntity, rsp, body, err := client.Client.SnippetsApi.CreateSnippet(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.SnippetEntity]{
		Client: client,
		Entity: &snippetEntity,
	}, nil
}

func (n *nifiClient) UpdateSnippet(entity nigoapi.SnippetEntity) ([]*ClientEntityPair[nigoapi.SnippetEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.SnippetEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the snippet
		entityPair, err := n.UpdateSnippetWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateSnippetWithClient(entity nigoapi.SnippetEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.SnippetEntity], error) {
	// Request on Nifi Rest API to update the snippet
	snippetEntity, rsp, body, err := client.Client.SnippetsApi.UpdateSnippet(client.Context, entity.Snippet.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.SnippetEntity]{
		Client: client,
		Entity: &snippetEntity,
	}, nil
}
