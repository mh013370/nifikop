package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetProcessGroup(id string) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessGroupEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to get the process group informations
		entityPair, err := n.GetProcessGroupWithClient(id, clientPair)
		if err != nil {
			n.log.Warn("Unable to locate process group at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", clientPair.NodeId),
				zap.String("processGroupId", id))
			entityPair = &ClientEntityPair[nigoapi.ProcessGroupEntity]{
				Client: clientPair,
			}
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetProcessGroupWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {
	// Request on Nifi Rest API to get the process group informations
	pGEntity, rsp, body, err := client.Client.ProcessGroupsApi.GetProcessGroup(client.Context, id)
	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}
	var ret *nigoapi.ProcessGroupEntity
	if pGEntity.Id == "" {
		ret = nil
	} else {
		ret = &pGEntity
	}

	return &ClientEntityPair[nigoapi.ProcessGroupEntity]{
		Client: client,
		Entity: ret,
	}, nil
}

func (n *nifiClient) CreateProcessGroup(entity nigoapi.ProcessGroupEntity, pgParentId string) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessGroupEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the versioned process group
		entityPair, err := n.CreateProcessGroupWithClient(entity, pgParentId, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, pgParentId string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {
	// Request on Nifi Rest API to create the versioned process group
	pgEntity, rsp, body, err := client.Client.ProcessGroupsApi.CreateProcessGroup(client.Context, pgParentId, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ProcessGroupEntity]{
		Client: client,
		Entity: &pgEntity,
	}, nil
}

func (n *nifiClient) UpdateProcessGroup(entity nigoapi.ProcessGroupEntity) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ProcessGroupEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the versioned process group
		entityPair, err := n.UpdateProcessGroupWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

// Updates a process group with a specifically provided client
func (n *nifiClient) UpdateProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error) {

	// Request on Nifi Rest API to update the versioned process group
	pgEntity, rsp, body, err := client.Client.ProcessGroupsApi.UpdateProcessGroup(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ProcessGroupEntity]{
		Client: client,
		Entity: &pgEntity,
	}, nil
}

func (n *nifiClient) RemoveProcessGroup(entity nigoapi.ProcessGroupEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		err := n.RemoveProcessGroupWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *nifiClient) RemoveProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the versioned process group
	_, rsp, body, err := client.Client.ProcessGroupsApi.RemoveProcessGroup(
		client.Context,
		entity.Id,
		&nigoapi.ProcessGroupsApiRemoveProcessGroupOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
