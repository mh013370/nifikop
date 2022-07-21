package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetRegistryClient(id string) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.RegistryClientEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the registy client informations
		regCliEntity, rsp, body, err := client.Client.ControllerApi.GetRegistryClient(client.Context, id)

		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			n.log.Warn("Unable to locate registry client at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", client.NodeId),
				zap.String("registryClient", id),
				zap.Any("responseEntity", regCliEntity))
		}
		var ret *nigoapi.RegistryClientEntity
		if regCliEntity.Id == "" {
			ret = nil
		} else {
			ret = &regCliEntity
		}
		entities = append(entities, &ClientEntityPair[nigoapi.RegistryClientEntity]{
			Client: client,
			Entity: ret,
		})
	}

	return entities, nil
}

func (n *nifiClient) CreateRegistryClient(entity nigoapi.RegistryClientEntity) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.RegistryClientEntity]{}
	for _, clientPair := range clients {
		entityPair, err := n.CreateRegistryClientWithClient(entity, ClientContextPair{
			Client:  clientPair.Client,
			Context: clientPair.Context,
		})
		if err != nil {
			return nil, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.RegistryClientEntity], error) {
	// Request on Nifi Rest API to create the registry client
	regCliEntity, rsp, body, err := client.Client.ControllerApi.CreateRegistryClient(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.RegistryClientEntity]{
		Client: client,
		Entity: &regCliEntity,
	}, nil
}

func (n *nifiClient) UpdateRegistryClient(entity nigoapi.RegistryClientEntity) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.RegistryClientEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to update the registry client
		entityPair, err := n.UpdateRegistryClientWithClient(entity, client)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.RegistryClientEntity], error) {
	// Request on Nifi Rest API to update the registry client
	regCliEntity, rsp, body, err := client.Client.ControllerApi.UpdateRegistryClient(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.RegistryClientEntity]{
		Client: client,
		Entity: &regCliEntity,
	}, nil
}

func (n *nifiClient) RemoveRegistryClient(entity nigoapi.RegistryClientEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		err := n.RemoveRegistryClientWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}
	return nil
}

func (n *nifiClient) RemoveRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the registry client
	_, rsp, body, err := client.Client.ControllerApi.DeleteRegistryClient(client.Context, entity.Id,
		&nigoapi.ControllerApiDeleteRegistryClientOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
