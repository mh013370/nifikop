package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetUsers() ([]*ClientEntityPair[[]nigoapi.UserEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[[]nigoapi.UserEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the users informations
		entityPair, err := n.GetUsersWithClient(client)

		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetUsersWithClient(client ClientContextPair) (*ClientEntityPair[[]nigoapi.UserEntity], error) {
	// Request on Nifi Rest API to get the users informations
	usersEntity, rsp, body, err := client.Client.TenantsApi.GetUsers(client.Context)

	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[[]nigoapi.UserEntity]{
		Client: client,
		Entity: &usersEntity.Users,
	}, nil
}

func (n *nifiClient) GetUser(id string) ([]*ClientEntityPair[nigoapi.UserEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the user informations
		userEntity, rsp, body, err := client.Client.TenantsApi.GetUser(client.Context, id)

		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			n.log.Warn("Unable to locate user at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", client.NodeId),
				zap.String("userId", id),
				zap.Any("responseEntity", userEntity))
		}
		var ret *nigoapi.UserEntity
		if userEntity.Id == "" {
			ret = nil
		} else {
			ret = &userEntity
		}
		entities = append(entities, &ClientEntityPair[nigoapi.UserEntity]{
			Client: client,
			Entity: ret,
		})
	}

	return entities, nil
}

func (n *nifiClient) CreateUser(entity nigoapi.UserEntity) ([]*ClientEntityPair[nigoapi.UserEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the user
		entityPair, err := n.CreateUserWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserEntity], error) {
	// Request on Nifi Rest API to create the user
	userEntity, rsp, body, err := client.Client.TenantsApi.CreateUser(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.UserEntity]{
		Client: client,
		Entity: &userEntity,
	}, nil
}

func (n *nifiClient) UpdateUser(entity nigoapi.UserEntity) ([]*ClientEntityPair[nigoapi.UserEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the user
		entityPair, err := n.UpdateUserWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserEntity], error) {
	// Request on Nifi Rest API to update the user
	userEntity, rsp, body, err := client.Client.TenantsApi.UpdateUser(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.UserEntity]{
		Client: client,
		Entity: &userEntity,
	}, nil
}

func (n *nifiClient) RemoveUser(entity nigoapi.UserEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		// Request on Nifi Rest API to remove the user
		err := n.RemoveUserWithClient(entity, clientPair)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (n *nifiClient) RemoveUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the user
	_, rsp, body, err := client.Client.TenantsApi.RemoveUser(client.Context, entity.Id,
		&nigoapi.TenantsApiRemoveUserOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
