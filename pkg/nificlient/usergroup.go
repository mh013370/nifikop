package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetUserGroups() ([]*ClientEntityPair[[]nigoapi.UserGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[[]nigoapi.UserGroupEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the user groups informations
		userGroupsEntity, rsp, body, err := client.Client.TenantsApi.GetUserGroups(client.Context)

		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			return nil, err
		}
		entities = append(entities, &ClientEntityPair[[]nigoapi.UserGroupEntity]{
			Client: client,
			Entity: &userGroupsEntity.UserGroups,
		})
	}

	return entities, nil
}

func (n *nifiClient) GetUserGroup(id string) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserGroupEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the user groups informations
		entityPair, err := n.GetUserGroupWithClient(id, client)

		if err != nil {
			n.log.Warn("Unable to locate user group at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", client.NodeId),
				zap.String("userGroupId", id))
			entityPair = &ClientEntityPair[nigoapi.UserGroupEntity]{
				Client: client,
			}
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) GetUserGroupWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Request on Nifi Rest API to get the user groups informations
	userGroupEntity, rsp, body, err := client.Client.TenantsApi.GetUserGroup(client.Context, id)

	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	var ret *nigoapi.UserGroupEntity
	if userGroupEntity.Id == "" {
		ret = nil
	} else {
		ret = &userGroupEntity
	}

	return &ClientEntityPair[nigoapi.UserGroupEntity]{
		Client: client,
		Entity: ret,
	}, err
}

func (n *nifiClient) CreateUserGroup(entity nigoapi.UserGroupEntity) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserGroupEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the user group
		entityPair, err := n.CreateUserGroupWithClient(entity, clientPair)

		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Request on Nifi Rest API to create the user group
	userGroupEntity, rsp, body, err := client.Client.TenantsApi.CreateUserGroup(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}
	return &ClientEntityPair[nigoapi.UserGroupEntity]{
		Client: client,
		Entity: &userGroupEntity,
	}, nil
}

func (n *nifiClient) UpdateUserGroup(entity nigoapi.UserGroupEntity) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.UserGroupEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the user group
		entityPair, err := n.UpdateUserGroupWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error) {
	// Request on Nifi Rest API to update the user group
	userGroupEntity, rsp, body, err := client.Client.TenantsApi.UpdateUserGroup(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.UserGroupEntity]{
		Client: client,
		Entity: &userGroupEntity,
	}, nil
}

func (n *nifiClient) RemoveUserGroup(entity nigoapi.UserGroupEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		// Request on Nifi Rest API to remove the user group
		err := n.RemoveUserGroupWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *nifiClient) RemoveUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the user group
	_, rsp, body, err := client.Client.TenantsApi.RemoveUserGroup(client.Context, entity.Id,
		&nigoapi.TenantsApiRemoveUserGroupOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
