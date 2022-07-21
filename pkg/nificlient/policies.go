package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetAccessPolicy(action, resource string) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.AccessPolicyEntity]{}
	for _, clientPair := range clients {
		entityPair, err := n.GetAccessPolicyWithClient(action, resource, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}
	return entities, nil
}

func (n *nifiClient) GetAccessPolicyWithClient(action, resource string, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Request on Nifi Rest API to get the access policy informations
	for {
		if resource[0:1] == "/" {
			resource = resource[1:]
			continue
		}
		break
	}

	accessPolicyEntity, rsp, body, err := client.Client.PoliciesApi.GetAccessPolicyForResource(client.Context, action, resource)

	if err := errorGetOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.AccessPolicyEntity]{
		Client: client,
		Entity: &accessPolicyEntity,
	}, nil
}

func (n *nifiClient) CreateAccessPolicy(entity nigoapi.AccessPolicyEntity) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.AccessPolicyEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to create the access policy
		entityPair, err := n.CreateAccessPolicyWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Request on Nifi Rest API to create the access policy
	accessPolicyEntity, rsp, body, err := client.Client.PoliciesApi.CreateAccessPolicy(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	n.log.Info("Created access policy",
		zap.Int32("nodeId", client.NodeId),
		zap.String("action", entity.Component.Action),
		zap.String("resource", entity.Component.Resource))

	return &ClientEntityPair[nigoapi.AccessPolicyEntity]{
		Client: client,
		Entity: &accessPolicyEntity,
	}, nil
}

func (n *nifiClient) UpdateAccessPolicy(entity nigoapi.AccessPolicyEntity) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.AccessPolicyEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the access policy
		entityPair, err := n.UpdateAccessPolicyWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error) {
	// Request on Nifi Rest API to update the access policy
	accessPolicyEntity, rsp, body, err := client.Client.PoliciesApi.UpdateAccessPolicy(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.AccessPolicyEntity]{
		Client: client,
		Entity: &accessPolicyEntity,
	}, nil
}

func (n *nifiClient) RemoveAccessPolicy(entity nigoapi.AccessPolicyEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		// Request on Nifi Rest API to remove the registry client
		err := n.RemoveAccessPolicyWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *nifiClient) RemoveAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the registry client
	_, rsp, body, err := client.Client.PoliciesApi.RemoveAccessPolicy(client.Context, entity.Id,
		&nigoapi.PoliciesApiRemoveAccessPolicyOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
