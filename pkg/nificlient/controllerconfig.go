package nificlient

import (
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetControllerConfig() ([]*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error) {
	clients := n.privilegeCoordinatorClients()

	if len(clients) == 0 {
		n.log.Error("Error during creating node client",
			zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	// Request on Nifi Rest API to get the reporting task informations
	var entities []*ClientEntityPair[nigoapi.ControllerConfigurationEntity]

	for _, cPair := range clients {
		out, rsp, body, err := cPair.Client.ControllerApi.GetControllerConfig(cPair.Context)

		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			return nil, err
		}
		entities = append(entities, &ClientEntityPair[nigoapi.ControllerConfigurationEntity]{
			Client: cPair,
			Entity: &out,
		})
	}

	return entities, nil
}

func (n *nifiClient) UpdateControllerConfig(entity nigoapi.ControllerConfigurationEntity) ([]*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error) {
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ControllerConfigurationEntity]{}
	// Request on Nifi Rest API to update the reporting task
	for _, cPair := range clients {
		entityPair, err := n.UpdateControllerConfigWithClient(entity, cPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateControllerConfigWithClient(entity nigoapi.ControllerConfigurationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error) {
	// Request on Nifi Rest API to update the reporting task
	cEntity, rsp, body, err := client.Client.ControllerApi.UpdateControllerConfig(client.Context, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ControllerConfigurationEntity]{
		Client: client,
		Entity: &cEntity,
	}, nil
}
