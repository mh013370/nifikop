package nificlient

import (
	"strconv"

	"github.com/antihax/optional"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
	"go.uber.org/zap"
)

func (n *nifiClient) GetReportingTask(id string) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ReportingTaskEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to get the reporting task informations
		out, rsp, body, err := client.Client.ReportingTasksApi.GetReportingTask(client.Context, id)

		if err := errorGetOperation(rsp, body, err, n.log); err != nil {
			n.log.Warn("Unable to locate reporting task at node. It will be created if it doesn't exist.",
				zap.Int32("nodeId", client.NodeId),
				zap.String("reportingTaskId", id),
				zap.Any("responseEntity", out))
		}
		var ret *nigoapi.ReportingTaskEntity
		if out.Id == "" {
			ret = nil
		} else {
			ret = &out
		}
		entities = append(entities, &ClientEntityPair[nigoapi.ReportingTaskEntity]{
			Client: client,
			Entity: ret,
		})
	}

	return entities, nil
}

func (n *nifiClient) CreateReportingTask(entity nigoapi.ReportingTaskEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ReportingTaskEntity]{}
	for _, client := range clients {
		// Request on Nifi Rest API to create the reporting task
		entityPair, err := n.CreateReportingTaskWithClient(entity, client)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) CreateReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Request on Nifi Rest API to create the reporting task
	out, rsp, body, err := client.Client.ControllerApi.CreateReportingTask(client.Context, entity)
	if err := errorCreateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ReportingTaskEntity]{
		Client: client,
		Entity: &out,
	}, nil
}

func (n *nifiClient) UpdateReportingTask(entity nigoapi.ReportingTaskEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ReportingTaskEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the reporting task
		entityPair, err := n.UpdateReportingTaskWithClient(entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Request on Nifi Rest API to update the reporting task
	out, rsp, body, err := client.Client.ReportingTasksApi.UpdateReportingTask(client.Context, entity.Id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ReportingTaskEntity]{
		Client: client,
		Entity: &out,
	}, nil
}

func (n *nifiClient) UpdateRunStatusReportingTask(id string, entity nigoapi.ReportingTaskRunStatusEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return nil, ErrNoNodeClientsAvailable
	}

	entities := []*ClientEntityPair[nigoapi.ReportingTaskEntity]{}
	for _, clientPair := range clients {
		// Request on Nifi Rest API to update the reporting task
		entityPair, err := n.UpdateRunStatusReportingTaskWithClient(id, entity, clientPair)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entityPair)
	}

	return entities, nil
}

func (n *nifiClient) UpdateRunStatusReportingTaskWithClient(id string, entity nigoapi.ReportingTaskRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error) {
	// Request on Nifi Rest API to update the reporting task
	out, rsp, body, err := client.Client.ReportingTasksApi.UpdateRunStatus(client.Context, id, entity)
	if err := errorUpdateOperation(rsp, body, err, n.log); err != nil {
		return nil, err
	}

	return &ClientEntityPair[nigoapi.ReportingTaskEntity]{
		Client: client,
		Entity: &out,
	}, nil
}

func (n *nifiClient) RemoveReportingTask(entity nigoapi.ReportingTaskEntity) error {
	// Get nigoapi client, favoring the one associated to the coordinator node.
	clients := n.privilegeCoordinatorClients()
	if len(clients) == 0 {
		n.log.Error("Error during creating node client", zap.Error(ErrNoNodeClientsAvailable))
		return ErrNoNodeClientsAvailable
	}

	for _, clientPair := range clients {
		// Request on Nifi Rest API to remove the reporting task
		err := n.RemoveReportingTaskWithClient(entity, clientPair)
		if err != nil {
			return err
		}
	}

	return nil
}

func (n *nifiClient) RemoveReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) error {
	// Request on Nifi Rest API to remove the reporting task
	_, rsp, body, err := client.Client.ReportingTasksApi.RemoveReportingTask(client.Context, entity.Id,
		&nigoapi.ReportingTasksApiRemoveReportingTaskOpts{
			Version: optional.NewString(strconv.FormatInt(*entity.Revision.Version, 10)),
		})

	return errorDeleteOperation(rsp, body, err, n.log)
}
