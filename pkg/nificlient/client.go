package nificlient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"emperror.dev/errors"
	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	nigoapi "github.com/konpyutaika/nigoapi/pkg/nifi"
)

const (
	PRIMARY_NODE        = "Primary Node"
	CLUSTER_COORDINATOR = "Cluster Coordinator"
	// ConnectNodeAction states that the NiFi node is connecting to the NiFi Cluster
	CONNECTING_STATUS = "CONNECTING"
	// ConnectStatus states that the NiFi node is connected to the NiFi Cluster
	CONNECTED_STATUS = "CONNECTED"
	// DisconnectNodeAction states that the NiFi node is disconnecting from NiFi Cluster
	DISCONNECTING_STATUS = "DISCONNECTING"
	// DisconnectStatus states that the NiFi node is disconnected from NiFi Cluster
	DISCONNECTED_STATUS = "DISCONNECTED"
	// OffloadNodeAction states that the NiFi node is offloading data to NiFi Cluster
	OFFLOADING_STATUS = "OFFLOADING"
	// OffloadStatus states that the NiFi node offloaded data to NiFi Cluster
	OFFLOADED_STATUS = "OFFLOADED"
	// RemoveNodeAction states that the NiFi node is removing from NiFi Cluster
	REMOVING_STATUS = "REMOVING"
	// RemoveStatus states that the NiFi node is removed from NiFi Cluster
	REMOVED_STATUS = "REMOVED"
)

// A container for holding a client and a response received from issuing a request to NiFi with that client.
// This is useful for internal-standalone cases where the operator acts like the cluster coordinator and there
// are follow-on requests based on some initial request.
type ClientEntityPair[T any] struct {
	Client ClientContextPair
	Entity *T
}

// A container to hold a NiFi client and context pair. These are always used together, so pairing them up eases use.
type ClientContextPair struct {
	Client  *nigoapi.APIClient
	Context context.Context
	NodeId  int32
}

// NiFiClient is the exported interface for NiFi operations
// Since this client acts like the NiFi cluster coordinator in some cases, the client wrappers
// will conditionally interact with NiFi nodes through specific clients instead of through the
// typical privileged client. That condition is whether the cluster type is internal-standalone or internal/external
type NifiClient interface {
	// Access func
	CreateAccessTokenUsingBasicAuth(username, password string, nodeId int32) (*string, error)

	// System/Cluster func
	DescribeCluster() (*ClientEntityPair[nigoapi.ClusterEntity], error)
	DescribeClusterFromNodeId(nodeId int32) (*ClientEntityPair[nigoapi.ClusterEntity], error)
	DisconnectClusterNode(nId int32) (*ClientEntityPair[nigoapi.NodeEntity], error)
	ConnectClusterNode(nId int32) (*ClientEntityPair[nigoapi.NodeEntity], error)
	OffloadClusterNode(nId int32) (*ClientEntityPair[nigoapi.NodeEntity], error)
	RemoveClusterNode(nId int32) error
	GetClusterNode(nId int32) (*ClientEntityPair[nigoapi.NodeEntity], error)
	RemoveClusterNodeFromClusterNodeId(nId string) error

	// Registry client func
	GetRegistryClient(id string) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error)
	CreateRegistryClient(entity nigoapi.RegistryClientEntity) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error)
	CreateRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.RegistryClientEntity], error)
	UpdateRegistryClient(entity nigoapi.RegistryClientEntity) ([]*ClientEntityPair[nigoapi.RegistryClientEntity], error)
	UpdateRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.RegistryClientEntity], error)
	RemoveRegistryClient(entity nigoapi.RegistryClientEntity) error
	RemoveRegistryClientWithClient(entity nigoapi.RegistryClientEntity, client ClientContextPair) error

	// Flow client func
	GetFlow(id string) ([]*ClientEntityPair[nigoapi.ProcessGroupFlowEntity], error)
	GetFlowWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupFlowEntity], error)
	UpdateFlowControllerServices(entity nigoapi.ActivateControllerServicesEntity) ([]*ClientEntityPair[nigoapi.ActivateControllerServicesEntity], error)
	UpdateFlowControllerServicesWithClient(entity nigoapi.ActivateControllerServicesEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ActivateControllerServicesEntity], error)
	UpdateFlowProcessGroup(entity nigoapi.ScheduleComponentsEntity) ([]*ClientEntityPair[nigoapi.ScheduleComponentsEntity], error)
	UpdateFlowProcessGroupWithClient(entity nigoapi.ScheduleComponentsEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ScheduleComponentsEntity], error)
	GetFlowControllerServices(id string) ([]*ClientEntityPair[nigoapi.ControllerServicesEntity], error)
	GetFlowControllerServicesWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ControllerServicesEntity], error)

	// Drop request func
	GetDropRequest(connectionId, id string) ([]*ClientEntityPair[nigoapi.DropRequestEntity], error)
	GetDropRequestWithClient(connectionId, id string, client ClientContextPair) (*ClientEntityPair[nigoapi.DropRequestEntity], error)
	CreateDropRequest(connectionId string) ([]*ClientEntityPair[nigoapi.DropRequestEntity], error)
	CreateDropRequestWithClient(connectionId string, client ClientContextPair) (*ClientEntityPair[nigoapi.DropRequestEntity], error)

	// Process Group func
	GetProcessGroup(id string) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	GetProcessGroupWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	CreateProcessGroup(entity nigoapi.ProcessGroupEntity, pgParentId string) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	CreateProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, pgParentId string, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	UpdateProcessGroup(entity nigoapi.ProcessGroupEntity) ([]*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	UpdateProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessGroupEntity], error)
	RemoveProcessGroup(entity nigoapi.ProcessGroupEntity) error
	RemoveProcessGroupWithClient(entity nigoapi.ProcessGroupEntity, client ClientContextPair) error

	// Version func
	CreateVersionUpdateRequest(pgId string, entity nigoapi.VersionControlInformationEntity) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	CreateVersionUpdateRequestWithClient(pgId string, entity nigoapi.VersionControlInformationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	GetVersionUpdateRequest(id string) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	GetVersionUpdateRequestWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	CreateVersionRevertRequest(pgId string, entity nigoapi.VersionControlInformationEntity) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	CreateVersionRevertRequestWithClient(pgId string, entity nigoapi.VersionControlInformationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	GetVersionRevertRequest(id string) ([]*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)
	GetVersionRevertRequestWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.VersionedFlowUpdateRequestEntity], error)

	// Snippet func
	CreateSnippet(entity nigoapi.SnippetEntity) ([]*ClientEntityPair[nigoapi.SnippetEntity], error)
	CreateSnippetWithClient(entity nigoapi.SnippetEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.SnippetEntity], error)
	UpdateSnippet(entity nigoapi.SnippetEntity) ([]*ClientEntityPair[nigoapi.SnippetEntity], error)
	UpdateSnippetWithClient(entity nigoapi.SnippetEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.SnippetEntity], error)

	// Processor func
	UpdateProcessor(entity nigoapi.ProcessorEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error)
	UpdateProcessorWithClient(entity nigoapi.ProcessorEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error)
	UpdateProcessorRunStatus(id string, entity nigoapi.ProcessorRunStatusEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error)
	UpdateProcessorRunStatusWithClient(id string, entity nigoapi.ProcessorRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error)

	// Input port func
	UpdateInputPortRunStatus(id string, entity nigoapi.PortRunStatusEntity) ([]*ClientEntityPair[nigoapi.ProcessorEntity], error)
	UpdateInputPortRunStatusWithClient(id string, entity nigoapi.PortRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ProcessorEntity], error)

	// Parameter context func
	GetParameterContexts() ([]*ClientEntityPair[[]nigoapi.ParameterContextEntity], error)
	GetParameterContext(id string) ([]*ClientEntityPair[nigoapi.ParameterContextEntity], error)
	GetParameterContextWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextEntity], error)
	CreateParameterContext(entity nigoapi.ParameterContextEntity) ([]*ClientEntityPair[nigoapi.ParameterContextEntity], error)
	CreateParameterContextWithClient(entity nigoapi.ParameterContextEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextEntity], error)
	RemoveParameterContext(entity nigoapi.ParameterContextEntity) error
	RemoveParameterContextWithClient(entity nigoapi.ParameterContextEntity, client ClientContextPair) error
	CreateParameterContextUpdateRequest(contextId string, entity nigoapi.ParameterContextEntity) ([]*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error)
	CreateParameterContextUpdateRequestWithClient(contextId string, entity nigoapi.ParameterContextEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error)
	GetParameterContextUpdateRequest(contextId, id string) ([]*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error)
	GetParameterContextUpdateRequestWithClient(contextId, id string, client ClientContextPair) (*ClientEntityPair[nigoapi.ParameterContextUpdateRequestEntity], error)

	// User groups func
	GetUserGroups() ([]*ClientEntityPair[[]nigoapi.UserGroupEntity], error)
	GetUserGroup(id string) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error)
	GetUserGroupWithClient(id string, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error)
	CreateUserGroup(entity nigoapi.UserGroupEntity) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error)
	CreateUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error)
	UpdateUserGroup(entity nigoapi.UserGroupEntity) ([]*ClientEntityPair[nigoapi.UserGroupEntity], error)
	UpdateUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserGroupEntity], error)
	RemoveUserGroup(entity nigoapi.UserGroupEntity) error
	RemoveUserGroupWithClient(entity nigoapi.UserGroupEntity, client ClientContextPair) error

	// User func
	GetUsers() ([]*ClientEntityPair[[]nigoapi.UserEntity], error)
	GetUsersWithClient(client ClientContextPair) (*ClientEntityPair[[]nigoapi.UserEntity], error)
	GetUser(id string) ([]*ClientEntityPair[nigoapi.UserEntity], error)
	CreateUser(entity nigoapi.UserEntity) ([]*ClientEntityPair[nigoapi.UserEntity], error)
	CreateUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserEntity], error)
	UpdateUser(entity nigoapi.UserEntity) ([]*ClientEntityPair[nigoapi.UserEntity], error)
	UpdateUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.UserEntity], error)
	RemoveUser(entity nigoapi.UserEntity) error
	RemoveUserWithClient(entity nigoapi.UserEntity, client ClientContextPair) error

	// Policies func
	GetAccessPolicy(action, resource string) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	GetAccessPolicyWithClient(action, resource string, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	CreateAccessPolicy(entity nigoapi.AccessPolicyEntity) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	CreateAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	UpdateAccessPolicy(entity nigoapi.AccessPolicyEntity) ([]*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	UpdateAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.AccessPolicyEntity], error)
	RemoveAccessPolicy(entity nigoapi.AccessPolicyEntity) error
	RemoveAccessPolicyWithClient(entity nigoapi.AccessPolicyEntity, client ClientContextPair) error

	// Reportingtask func
	GetReportingTask(id string) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	CreateReportingTask(entity nigoapi.ReportingTaskEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	CreateReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	UpdateReportingTask(entity nigoapi.ReportingTaskEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	UpdateReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	UpdateRunStatusReportingTask(id string, entity nigoapi.ReportingTaskRunStatusEntity) ([]*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	UpdateRunStatusReportingTaskWithClient(id string, entity nigoapi.ReportingTaskRunStatusEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ReportingTaskEntity], error)
	RemoveReportingTask(entity nigoapi.ReportingTaskEntity) error
	RemoveReportingTaskWithClient(entity nigoapi.ReportingTaskEntity, client ClientContextPair) error

	// ControllerConfig func
	GetControllerConfig() ([]*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error)
	UpdateControllerConfig(entity nigoapi.ControllerConfigurationEntity) ([]*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error)
	UpdateControllerConfigWithClient(entity nigoapi.ControllerConfigurationEntity, client ClientContextPair) (*ClientEntityPair[nigoapi.ControllerConfigurationEntity], error)

	Build() error
}

type nifiClient struct {
	NifiClient
	log        *zap.Logger
	opts       *clientconfig.NifiConfig
	client     *nigoapi.APIClient
	nodeClient map[int32]*nigoapi.APIClient
	timeout    time.Duration
	nodes      []nigoapi.NodeDto

	// client funcs for mocking
	newClient func(*nigoapi.Configuration) *nigoapi.APIClient
}

func New(opts *clientconfig.NifiConfig, logger *zap.Logger) NifiClient {
	nClient := &nifiClient{
		log:     logger,
		opts:    opts,
		timeout: time.Duration(opts.OperationTimeout) * time.Second,
	}

	nClient.newClient = nigoapi.NewAPIClient
	return nClient
}

func (n *nifiClient) Build() error {
	config := n.getNifiGoApiConfig()
	n.client = n.newClient(config)

	n.nodeClient = make(map[int32]*nigoapi.APIClient)
	for nodeId, _ := range n.opts.NodesURI {
		nodeConfig := n.getNiNodeGoApiConfig(nodeId)
		n.nodeClient[nodeId] = n.newClient(nodeConfig)
	}

	// we do not describe standalone clusters or those which explicitly specify to skip cluster describe
	if !n.opts.SkipDescribeCluster && !n.opts.IsStandalone {
		clusterEntity, err := n.DescribeCluster()
		if err != nil || clusterEntity.Entity == nil || clusterEntity.Entity.Cluster == nil {
			err = errorfactory.New(errorfactory.NodesUnreachable{}, err, fmt.Sprintf("could not connect to nifi nodes: %s", n.opts.NifiURI))
			return err
		}

		n.nodes = clusterEntity.Entity.Cluster.Nodes
	}

	return nil
}

// Return a set of clients, possibly just 1, to interact with the desired NiFi cluster
// An empty return list indicates no clients are available.
func (n *nifiClient) privilegeCoordinatorClients() []ClientContextPair {
	clients := []ClientContextPair{}
	if n.opts.IsStandalone {
		// for standalone clusters, we perform all of the coordination in the operator
		return n.getAllNodeClients()

	} else {
		// for non-standalone internal clusters, we delegate coordination to the privileged NiFi cluster coordinator
		client, context, nodeId := n.privilegeCoordinatorClient()
		if client == nil {
			return clients
		}
		clients = append(clients, ClientContextPair{
			Client:  client,
			Context: context,
			NodeId:  nodeId,
		})
		return clients
	}
}

// find the cluster coordinator client for clustered, non-standalone NiFis
func (n *nifiClient) privilegeCoordinatorClient() (*nigoapi.APIClient, context.Context, int32) {
	if clientId := n.coordinatorNodeId(); clientId != nil {
		return n.nodeClient[*clientId], n.opts.NodesContext[*clientId], *clientId
	}

	if clientId := n.privilegeNodeClient(); clientId != nil {
		return n.nodeClient[*clientId], n.opts.NodesContext[*clientId], *clientId
	}

	return n.client, nil, 0
}

func (n *nifiClient) privilegeCoordinatorExceptNodeIdClient(nId int32) (*nigoapi.APIClient, context.Context, int32) {
	nodeDto := n.nodeDtoByNodeId(nId)
	if nodeDto == nil || isCoordinator(nodeDto) {
		if clientId := n.firstConnectedNodeId(nId); clientId != nil {
			return n.nodeClient[*clientId], n.opts.NodesContext[*clientId], *clientId
		}
	}

	return n.privilegeCoordinatorClient()
}

// TODO : change logic by binding in status the nodeId with the Nifi Cluster Node id ?
func (n *nifiClient) firstConnectedNodeId(excludeId int32) *int32 {
	// Convert nodeId to a Cluster Node for the one to exclude
	excludedNodeDto := n.nodeDtoByNodeId(excludeId)
	// For each NiFi Cluster Node
	for id := range n.nodes {
		nodeDto := n.nodes[id]
		// Check that it's not the one exclueded and it is Connected
		if excludedNodeDto == nil || (nodeDto.NodeId != excludedNodeDto.NodeId && isConnected(excludedNodeDto)) {
			// Check that a Node exist in the NifiCluster definition, and that we have a client associated
			if nId := n.nodeIdByNodeDto(&nodeDto); nId != nil {
				return nId
			}
		}
	}
	return nil
}

func (n *nifiClient) coordinatorNodeId() *int32 {
	for id := range n.nodes {
		nodeDto := n.nodes[id]
		// We return the Node Id associated to the Cluster Node coordinator, if it is connected
		if isCoordinator(&nodeDto) && isConnected(&nodeDto) {
			return n.nodeIdByNodeDto(&nodeDto)
		}
	}
	return nil
}

func (n *nifiClient) privilegeNodeClient() *int32 {
	for id := range n.nodeClient {
		return &id
	}
	return nil
}

func (n *nifiClient) nodeDtoByNodeId(nId int32) *nigoapi.NodeDto {
	for id := range n.nodes {
		nodeDto := n.nodes[id]
		// Check if the Cluster Node uri match with the one associated to the NifiCluster nodeId searched
		if fmt.Sprintf("%s:%d", nodeDto.Address, nodeDto.ApiPort) == fmt.Sprintf(n.opts.NodeURITemplate, nId) {
			return &nodeDto
		}
	}
	return nil
}

func (n *nifiClient) nodeIdByNodeDto(nodeDto *nigoapi.NodeDto) *int32 {
	// Extract the uri associated to the Cluster Node
	searchedUri := fmt.Sprintf("%s:%d", nodeDto.Address, nodeDto.ApiPort)
	// For each uri generated from NifiCluster resources node defined
	for id, uri := range n.opts.NodesURI {
		// Check if we find a match
		if uri.HostListener == searchedUri {
			findId := id
			return &findId
		}
	}

	return nil
}

func (n *nifiClient) setNodeFromNodes(nodeDto *nigoapi.NodeDto) {
	for id := range n.nodes {
		if n.nodes[id].NodeId == nodeDto.NodeId {
			n.nodes[id] = *nodeDto
			break
		}
	}
}

// Get a list of all clients with their respective contexts to interact with NiFi nodes.
// This is used in standalone cluster scenarios where nifikop must interact with each node separately.
func (n *nifiClient) getAllNodeClients() []ClientContextPair {
	clients := []ClientContextPair{}

	for clientId := range n.opts.NodesURI {
		client := n.nodeClient[clientId]
		context := n.opts.NodesContext[clientId]
		clients = append(clients, ClientContextPair{
			Client:  client,
			Context: context,
			NodeId:  clientId,
		})
	}
	return clients
}

// NewFromConfig is a convenient wrapper around New() and ClusterConfig()
func NewFromConfig(opts *clientconfig.NifiConfig, logger *zap.Logger) (NifiClient, error) {
	var client NifiClient
	var err error

	if opts == nil {
		return nil, errorfactory.New(errorfactory.NilClientConfig{}, errors.New("The NiFi client config is nil"), "The NiFi client config is nil")
	}
	// if not present, set the generation seed header so the component UUID generated by each NiFi is the same.
	if _, ok := opts.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER]; !ok || opts.UniqueIdSeedHeader {
		opts.DefaultHeaders[clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER] = uuid.NewString()
	}

	client = New(opts, logger)
	err = client.Build()
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (n *nifiClient) getNifiGoApiConfig() (config *nigoapi.Configuration) {
	config = nigoapi.NewConfiguration()

	for key, value := range n.opts.DefaultHeaders {
		config.AddDefaultHeader(key, value)
	}

	if n.opts.UniqueIdSeedHeader {
		config.AddDefaultHeader(clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER, uuid.NewString())
	}

	protocol := "http"
	var transport *http.Transport = nil
	if n.opts.UseSSL {
		transport = &http.Transport{}
		config.Scheme = "HTTPS"
		n.opts.TLSConfig.BuildNameToCertificate()
		transport.TLSClientConfig = n.opts.TLSConfig
		protocol = "https"
	}

	if len(n.opts.ProxyUrl) > 0 {
		proxyUrl, err := url.Parse(n.opts.ProxyUrl)
		if err == nil {
			if transport == nil {
				transport = &http.Transport{}
			}
			transport.Proxy = http.ProxyURL(proxyUrl)
		}
	}

	config.HTTPClient = &http.Client{}
	if transport != nil {
		config.HTTPClient = &http.Client{Transport: transport}
	}

	config.BasePath = fmt.Sprintf("%s://%s/nifi-api", protocol, n.opts.NifiURI)
	config.Host = n.opts.NifiURI
	if len(n.opts.NifiURI) == 0 {
		for nodeId, _ := range n.opts.NodesURI {
			config.BasePath = fmt.Sprintf("%s://%s/nifi-api", protocol, n.opts.NodesURI[nodeId].RequestHost)
			config.Host = n.opts.NodesURI[nodeId].RequestHost
		}
	}
	return
}

func (n *nifiClient) getNiNodeGoApiConfig(nodeId int32) (config *nigoapi.Configuration) {
	config = nigoapi.NewConfiguration()

	for key, value := range n.opts.DefaultHeaders {
		config.AddDefaultHeader(key, value)
	}

	if n.opts.UniqueIdSeedHeader {
		config.AddDefaultHeader(clientconfig.CLUSTER_ID_GENERATION_SEED_HEADER, uuid.NewString())
	}

	config.HTTPClient = &http.Client{}
	protocol := "http"

	var transport *http.Transport = nil
	if n.opts.UseSSL {
		transport = &http.Transport{}
		config.Scheme = "HTTPS"
		n.opts.TLSConfig.BuildNameToCertificate()
		transport.TLSClientConfig = n.opts.TLSConfig
		protocol = "https"
	}

	if n.opts.ProxyUrl != "" {
		proxyUrl, err := url.Parse(n.opts.ProxyUrl)
		if err == nil {
			if transport == nil {
				transport = &http.Transport{}
			}
			transport.Proxy = http.ProxyURL(proxyUrl)
		}
	}
	config.HTTPClient = &http.Client{}
	if transport != nil {
		config.HTTPClient = &http.Client{Transport: transport}
	}

	config.BasePath = fmt.Sprintf("%s://%s/nifi-api", protocol, n.opts.NodesURI[nodeId].RequestHost)
	config.Host = n.opts.NodesURI[nodeId].RequestHost
	if len(n.opts.NifiURI) != 0 {
		config.Host = n.opts.NifiURI
	}

	return
}

func isCoordinator(node *nigoapi.NodeDto) bool {
	// For each role looking that it contains the Coordinator one.
	for _, role := range node.Roles {
		if role == CLUSTER_COORDINATOR {
			return true
		}
	}
	return false
}

func isConnected(node *nigoapi.NodeDto) bool {
	return node.Status == CONNECTED_STATUS
}
