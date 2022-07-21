package clientconfig

import (
	"context"
	"crypto/tls"

	"go.uber.org/zap"
)

const (
	NifiDefaultTimeout = int64(5)

	// This header is used by nifi as a seed for generating component UUIDs on coordinator-replicated requests
	// This is how it is ensured that ProcessGroup UUIDs are the same for each node in the cluster. We do the same
	// for standalone clusters.
	// source: https://github.com/apache/nifi/blob/0de83292dec9f3077e5f06ebb5c6f14f01b74129/nifi-nar-bundles/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/api/ApplicationResource.java#L206-L231
	CLUSTER_ID_GENERATION_SEED_HEADER = "X-Cluster-Id-Generation-Seed"

	// The alias NiFi uses for the root process group
	ROOT_PROCESS_GROUP_ALIAS = "root"
)

type Manager interface {
	BuildConfig() (*NifiConfig, error)
	BuildConnect() (ClusterConnect, error)
}

type ClusterConnect interface {
	//NodeConnection(log zap.Logger, client client.Client) (node nificlient.NifiClient, err error)
	IsInternal() bool
	IsExternal() bool
	IsStandalone() bool
	ClusterLabelString() string
	IsReady(log zap.Logger) bool
	Id() string
}

// NifiConfig are the options to creating a new ClusterAdmin client
type NifiConfig struct {
	NodeURITemplate string
	NodesURI        map[int32]NodeUri
	NifiURI         string
	UseSSL          bool
	TLSConfig       *tls.Config
	ProxyUrl        string
	IsStandalone    bool

	OperationTimeout   int64
	RootProcessGroupId string
	NodesContext       map[int32]context.Context
	DefaultHeaders     map[string]string
	// Whether or not to generate a unique IdSeedHeader for every client that gets generated.
	// This is useful when a NifiClient will be used to create many objects at a time in a NiFi cluster.
	UniqueIdSeedHeader bool

	SkipDescribeCluster bool
}

func (n *NifiConfig) GetIDSeedHeader() string {
	if n.DefaultHeaders == nil {
		return ""
	}
	if seed, ok := n.DefaultHeaders[CLUSTER_ID_GENERATION_SEED_HEADER]; ok {
		return seed
	}
	return ""
}

type NodeUri struct {
	HostListener string
	RequestHost  string
}
