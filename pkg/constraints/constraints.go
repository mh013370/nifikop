package constraints

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
)

// Cluster captures all variants of nifi cluster types
type Cluster interface {
	*v1alpha1.NifiCluster | *v1alpha1.NifiReplicaCluster
}

type ClusterSpec interface {
	v1alpha1.NifiClusterSpec | v1alpha1.NifiReplicaClusterSpec
}

type ClusterStatus interface {
	v1alpha1.NifiClusterStatus | v1alpha1.NifiReplicaClusterStatus
}

