package types

import (
	"testing"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
)

func Testx(t *testing.T) {
	cluster := &v1alpha1.NifiCluster{}
	cluster2 := &v1alpha1.NifiReplicaCluster{}

	wrap := &ClusterWrapper[*v1alpha1.NifiCluster]{
		Cluster: cluster,
	}

	switch 
}