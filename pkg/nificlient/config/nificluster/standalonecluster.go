package nificluster

import (
	"fmt"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"go.uber.org/zap"
)

// a standalone cluster is assumed to be internal
type StandaloneCluster struct {
	Status    v1alpha1.NifiClusterStatus
	Name      string
	Namespace string
}

func (cluster *StandaloneCluster) ClusterLabelString() string {
	return fmt.Sprintf("%s.%s", cluster.Name, cluster.Namespace)
}

func (c *StandaloneCluster) IsInternal() bool {
	return false
}

func (c *StandaloneCluster) IsExternal() bool {
	return false
}

func (c *StandaloneCluster) IsStandalone() bool {
	return true
}

func (c *StandaloneCluster) IsReady(log zap.Logger) bool {
	for _, nodeState := range c.Status.NodesState {
		if nodeState.ConfigurationState != v1alpha1.ConfigInSync || nodeState.GracefulActionState.State != v1alpha1.GracefulUpscaleSucceeded ||
			!nodeState.PodIsReady {
			return false
		}
	}
	return c.Status.State.IsReady()
}

func (c *StandaloneCluster) Id() string {
	return c.Name
}
