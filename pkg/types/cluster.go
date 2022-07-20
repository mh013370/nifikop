package types

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/constraints"
)

type ClusterWrapper[C constraints.Cluster] struct {
	Cluster C
}