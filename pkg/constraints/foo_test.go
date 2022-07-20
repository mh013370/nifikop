package constraints

import (
	"testing"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
)


func Test(t *testing.T) {
	cluster := &v1alpha1.NifiCluster{
		Spec: v1alpha1.NifiClusterSpec{
			Type: v1alpha1.ExternalCluster,
		},
	}
	
	spec := GetSpec[](cluster)
}
