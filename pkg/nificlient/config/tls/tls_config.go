package tls

import (
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/k8sutil"
	"github.com/konpyutaika/nifikop/pkg/nificlient/config/common"
	"github.com/konpyutaika/nifikop/pkg/nificlient/config/nificluster"
	"github.com/konpyutaika/nifikop/pkg/util"
	"github.com/konpyutaika/nifikop/pkg/util/clientconfig"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (n *tls) BuildConfig() (*clientconfig.NifiConfig, error) {
	var cluster *v1alpha1.NifiCluster
	var err error
	if cluster, err = k8sutil.LookupNifiCluster(n.client, n.clusterRef.Name, n.clusterRef.Namespace); err != nil {
		return nil, err
	}
	return clusterConfig(n.client, cluster)
}

func (n *tls) BuildConnect() (cluster clientconfig.ClusterConnect, err error) {
	var c *v1alpha1.NifiCluster
	if c, err = k8sutil.LookupNifiCluster(n.client, n.clusterRef.Name, n.clusterRef.Namespace); err != nil {
		return
	}

	if c.IsInternal() {
		cluster = &nificluster.InternalCluster{
			Name:      c.Name,
			Namespace: c.Namespace,
			Status:    c.Status,
		}
		return
	} else if c.IsStandalone() {
		cluster = &nificluster.StandaloneCluster{
			Name:      c.Name,
			Namespace: c.Namespace,
			Status:    c.Status,
		}
		return
	}

	config, err := n.BuildConfig()
	cluster = &nificluster.ExternalCluster{
		NodeURITemplate:    c.Spec.NodeURITemplate,
		NodeIds:            util.NodesToIdList(c.Spec.Nodes),
		NifiURI:            c.Spec.NifiURI,
		RootProcessGroupId: c.Spec.RootProcessGroupId,
		Name:               c.Name,

		NifiConfig: config,
	}

	return
}

func clusterConfig(client client.Client, cluster *v1alpha1.NifiCluster) (*clientconfig.NifiConfig, error) {
	conf := common.ClusterConfig(cluster)

	if conf.UseSSL {
		tlsConfig, err := common.TlsConfig(client, cluster)
		if err != nil {
			return conf, err
		}
		conf.TLSConfig = tlsConfig
	}

	return conf, nil
}
