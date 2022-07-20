package pki

import (
	"context"
	"crypto/tls"

	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/constraints"
	"github.com/konpyutaika/nifikop/pkg/pki/certmanagerpki"
	"github.com/konpyutaika/nifikop/pkg/util/pki"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MockBackend is used for mocking during testing
var MockBackend = v1alpha1.PKIBackend("mock")

// GetPKIManager returns a PKI/User manager interface for a given cluster
func GetPKIManager[C constraints.Cluster](client client.Client, cluster v1alpha1.Cluster) pki.Manager {
	
	switch cluster.GetCommonSpec().ListenersConfig.SSLSecrets.PKIBackend {

		// Use cert-manager for pki backend
		case v1alpha1.PKIBackendCertManager:
			return certmanagerpki.New(client, cluster)
	
		// TODO : Add vault
		// Use vault for pki backend
		/*case v1alpha1.PKIBackendVault:
		return vaultpki.New(client, cluster)*/
	
		// Return mock backend for testing - cannot be triggered by CR due to enum in api schema
		case MockBackend:
			return newMockPKIManager(client, cluster)
	
		// Default use cert-manager - state explicitly for clarity and to make compiler happy
		default:
			return certmanagerpki.New(client, cluster)
		}
	
}

// Mock types and functions

type mockPKIManager struct {
	pki.Manager
	client  client.Client
	cluster *v1alpha1.NifiCluster
}

func newMockPKIManager(client client.Client, cluster *v1alpha1.NifiCluster) pki.Manager {
	return &mockPKIManager{client: client, cluster: cluster}
}

func (m *mockPKIManager) ReconcilePKI(ctx context.Context, logger zap.Logger, scheme *runtime.Scheme, externalHostnames []string) error {
	return nil
}

func (m *mockPKIManager) FinalizePKI(ctx context.Context, logger zap.Logger) error {
	return nil
}

func (m *mockPKIManager) ReconcileUserCertificate(ctx context.Context, user *v1alpha1.NifiUser, scheme *runtime.Scheme) (*pki.UserCertificate, error) {
	return &pki.UserCertificate{}, nil
}

func (m *mockPKIManager) FinalizeUserCertificate(ctx context.Context, user *v1alpha1.NifiUser) error {
	return nil
}

func (m *mockPKIManager) GetControllerTLSConfig() (*tls.Config, error) {
	return &tls.Config{}, nil
}
