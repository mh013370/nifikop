package nifi

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/imdario/mergo"
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/resources/templates"
	"github.com/konpyutaika/nifikop/pkg/util"
	nifiutil "github.com/konpyutaika/nifikop/pkg/util/nifi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	runtimeClient "sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *Reconciler) service(id int32, log logr.Logger) runtimeClient.Object {

	usedPorts := generateServicePortForInternalListeners(r.NifiCluster.Spec.ListenersConfig.InternalListeners)

	return &corev1.Service{
		ObjectMeta: templates.ObjectMetaWithAnnotations(
			nifiutil.ComputeNodeName(id, r.NifiCluster.Name),
			util.MergeLabels(
				r.NifiCluster.Spec.Service.Labels,
				nifiutil.LabelsForNifi(r.NifiCluster.Name),
				map[string]string{"nodeId": fmt.Sprintf("%d", id)},
			),
			r.NifiCluster.Spec.Service.Annotations,
			r.NifiCluster),
		Spec: corev1.ServiceSpec{
			Type:            corev1.ServiceTypeClusterIP,
			SessionAffinity: corev1.ServiceAffinityNone,
			Selector:        util.MergeLabels(nifiutil.LabelsForNifi(r.NifiCluster.Name), map[string]string{"nodeId": fmt.Sprintf("%d", id)}),
			Ports:           usedPorts,
		},
	}
}

func (r *Reconciler) externalServices(log logr.Logger) []runtimeClient.Object {

	var services []runtimeClient.Object
	for _, eService := range r.NifiCluster.Spec.ExternalServices {

		annotations := &eService.Metadata.Annotations
		if err := mergo.Merge(annotations, r.NifiCluster.Spec.Service.Annotations); err != nil {
			log.Error(err, "error occurred during merging service annotations")
		}

		usedPorts := r.generateServicePortForExternalListeners(eService)
		services = append(services, &corev1.Service{
			ObjectMeta: templates.ObjectMetaWithAnnotations(
				eService.Name,
				util.MergeLabels(
					r.NifiCluster.Spec.Service.Labels,
					eService.Metadata.Labels,
					nifiutil.LabelsForNifi(r.NifiCluster.Name),
				),
				*annotations,
				r.NifiCluster),
			Spec: corev1.ServiceSpec{
				Type:                     eService.Spec.Type,
				SessionAffinity:          corev1.ServiceAffinityClientIP,
				Selector:                 nifiutil.LabelsForNifi(r.NifiCluster.Name),
				Ports:                    usedPorts,
				ClusterIP:                eService.Spec.ClusterIP,
				ExternalIPs:              eService.Spec.ExternalIPs,
				LoadBalancerIP:           eService.Spec.LoadBalancerIP,
				LoadBalancerSourceRanges: eService.Spec.LoadBalancerSourceRanges,
				ExternalName:             eService.Spec.ExternalName,
			},
		})
	}
	return services
}

//
func generateServicePortForInternalListeners(listeners []v1alpha1.InternalListenerConfig) []corev1.ServicePort {
	var usedPorts []corev1.ServicePort

	for _, iListeners := range listeners {
		usedPorts = append(usedPorts, corev1.ServicePort{
			Name:       strings.ReplaceAll(iListeners.Name, "_", ""),
			Port:       iListeners.ContainerPort,
			TargetPort: intstr.FromInt(int(iListeners.ContainerPort)),
			Protocol:   corev1.ProtocolTCP,
		})
	}

	return usedPorts
}

func (r *Reconciler) generateServicePortForExternalListeners(eService v1alpha1.ExternalServiceConfig) []corev1.ServicePort {
	var usedPorts []corev1.ServicePort

	for _, port := range eService.Spec.PortConfigs {
		for _, iListener := range r.NifiCluster.Spec.ListenersConfig.InternalListeners {
			if port.InternalListenerName == iListener.Name {
				usedPorts = append(usedPorts, corev1.ServicePort{
					Name:       strings.ReplaceAll(iListener.Name, "_", ""),
					Port:       port.Port,
					TargetPort: intstr.FromInt(int(iListener.ContainerPort)),
					Protocol:   corev1.ProtocolTCP,
				})
			}
		}
	}

	return usedPorts
}
