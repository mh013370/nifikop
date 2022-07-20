/*
Copyright 2020.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NifiReplicaClusterSpec defines the desired state of NifiReplicaCluster
type NifiReplicaClusterSpec struct {
	ClusterSpec
	// ClusterSpec embedded interace that contains everything shared between NifiCluster and NifiReplicaCluster
	CommonClusterSpec `json:",inline"`
	// Replicas is the number of replicas this cluster currently has
	// +kubebuilder:default:=1
	// +optional
	Replicas int32 `json:"replicas"`
	// Application defines the policy for the Deployment/StatefulSet owned by NiFiKop.
	Application ApplicationPolicy `json:"application"`
	// nodeConfig specifies node configs to apply to each replica in this cluster
	NodeConfig NodeConfig `json:"nodeConfig,omitempty"`
}

// NifiReplicaClusterStatus defines the observed state of NifiReplicaCluster
type NifiReplicaClusterStatus struct {
	ClusterStatus
	CommonClusterStatus `json:",inline"`
	// the time that this cluster was created
	CreationTime metav1.Time `json:"creationTime,omitempty"`
}

// NifiState holds information about nifi state
type ReplicaNodeState struct {
	// RootProcessGroupId contains the uuid of the root process group for this cluster
	RootProcessGroupId string `json:"rootProcessGroupId,omitempty"`
	// ConfigurationState holds info about the config
	ConfigurationState ConfigurationState `json:"configurationState"`
	// PodIsReady whether or not the associated pod is ready
	PodIsReady bool `json:"podIsReady"`
}

// ApplicationPolicy holds information that applies to the Deployment/StatefulSet created for this cluster
type ApplicationPolicy struct {
	// CreateStatefulSet if true, create and configure a StatefulSet. Otherwise, create a Deployment.
	CreateStatefulSet bool `json:"createStatefulSet"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status

// NifiReplicaCluster is the Schema for the nifireplicaclusters API
type NifiReplicaCluster struct {
	Cluster
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NifiReplicaClusterSpec   `json:"spec,omitempty"`
	Status NifiReplicaClusterStatus `json:"status,omitempty"`
}
func (n *NifiReplicaCluster) GetTypeMeta() metav1.TypeMeta {
	return n.TypeMeta
}
func (n *NifiReplicaCluster) GetObjectMeta() metav1.ObjectMeta {
	return n.ObjectMeta
}
func (n *NifiReplicaCluster) GetName() string {
	return n.Name
}
func (n *NifiReplicaCluster) GetNamespace() string {
	return n.Namespace
}
func (n *NifiReplicaCluster) GetSpec() ClusterSpec {
	return n.Spec
}
func (n *NifiReplicaCluster) GetCommonSpec() CommonClusterSpec {
	return n.Spec.CommonClusterSpec
}
func (n *NifiReplicaCluster) GetStatus() ClusterStatus {
	return n.Status
}

//+kubebuilder:object:root=true

// NifiReplicaClusterList contains a list of NifiReplicaCluster
type NifiReplicaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NifiReplicaCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NifiReplicaCluster{}, &NifiReplicaClusterList{})
}
