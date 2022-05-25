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

package controllers

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/go-logr/logr"
	"github.com/konpyutaika/nifikop/api/v1alpha1"
	"github.com/konpyutaika/nifikop/pkg/k8sutil"
	"github.com/konpyutaika/nifikop/pkg/errorfactory"
	"github.com/konpyutaika/nifikop/pkg/util"
	"github.com/konpyutaika/nifikop/pkg/pki"
)

var replicaClusterFinalizer = "nifireplicaclusters.nifi.konpyutaika.com/finalizer"
var replicaClusterUsersFinalizer = "nifireplicaclusters.nifi.konpyutaika.com/users"

// NifiReplicaClusterReconciler reconciles a NifiReplicaCluster object
type NifiReplicaClusterReconciler struct {
	client.Client
	Scheme 					 *runtime.Scheme
	Log              logr.Logger
	Namespaces       []string
	Recorder         record.EventRecorder
	RequeueIntervals map[string]int
	RequeueOffset    int
}

//+kubebuilder:rbac:groups=nifi.konpyutaika.com,resources=nifireplicaclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=nifi.konpyutaika.com,resources=nifireplicaclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=nifi.konpyutaika.com,resources=nifireplicaclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups="policy",resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the NifiReplicaCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.11.0/pkg/reconcile
func (r *NifiReplicaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = r.Log.WithValues("nifireplicacluster", req.NamespacedName)

	// Fetch the NifiCluster instance
	instance := &v1alpha1.NifiReplicaCluster{}
	err := r.Client.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return Reconciled()
		}
		// Error reading the object - requeue the request.
		return RequeueWithError(r.Log, err.Error(), err)
	}

	// Check if marked for deletion and run finalizers
	if k8sutil.IsMarkedForDeletion(instance.ObjectMeta) {
		return r.checkFinalizers(ctx, instance)
	}

	return ctrl.Result{}, nil
}

func (r *NifiReplicaClusterReconciler) checkFinalizers(ctx context.Context,
	cluster *v1alpha1.NifiReplicaCluster) (reconcile.Result, error) {
		r.Log.Info(fmt.Sprintf("NifiReplicaCluster %s is marked for deletion, checking for children", cluster.Name))

		// If the main finalizer is gone then we've already finished up
	if !util.StringSliceContains(cluster.GetFinalizers(), clusterFinalizer) {
		return Reconciled()
	}

	var err error

	var namespaces []string
	if r.Namespaces == nil || len(r.Namespaces) == 0 {
		// Fetch a list of all namespaces for DeleteAllOf requests
		namespaces = make([]string, 0)
		var namespaceList corev1.NamespaceList
		if err := r.Client.List(ctx, &namespaceList); err != nil {
			return RequeueWithError(r.Log, "failed to get namespace list", err)
		}
		for _, ns := range namespaceList.Items {
			namespaces = append(namespaces, ns.Name)
		}
	} else {
		// use configured namespaces
		namespaces = r.Namespaces
	}

	if cluster.Spec.ListenersConfig.SSLSecrets != nil {
		// If we haven't deleted all nifiusers yet, iterate namespaces and delete all nifiusers
		// with the matching label.
		if util.StringSliceContains(cluster.GetFinalizers(), clusterUsersFinalizer) {
			r.Log.Info(fmt.Sprintf("Sending delete nifiusers request to all namespaces for cluster %s/%s", cluster.Namespace, cluster.Name))
			for _, ns := range namespaces {
				if err := r.Client.DeleteAllOf(
					ctx,
					&v1alpha1.NifiUser{},
					client.InNamespace(ns),
					client.MatchingLabels{ReplicaClusterRefLabel: ReplicaClusterLabelString(cluster)},
				); err != nil {
					if client.IgnoreNotFound(err) != nil {
						return RequeueWithError(r.Log, "failed to send delete request for children nifiusers", err)
					}
					r.Log.Info(fmt.Sprintf("No matching nifiusers in namespace: %s", ns))
				}
			}
			if cluster, err = r.removeFinalizer(ctx, cluster, clusterUsersFinalizer); err != nil {
				return RequeueWithError(r.Log, "failed to remove users finalizer from nificluster", err)
			}
		}

		// Do any necessary PKI cleanup - a PKI backend should make sure any
		// user finalizations are done before it does its final cleanup
		interval := util.GetRequeueInterval(r.RequeueIntervals["CLUSTER_TASK_NOT_READY_REQUEUE_INTERVAL"]/3, r.RequeueOffset)
		r.Log.Info("Tearing down any PKI resources for the nificluster")
		if err = pki.GetPKIManager(r.Client, cluster).FinalizePKI(ctx, r.Log); err != nil {
			switch err.(type) {
			case errorfactory.ResourceNotReady:
				r.Log.Info("The PKI is not ready to be torn down")
				return ctrl.Result{
					Requeue:      true,
					RequeueAfter: interval,
				}, nil
			default:
				return RequeueWithError(r.Log, "failed to finalize PKI", err)
			}
		}

	}

	r.Log.Info("Finalizing deletion of nificluster instance")
	if _, err = r.removeFinalizer(ctx, cluster, clusterFinalizer); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// We may have been a requeue from earlier with all conditions met - but with
			// the state of the finalizer not yet reflected in the response we got.
			return Reconciled()
		}
		return RequeueWithError(r.Log, "failed to remove main finalizer", err)
	}

	return reconcile.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NifiReplicaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.NifiReplicaCluster{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
