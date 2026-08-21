package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	"github.com/hashicorp/go-multierror"
	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	condition "github.com/mariadb-operator/mariadb-operator/v26/pkg/condition"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/controller/replication"
	mdbpod "github.com/mariadb-operator/mariadb-operator/v26/pkg/pod"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	stspkg "github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (r *MariaDBReconciler) reconcileStatus(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) (ctrl.Result, error) {
	if mdb.IsSuspended() {
		return ctrl.Result{}, r.patchStatus(ctx, mdb, func(status *mariadbv1alpha1.MariaDBStatus) error {
			condition.SetReadySuspended(status)
			return nil
		})
	}
	logger := log.FromContext(ctx).WithName("status").V(1)

	logger.Info("Reconciling MariaDB Status", "MariaDB:", mdb.Name)

	// Discover and persist the external replication server_id offset before the StatefulSet is built,
	// so pods are created with a non-colliding server_id and never need a restart to adopt it.
	if result, err := r.reconcileExternalReplServerId(ctx, mdb); !result.IsZero() || err != nil {
		return result, err
	}

	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(mdb), &sts); err != nil {
		logger.Info("error getting StatefulSet", "err", err)
	}

	replRoles, replErr := r.getReplicationRoles(ctx, mdb)
	if replErr != nil {
		logger.Info("error getting replication state", "err", replErr)
	}
	replStatus, replErrStatusErr := r.getReplicaStatus(ctx, mdb, logger)
	if replErrStatusErr != nil {
		logger.Info("error getting replication status", "err", replStatus)
	}

	mxsPrimaryPodIndex, mxsErr := r.getMaxScalePrimaryPod(ctx, mdb)
	if mxsErr != nil {
		logger.Info("error getting MaxScale primary Pod", "err", mxsErr)
	}

	tlsStatus, err := r.getTLSStatus(ctx, mdb)
	if err != nil {
		logger.Info("error getting TLS status", "err", err)
	}

	return ctrl.Result{}, r.patchStatus(ctx, mdb,
		r.statusPatcher(ctx, mdb, &sts, replRoles, replStatus, tlsStatus, mxsPrimaryPodIndex, mxsErr))
}

// statusPatcher builds the patcher that reconciles the MariaDB status from the state gathered by reconcileStatus.
func (r *MariaDBReconciler) statusPatcher(ctx context.Context, mdb *mariadbv1alpha1.MariaDB, sts *appsv1.StatefulSet,
	replRoles map[string]mariadbv1alpha1.ReplicationRole, replStatus map[string]mariadbv1alpha1.ReplicaStatus,
	tlsStatus *mariadbv1alpha1.MariaDBTLSStatus, mxsPrimaryPodIndex *int, mxsErr error) func(*mariadbv1alpha1.MariaDBStatus) error {
	return func(status *mariadbv1alpha1.MariaDBStatus) error {
		status.DefaultVersion = r.Environment.MariadbDefaultVersion
		status.Replicas = sts.Status.ReadyReplicas
		defaultPrimary(mdb)
		setMaxScalePrimary(mdb, mxsPrimaryPodIndex)

		if replRoles != nil {
			if status.Replication == nil {
				status.Replication = &mariadbv1alpha1.ReplicationStatus{}
			}
			status.Replication.Roles = replRoles
		}
		if replStatus != nil {
			if status.Replication == nil {
				status.Replication = &mariadbv1alpha1.ReplicationStatus{}
			}
			status.Replication.Replicas = replStatus
		}
		// reset replication status after a cluster-level switchover
		if mdb.IsMultiClusterPrimary() && mdb.IsGaleraEnabled() {
			status.Replication = nil
		}

		if tlsStatus != nil {
			status.TLS = tlsStatus
		}

		if apierrors.IsNotFound(mxsErr) && mdb.IsMaxScaleEnabled() {
			r.ConditionReady.PatcherRefResolver(mxsErr, mariadbv1alpha1.MaxScale{})(&mdb.Status)
			return nil
		}
		if mdb.IsRestoringBackup() || mdb.IsResizingStorage() || mdb.IsSwitchingPrimary() || mdb.HasGaleraNotReadyCondition() {
			return nil
		}

		if err := r.setUpdatedCondition(ctx, mdb); err != nil {
			log.FromContext(ctx).V(1).Info("error setting MariaDB updated condition", "err", err)
		}
		condition.SetReadyWithMariaDB(&mdb.Status, sts, mdb)
		return nil
	}
}

// externalReplServerIdGap is the room left between the highest server_id already in use on the
// external MariaDB and the offset assigned to this cluster. It gives headroom for this cluster to
// scale out and for other clusters replicating from the same source to claim their own blocks.
const externalReplServerIdGap = 100

// reconcileExternalReplServerId auto-discovers a non-colliding server_id offset for external
// replication and persists it to status. It is computed only once: when a manual serverIdOffset is
// set, or once the offset is already persisted, it is a no-op. While the external MariaDB is not
// reachable it requeues, which short-circuits the reconcile loop and prevents the StatefulSet from
// being created before the offset is known (avoiding a rolling restart).
func (r *MariaDBReconciler) reconcileExternalReplServerId(ctx context.Context,
	mdb *mariadbv1alpha1.MariaDB) (ctrl.Result, error) {
	if !mdb.IsReplicationEnabled() {
		return ctrl.Result{}, nil
	}
	replication := mdb.Replication()
	if !replication.IsExternalReplication() {
		return ctrl.Result{}, nil
	}
	// Manual offset takes precedence and is left untouched.
	if replication.ReplicaFromExternal.ServerIdOffset != nil {
		return ctrl.Result{}, nil
	}
	// Compute-once: never re-query once persisted.
	if mdb.Status.ExternalReplication != nil && mdb.Status.ExternalReplication.ServerIdOffset != nil {
		return ctrl.Result{}, nil
	}
	logger := log.FromContext(ctx).WithName("external-repl-server-id")

	emdb, err := r.RefResolver.ExternalMariaDB(ctx, &replication.ReplicaFromExternal.MariaDBRef.ObjectReference, mdb.Namespace)
	if err != nil {
		logger.Info("error getting external MariaDB, requeuing", "err", err)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	if !emdb.IsReady() {
		logger.Info("external MariaDB is not ready, requeuing")
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	// The external MariaDB endpoint (e.g. a VIP fronting a MaxScale readconnroute) may resolve to
	// either the primary or a replica. Server ids must be enumerated from the primary, as only it sees
	// every replica registered in the topology (SHOW SLAVE HOSTS). If we land on a replica, follow
	// SHOW REPLICA STATUS to the primary and connect there directly, reusing the same credentials/TLS.
	client, err := sql.NewClientWithMariaDB(ctx, emdb, r.RefResolver)
	if err != nil {
		logger.Info("error connecting to external MariaDB, requeuing", "err", err)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}
	defer client.Close()

	masterHost, masterPort, isReplica, err := client.ReplicationMasterEndpoint(ctx)
	if err != nil {
		logger.Info("error resolving external primary, requeuing", "err", err)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	primaryClient := client
	if isReplica {
		logger.Info("external endpoint is a replica, connecting to its primary",
			"master-host", masterHost, "master-port", masterPort)
		masterClient, err := sql.NewClientWithMariaDB(ctx, emdb, r.RefResolver, sql.WithHost(masterHost), sql.WithPort(masterPort))
		if err != nil {
			logger.Info("error connecting to external primary, requeuing", "err", err)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
		defer masterClient.Close()
		primaryClient = masterClient
	}

	ids, err := primaryClient.InUseServerIds(ctx)
	if err != nil {
		logger.Info("error getting in-use server ids, requeuing", "err", err)
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	}

	offset := externalReplServerIdGap
	if len(ids) > 0 {
		offset = slices.Max(ids) + externalReplServerIdGap
	}
	logger.Info("discovered external replication server_id offset", "offset", offset, "in-use-server-ids", ids)

	if err := r.patchStatus(ctx, mdb, func(status *mariadbv1alpha1.MariaDBStatus) error {
		status.ExternalReplication = &mariadbv1alpha1.ExternalReplicationStatus{
			ServerIdOffset: &offset,
		}
		return nil
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("error patching MariaDB status: %v", err)
	}
	return ctrl.Result{}, nil
}

func shouldReconcileReplicationRoleForPod(mdb *mariadbv1alpha1.MariaDB, podIndex int) bool {
	return mdb.IsReplicationEnabled() ||
		(mdb.IsGaleraEnabled() && mdb.IsMultiClusterPrimaryReplica(podIndex))
}

func (r *MariaDBReconciler) getReplicationRoles(ctx context.Context,
	mdb *mariadbv1alpha1.MariaDB) (map[string]mariadbv1alpha1.ReplicationRole, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Getting Replication Roles")
	if !mdb.IsReplicationEnabled() {
		return nil, nil
	}

	clientSet, err := replication.NewReplicationClientSet(mdb, r.RefResolver)
	if err != nil {
		return nil, fmt.Errorf("error creating mariadb clientset: %v", err)
	}
	defer clientSet.Close()

	var replState map[string]mariadbv1alpha1.ReplicationRole

	for i := 0; i < int(mdb.Spec.Replicas); i++ {
		if !shouldReconcileReplicationRoleForPod(mdb, i) {
			continue
		}
		pod := stspkg.PodName(mdb.ObjectMeta, i)

		client, err := clientSet.ClientForIndex(ctx, i)
		if err != nil {
			logger.V(1).Info("error getting client for Pod", "err", err, "pod", pod)
			continue
		}
		role, err := r.getReplicationRole(ctx, mdb, i, client, logger)
		if err != nil {
			logger.V(1).Info("error getting Pod replication role", "err", err, "pod", pod)
			continue
		}

		if replState == nil {
			replState = make(map[string]mariadbv1alpha1.ReplicationRole)
		}
		replState[pod] = role
	}
	return replState, nil
}

func (r *MariaDBReconciler) getReplicationRole(ctx context.Context, mdb *mariadbv1alpha1.MariaDB, podIndex int,
	client *sql.Client, logger logr.Logger) (mariadbv1alpha1.ReplicationRole, error) {
	var (
		err              error
		aggErr           *multierror.Error
		isPrimaryReplica bool
		isReplica        bool
		isPrimary        bool
	)

	isPrimaryReplica, err = r.isMultiClusterPrimaryReplica(ctx, mdb, podIndex, client, logger)
	if err != nil && !sql.IsConnectionNotExists(err) {
		aggErr = multierror.Append(aggErr, err)
	}
	isReplica, err = client.IsReplicationReplica(ctx)
	aggErr = multierror.Append(aggErr, err)
	isPrimary, err = client.HasConnectedReplicas(ctx)
	aggErr = multierror.Append(aggErr, err)

	if err := aggErr.ErrorOrNil(); err != nil {
		return mariadbv1alpha1.ReplicationRoleUnknown, err
	}

	role := mariadbv1alpha1.ReplicationRoleUnknown
	if isPrimaryReplica {
		role = mariadbv1alpha1.ReplicationRolePrimaryReplica
	} else if isReplica {
		role = mariadbv1alpha1.ReplicationRoleReplica
	} else if isPrimary {
		role = mariadbv1alpha1.ReplicationRolePrimary
	}
	return role, nil
}

func (r *MariaDBReconciler) isMultiClusterPrimaryReplica(ctx context.Context, mdb *mariadbv1alpha1.MariaDB, podIndex int,
	client *sql.Client, logger logr.Logger) (bool, error) {
	if !mdb.IsMultiClusterPrimaryReplica(podIndex) {
		return false, nil
	}
	if mdb.IsReplicationEnabled() {
		return client.IsReplicationPrimaryReplica(
			ctx,
			logger,
			sql.WithConnectionName(replication.MultiClusterReplicaConnectionName),
		)
	} else if mdb.IsGaleraEnabled() {
		return client.IsReplicationRunning(
			ctx,
			logger,
			sql.WithConnectionName(replication.MultiClusterReplicaConnectionName),
		)
	}
	return false, nil
}

func shouldReconcileReplicaStatusForPod(mdb *mariadbv1alpha1.MariaDB, podIndex int) bool {
	isReplica := podIndex != *mdb.Status.CurrentPrimaryPodIndex
	replication := mdb.Replication()
	isExternalReplication := replication.IsExternalReplication()
	if mdb.IsMultiClusterEnabled() {
		if mdb.IsReplicationEnabled() {
			return isReplica || mdb.IsMultiClusterPrimaryReplica(podIndex)
		} else if mdb.IsGaleraEnabled() {
			return mdb.IsMultiClusterPrimaryReplica(podIndex)
		}
		return false
	}
	return isReplica || isExternalReplication
}

func (r *MariaDBReconciler) getReplicaStatus(ctx context.Context,
	mdb *mariadbv1alpha1.MariaDB, logger logr.Logger) (map[string]mariadbv1alpha1.ReplicaStatus, error) {
	replStatus := ptr.Deref(mdb.Status.Replication, mariadbv1alpha1.ReplicationStatus{})

	if mdb.Status.CurrentPrimaryPodIndex == nil {
		return replStatus.Replicas, nil
	}

	clientSet := sql.NewClientSet(mdb, r.RefResolver)
	defer clientSet.Close()

	var replicaStatus map[string]mariadbv1alpha1.ReplicaStatus
	replication := mdb.Replication()
	for i := 0; i < int(mdb.Spec.Replicas); i++ {
		if !shouldReconcileReplicaStatusForPod(mdb, i) {
			continue
		}

		if i == *mdb.Status.CurrentPrimaryPodIndex && !replication.IsExternalReplication() {
			continue
		}
		pod := stspkg.PodName(mdb.ObjectMeta, i)

		var currentReplicaStatus *mariadbv1alpha1.ReplicaStatus
		if current, ok := replStatus.Replicas[pod]; ok {
			currentReplicaStatus = &current
		}
		// when the Pods are restarted or unstable, SQL connections could fail, keep the current state
		preserveCurrentState := func() {
			if replicaStatus == nil {
				replicaStatus = make(map[string]mariadbv1alpha1.ReplicaStatus)
			}
			if currentReplicaStatus != nil {
				replicaStatus[pod] = *currentReplicaStatus
			}
		}

		client, err := clientSet.ClientForIndex(ctx, i)
		if err != nil {
			logger.V(1).Info("error getting client for Pod", "err", err, "pod", pod)
			preserveCurrentState()
			continue
		}

		var replOpts []sql.ReplicationOpt
		if mdb.IsMultiClusterPrimaryReplica(i) {
			replOpts = append(replOpts, sql.WithConnectionName(*replication.MultiClusterReplicaConnectionName))
		}
		newReplicaStatus, err := client.ReplicaStatus(ctx, logger, replOpts...)
		if err != nil {
			logger.V(1).Info("error checking Pod replica status", "err", err, "pod", pod)
			preserveCurrentState()
			continue
		}

		mergedReplicaStatus := mergeReplicaStatus(currentReplicaStatus, newReplicaStatus)
		if mergedReplicaStatus != nil {
			if replicaStatus == nil {
				replicaStatus = make(map[string]mariadbv1alpha1.ReplicaStatus)
			}
			replicaStatus[pod] = *mergedReplicaStatus
		}
	}
	return replicaStatus, nil
}

func (r *MariaDBReconciler) getMaxScalePrimaryPod(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) (*int, error) {
	if !mdb.IsMaxScaleEnabled() {
		return nil, nil
	}
	mxs, err := r.RefResolver.MaxScale(ctx, mdb.Spec.MaxScaleRef, mdb.Namespace)
	if err != nil {
		return nil, err
	}
	primarySrv := mxs.Status.GetPrimaryServer()
	if primarySrv == nil {
		return nil, errors.New("MaxScale primary server not found")
	}
	podIndex, err := podIndexForServer(*primarySrv, mxs, mdb)
	if err != nil {
		return nil, fmt.Errorf("error getting Pod for MaxScale server '%s': %v", *primarySrv, err)
	}
	return podIndex, nil
}

func (r *MariaDBReconciler) setUpdatedCondition(ctx context.Context, mdb *mariadbv1alpha1.MariaDB) error {
	var sts appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(mdb), &sts); err != nil {
		return err
	}
	if sts.Status.UpdateRevision == "" {
		return nil
	}

	pods, err := mdbpod.ListMariaDBPods(ctx, r.Client, mdb)
	if err != nil {
		return fmt.Errorf("error listing Pods: %v", err)
	}

	podsUpdated := 0
	for _, pod := range pods {
		if mdbpod.PodUpdated(&pod, sts.Status.UpdateRevision) {
			podsUpdated++
		}
	}

	logger := log.FromContext(ctx)

	if podsUpdated >= int(sts.Status.Replicas) {
		logger.V(1).Info("MariaDB is up to date")
		condition.SetUpdated(&mdb.Status)
	} else if podsUpdated > 0 {
		logger.V(1).Info("MariaDB update in progress")
		condition.SetUpdating(&mdb.Status)
	} else {
		logger.V(1).Info("MariaDB has a pending update")
		condition.SetPendingUpdate(&mdb.Status)
	}
	return nil
}

func mergeReplicaStatus(current *mariadbv1alpha1.ReplicaStatus,
	new *mariadbv1alpha1.ReplicaStatusVars) *mariadbv1alpha1.ReplicaStatus {
	if new == nil {
		return current
	}
	now := metav1.Now()
	// First report — initialize new status
	if current == nil {
		return &mariadbv1alpha1.ReplicaStatus{
			ReplicaStatusVars:       *new,
			LastErrorTransitionTime: now,
		}
	}
	// No error state change
	if current.EqualErrors(new) {
		return &mariadbv1alpha1.ReplicaStatus{
			ReplicaStatusVars:       *new,
			LastErrorTransitionTime: current.LastErrorTransitionTime,
		}
	}
	// Transition: healthy <-> error or changed error type
	return &mariadbv1alpha1.ReplicaStatus{
		ReplicaStatusVars:       *new,
		LastErrorTransitionTime: now,
	}
}

func podIndexForServer(serverName string, mxs *mariadbv1alpha1.MaxScale, mdb *mariadbv1alpha1.MariaDB) (*int, error) {
	var server *mariadbv1alpha1.MaxScaleServer
	for _, srv := range mxs.Spec.Servers {
		if serverName == srv.Name {
			server = &srv
			break
		}
	}
	if server == nil {
		return nil, fmt.Errorf("MaxScale server '%s' not found", serverName)
	}

	for i := 0; i < int(mdb.Spec.Replicas); i++ {
		address := stspkg.PodFQDNWithService(mdb.ObjectMeta, i, mdb.InternalServiceKey().Name)
		if server.Address == address {
			return &i, nil
		}
	}
	return nil, fmt.Errorf("MariaDB Pod with address '%s' not found", server.Address)
}

func defaultPrimary(mdb *mariadbv1alpha1.MariaDB) {
	if mdb.Status.CurrentPrimaryPodIndex != nil || mdb.Status.CurrentPrimary != nil {
		return
	}
	podIndex := 0
	if mdb.IsGaleraEnabled() {
		galera := ptr.Deref(mdb.Spec.Galera, mariadbv1alpha1.Galera{})
		podIndex = ptr.Deref(galera.Primary.PodIndex, 0)
	}
	if mdb.IsReplicationEnabled() {
		replication := ptr.Deref(mdb.Spec.Replication, mariadbv1alpha1.Replication{})
		podIndex = ptr.Deref(replication.Primary.PodIndex, 0)
	}
	mdb.Status.CurrentPrimaryPodIndex = &podIndex
	mdb.Status.CurrentPrimary = ptr.To(stspkg.PodName(mdb.ObjectMeta, podIndex))
}

func setMaxScalePrimary(mdb *mariadbv1alpha1.MariaDB, podIndex *int) {
	if !mdb.IsMaxScaleEnabled() || podIndex == nil {
		return
	}
	mdb.Status.CurrentPrimaryPodIndex = podIndex
	mdb.Status.CurrentPrimary = ptr.To(stspkg.PodName(mdb.ObjectMeta, *podIndex))
}
