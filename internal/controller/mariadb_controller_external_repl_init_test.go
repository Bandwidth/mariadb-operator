package controller

import (
	"fmt"
	"slices"
	"strconv"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/builder"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	sqlClient "github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/statefulset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var _ = Describe("MariaDB replication from external server", Ordered, func() {

	var (
		key           = testMdbERkey
		pbRecoveryKey = testMdbPbRecoveryERkey
		mdb           = &mariadbv1alpha1.MariaDB{}
	)

	It("should reconcile", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, testMdbERkey, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting to create a Service")
		var svc corev1.Service
		Expect(k8sClient.Get(testCtx, key, &svc)).To(Succeed())

		By("Expecting to create a primary Service")
		Expect(k8sClient.Get(testCtx, mdb.PrimaryServiceKey(), &svc)).To(Succeed())
		Expect(svc.Spec.Selector["statefulset.kubernetes.io/pod-name"]).To(Equal(statefulset.PodName(mdb.ObjectMeta, 0)))

		By("Expecting to create a secondary Service")
		Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &svc)).To(Succeed())

		By("Expecting role label to be set to primary")
		Eventually(func() bool {
			currentPrimary := *mdb.Status.CurrentPrimary
			primaryPodKey := types.NamespacedName{
				Name:      currentPrimary,
				Namespace: mdb.Namespace,
			}
			var primaryPod corev1.Pod
			if err := k8sClient.Get(testCtx, primaryPodKey, &primaryPod); err != nil {
				return apierrors.IsNotFound(err)
			}
			return primaryPod.Labels["k8s.mariadb.com/role"] == "primary"
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting Connection to be ready eventually")
		Eventually(func() bool {
			var conn mariadbv1alpha1.Connection
			if err := k8sClient.Get(testCtx, key, &conn); err != nil {
				return false
			}
			return conn.IsReady()
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting primary Connection to be ready eventually")
		Eventually(func() bool {
			var conn mariadbv1alpha1.Connection
			if err := k8sClient.Get(testCtx, mdb.PrimaryConnectioneKey(), &conn); err != nil {
				return false
			}
			return conn.IsReady()
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting secondary Connection to be ready eventually")
		Eventually(func() bool {
			var conn mariadbv1alpha1.Connection
			if err := k8sClient.Get(testCtx, mdb.SecondaryConnectioneKey(), &conn); err != nil {
				return false
			}
			return conn.IsReady()
		}, testTimeout, testInterval).Should(BeTrue())
		var endpoints discoveryv1.EndpointSlice

		By("Expecting to create secondary Endpoints: " + strconv.Itoa(int(mdb.Spec.Replicas)))
		Eventually(func() bool {
			Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &endpoints)).To(Succeed())
			count := 0
			for _, address := range endpoints.Endpoints {
				if *address.Conditions.Ready {
					count++
				}
			}
			return count == int(mdb.Spec.Replicas)

		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting to create a PodDisruptionBudget")
		var pdb policyv1.PodDisruptionBudget
		Expect(k8sClient.Get(testCtx, key, &pdb)).To(Succeed())

		By("Expecting the logical backup to inherit resources from the template")
		refResolver := refresolver.New(k8sClient)
		emdb, err := refResolver.ExternalMariaDB(testCtx, &mdb.Replication().ReplicaFromExternal.MariaDBRef.ObjectReference, testNamespace)
		Expect(err).To(Succeed())
		var logicalBackup mariadbv1alpha1.Backup
		Expect(k8sClient.Get(testCtx, types.NamespacedName{
			Name:      mdb.ExternalReplLogicalBackupName(),
			Namespace: emdb.Namespace,
		}, &logicalBackup)).To(Succeed())
		Expect(logicalBackup.Spec.Resources).NotTo(BeNil())
		Expect(logicalBackup.Spec.Resources.Limits.Cpu().String()).To(Equal("300m"))
		Expect(logicalBackup.Spec.Resources.Limits.Memory().String()).To(Equal("512Mi"))
		Expect(logicalBackup.Spec.Resources.Requests.Cpu().String()).To(Equal("100m"))
		Expect(logicalBackup.Spec.Resources.Requests.Memory().String()).To(Equal("128Mi"))

		By("Expecting each Restore to inherit resources from replica.bootstrapFrom.restoreJob")
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			var restore mariadbv1alpha1.Restore
			err := k8sClient.Get(testCtx, mdb.RestoreKeyInPod(i), &restore)
			if apierrors.IsNotFound(err) {
				continue
			}
			Expect(err).To(Succeed())
			Expect(restore.Spec.Resources).NotTo(BeNil())
			Expect(restore.Spec.Resources.Limits.Cpu().String()).To(Equal("300m"))
			Expect(restore.Spec.Resources.Limits.Memory().String()).To(Equal("512Mi"))
			Expect(restore.Spec.Resources.Requests.Cpu().String()).To(Equal("100m"))
			Expect(restore.Spec.Resources.Requests.Memory().String()).To(Equal("128Mi"))
		}
	})

	It("should recover if replication is broken", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady() && mdb.IsExternalReplInitialized()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting to get SqlClient from Pod 2")
		refResolver := refresolver.New(k8sClient)
		var client *sqlClient.Client
		var err error
		podIndex := 2
		client, err = sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, podIndex)
		Expect(err).To(Succeed())
		defer client.Close()

		By("Expecting to break replication on Pod 2")
		Expect(
			client.Exec(testCtx, "STOP SLAVE;"),
			client.Exec(testCtx, "RESET MASTER;"),
			client.Exec(testCtx, "RESET SLAVE;"),
			client.Exec(testCtx, "SET GLOBAL gtid_slave_pos='0-1-0';"),
			client.Exec(testCtx, "START SLAVE;"),
		).To(Succeed())

		By("Expecting replication to be ready eventually on Pod " + strconv.Itoa(podIndex))
		Eventually(func() bool {
			isReplicaHealthy, _ := client.IsReplicationHealthy(testCtx)
			return isReplicaHealthy
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting replication status to get back to slave Pod " + strconv.Itoa(podIndex))
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return apierrors.IsNotFound(err)
			}
			return (mdb.Status.Replication.Roles)[statefulset.PodName(mdb.ObjectMeta, podIndex)] == mariadbv1alpha1.ReplicationRoleReplica
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting MariaDB status to get back to running and Ready")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return apierrors.IsNotFound(err)
			}
			condition := meta.FindStatusCondition(mdb.Status.Conditions, mariadbv1alpha1.ConditionTypeReady)
			return condition != nil && condition.Status == metav1.ConditionTrue
		}, testHighTimeout, testInterval).Should(BeTrue())

		var endpoints discoveryv1.EndpointSlice
		By("Expecting Pod " + strconv.Itoa(podIndex) + " to present on the secondary endpoints")
		Eventually(func() bool {
			Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &endpoints)).To(Succeed())

			podKey := types.NamespacedName{
				Name:      statefulset.PodName(mdb.ObjectMeta, podIndex),
				Namespace: testNamespace,
			}
			var pod corev1.Pod
			Expect(k8sClient.Get(testCtx, podKey, &pod)).To(Succeed())

			for _, address := range endpoints.Endpoints {
				if address.Addresses[0] == pod.Status.PodIP && *address.Conditions.Ready {
					return true
				}
			}
			return false
		}, testTimeout, testInterval).Should(BeTrue())
	})

	It("should recover in case of missing GTID replication error (1236)", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting to get SqlClient from Pod 2")
		refResolver := refresolver.New(k8sClient)
		var client *sqlClient.Client
		var err error
		podIndex := 2
		client, err = sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, podIndex)
		Expect(err).To(Succeed())
		defer client.Close()

		By("Suspend MariaDB")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			mdb.Spec.Suspend = true

			return k8sClient.Update(testCtx, mdb) == nil
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting MariaDB to eventually be suspended")
		expectMariadbFn(testCtx, k8sClient, key, func(mdb *mariadbv1alpha1.MariaDB) bool {
			condition := meta.FindStatusCondition(mdb.Status.Conditions, mariadbv1alpha1.ConditionTypeReady)
			if condition == nil {
				return false
			}
			return condition.Status == metav1.ConditionFalse && condition.Reason == mariadbv1alpha1.ConditionReasonSuspended
		})

		By("Expecting to stop replication on Pod 2")
		Expect(client.Exec(testCtx, "STOP SLAVE")).To(Succeed(), client.Exec(testCtx, "SET GLOBAL gtid_slave_pos = '0-9999-9999'"))

		By("Expecting to set Invalid GTID position on Pod 2")
		Expect(client.Exec(testCtx, "SET GLOBAL gtid_slave_pos = '0-9999-9999'")).To(Succeed())

		By("Expecting to start replication on Pod 2")
		Expect(client.Exec(testCtx, "START SLAVE")).To(Succeed())

		By("Expecting replication error 1236 on Pod " + strconv.Itoa(podIndex))
		Eventually(func() bool {
			rStatus, err := client.GetReplicationStatus(testCtx)
			if err != nil {
				return false
			}
			return rStatus.LastIOErrno.Int32 == 1236

		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Resume MariaDB")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			mdb.Spec.Suspend = false

			return k8sClient.Update(testCtx, mdb) == nil
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting no replication error 1236 on Pod " + strconv.Itoa(podIndex))
		Eventually(func() bool {
			rStatus, err := client.GetReplicationStatus(testCtx)
			if err != nil {
				return false
			}
			return rStatus.LastIOErrno.Int32 != 1236

		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Expecting replication status to get back to slave Pod " + strconv.Itoa(podIndex))
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return apierrors.IsNotFound(err)
			}
			return (mdb.Status.Replication.Roles)[statefulset.PodName(mdb.ObjectMeta, podIndex)] == mariadbv1alpha1.ReplicationRoleReplica
		}, testHighTimeout, testInterval).Should(BeTrue())

		var endpoints discoveryv1.EndpointSlice
		By("Expecting Pod " + strconv.Itoa(podIndex) + " to present on the secondary endpoints")
		Eventually(func() bool {
			Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &endpoints)).To(Succeed())

			podKey := types.NamespacedName{
				Name:      statefulset.PodName(mdb.ObjectMeta, podIndex),
				Namespace: testNamespace,
			}
			var pod corev1.Pod
			Expect(k8sClient.Get(testCtx, podKey, &pod)).To(Succeed())

			for _, address := range endpoints.Endpoints {
				if address.Addresses[0] == pod.Status.PodIP && *address.Conditions.Ready {
					return true
				}
			}
			return false
		}, testTimeout, testInterval).Should(BeTrue())

	})

	It("should reuse physical backup if it still valid", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		// Get current backup age
		By("Expecting to get current external MariadDB object")
		refResolver := refresolver.New(k8sClient)
		emdb, err := refResolver.ExternalMariaDB(testCtx, &mdb.Replication().ReplicaFromExternal.MariaDBRef.ObjectReference, testNamespace)
		Expect(err).To(Succeed())

		emdbKey := types.NamespacedName{
			Name:      emdb.Name,
			Namespace: emdb.Namespace,
		}
		var existingLogicalBackup mariadbv1alpha1.Backup
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, emdbKey, &existingLogicalBackup)
		Expect(err).To(Succeed())
		firstLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

		var existingPhysicalBackup mariadbv1alpha1.PhysicalBackup
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
		Expect(err).To(Succeed())
		firstPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

		// Insert data on the external master to be sure that replica is not up to date with the master
		By("Expecting to get SqlClient from the external MariaDB")
		client, err := sqlClient.NewClientWithMariaDB(testCtx, emdb, refResolver)
		Expect(err).To(Succeed())
		defer client.Close()

		By("Expecting to insert data on the external master")
		Expect(
			client.Exec(testCtx, "CREATE DATABASE IF NOT EXISTS test;"),
			client.Exec(testCtx, "USE test;"),
			client.Exec(testCtx, "CREATE TABLE IF NOT EXISTS t (id INT PRIMARY KEY);"),
			client.Exec(testCtx, "INSERT INTO t VALUES (1);"),
			client.Exec(testCtx, "USE inttest;"),
			client.Exec(testCtx, "INSERT INTO t VALUES (1);"),
		).To(Succeed())
		podIndex := 1
		testDeletePod(mdb, podIndex, true)

		// Expect to get in recovering state eventually
		By("Expecting MariaDB to be in recovering state eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}

			return mdb.IsRecoveringReplicas()

		}, testHighTimeout, testInterval).Should(BeTrue())

		// Expect to get back to ready state eventually
		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()

			// return (mdb.Status.Replication.Roles)[statefulset.PodName(mdb.ObjectMeta, podIndex)] == mariadbv1alpha1.ReplicationRoleReplica
		}, testHighTimeout, testInterval).Should(BeTrue())

		// Get current Physical backup age
		By("Expecting to get physical backup object")
		err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
		Expect(err).To(Succeed())
		secondPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

		// Physical backup should not be updated as it's still valid
		By("Expecting to have same CreationTimestamp on physical backup Object before and after the Pod recreation")
		Expect(firstPhysicalBackupCreationTimestamp).To(Equal(secondPhysicalBackupCreationTimestamp))

		// Get current backup age
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, emdbKey, &existingLogicalBackup)
		Expect(err).To(Succeed())
		secondLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

		// Last age should be older than first
		By("Expecting to have same CreationTimestamp on backup Object before and after the Pod recreation")
		Expect(firstLogicalBackupCreationTimestamp).To(Equal(secondLogicalBackupCreationTimestamp))

	})

	It("should invalidate physical backup if older than the master binlog retention period", func() {
		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		// Get current backup age
		By("Expecting to get current external MariadDB object")
		refResolver := refresolver.New(k8sClient)
		emdb, err := refResolver.ExternalMariaDB(testCtx, &mdb.Replication().ReplicaFromExternal.MariaDBRef.ObjectReference, testNamespace)
		Expect(err).To(Succeed())

		logicalBackupKey := types.NamespacedName{
			Name:      emdb.Name,
			Namespace: emdb.Namespace,
		}

		var existingLogicalBackup mariadbv1alpha1.Backup
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, logicalBackupKey, &existingLogicalBackup)
		Expect(err).To(Succeed())
		firstLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

		var existingPhysicalBackup mariadbv1alpha1.PhysicalBackup
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
		Expect(err).To(Succeed())
		firstPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

		podIndex := 2

		// Change binlog_expire_logs_seconds to 10 on the master server
		By("Expecting to get SqlClient from the external MariaDB")
		client, err := sqlClient.NewClientWithMariaDB(testCtx, emdb, refResolver)
		Expect(err).To(Succeed())
		defer client.Close()

		By("Expecting to set binlog_expire_logs_seconds to 10 on the master server")
		Expect(client.SetSystemVariable(testCtx, "binlog_expire_logs_seconds", "10")).To(Succeed())

		testDeletePod(mdb, podIndex, true)

		By("Expecting MariaDB to be in recovering state eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}

			return mdb.IsRecoveringReplicas()

		}, testHighTimeout, testInterval).Should(BeTrue())

		// Revert binlog_expire_logs_seconds to 30 days on the master server
		By("Expecting to set expire_logs_days to 30 on the master server")
		Expect(client.SetSystemVariable(testCtx, "expire_logs_days", "30")).To(Succeed())

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()

			// return (mdb.Status.Replication.Roles)[statefulset.PodName(mdb.ObjectMeta, podIndex)] == mariadbv1alpha1.ReplicationRoleReplica
		}, testHighTimeout, testInterval).Should(BeTrue())

		// Get current Physical backup age
		By("Expecting to get physical backup object")
		err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
		Expect(err).To(Succeed())
		secondPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

		// Physical backup should be updated as it's older than the master binlog retention period
		By("Expecting to have different CreationTimestamp on physical backup Object before and after the Pod recreation")
		Expect(firstPhysicalBackupCreationTimestamp).ShouldNot(Equal(secondPhysicalBackupCreationTimestamp))

		// Get current backup age
		By("Expecting to get backup object")
		err = k8sClient.Get(testCtx, logicalBackupKey, &existingLogicalBackup)
		Expect(err).To(Succeed())
		secondLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

		// Logical backup should should not be touched as the cluster still has valid replicas for a physical backup
		By("Expecting to have same CreationTimestamp on backup Object before and after the Pod recreation")
		Expect(firstLogicalBackupCreationTimestamp).Should(Equal(secondLogicalBackupCreationTimestamp))

		// Revert binlog_expire_logs_seconds to 30 days on the master server
		By("Expecting to set expire_logs_days to 30 on the master server")
		Expect(client.SetSystemVariable(testCtx, "expire_logs_days", "30")).To(Succeed())
	})

	It("should invalidate logical backup if older than the master binlog retention period and no phy backup is avail and no valid replicas",
		func() {
			By("Expecting MariaDB to be ready eventually")
			Eventually(func() bool {
				if err := k8sClient.Get(testCtx, key, mdb); err != nil {
					return false
				}
				return mdb.IsReady()
			}, testHighTimeout, testInterval).Should(BeTrue())

			// Get current backup age
			By("Expecting to get current external MariadDB object")
			refResolver := refresolver.New(k8sClient)
			emdb, err := refResolver.ExternalMariaDB(testCtx, &mdb.Replication().ReplicaFromExternal.MariaDBRef.ObjectReference, testNamespace)
			Expect(err).To(Succeed())

			logicalBackupKey := types.NamespacedName{
				Name:      emdb.Name,
				Namespace: emdb.Namespace,
			}

			var existingLogicalBackup mariadbv1alpha1.Backup
			By("Expecting to get backup object")
			err = k8sClient.Get(testCtx, logicalBackupKey, &existingLogicalBackup)
			Expect(err).To(Succeed())
			firstLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

			var existingPhysicalBackup mariadbv1alpha1.PhysicalBackup
			By("Expecting to get backup object")
			err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
			Expect(err).To(Succeed())
			firstPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

			// Change binlog_expire_logs_seconds to 10 on the master server
			By("Expecting to get SqlClient from the external MariaDB")
			client, err := sqlClient.NewClientWithMariaDB(testCtx, emdb, refResolver)
			Expect(err).To(Succeed())
			defer client.Close()

			By("Expecting to set binlog_expire_logs_seconds to 30 on the master server")
			Expect(client.SetSystemVariable(testCtx, "binlog_expire_logs_seconds", "30")).To(Succeed())

			// Delete physical backup to be sure that only logical backup is available
			By("Expecting to delete physical backup")
			Expect(k8sClient.Delete(testCtx, &existingPhysicalBackup)).To(Succeed())

			// Delete all replicas to be sure that no valid replica exists for a physical backup
			for i := 0; i < int(mdb.Spec.Replicas); i++ {
				testDeletePod(mdb, i, true)
			}

			By("Expecting MariaDB to be in recovering state eventually")
			Eventually(func() bool {
				if err := k8sClient.Get(testCtx, key, mdb); err != nil {
					return false
				}
				return mdb.IsRecoveringReplicas()

			}, testHighTimeout, testInterval).Should(BeTrue())

			By("Expecting Logical backup to replaced eventually")
			Eventually(func() bool {
				if err := k8sClient.Get(testCtx, logicalBackupKey, &existingLogicalBackup); err != nil {
					return false
				}
				secondLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time
				return secondLogicalBackupCreationTimestamp.After(firstLogicalBackupCreationTimestamp)
			}, testHighTimeout, testInterval).Should(BeTrue())

			// Revert binlog_expire_logs_seconds to 30 days on the master server to avoid issues with other tests
			Expect(client.SetSystemVariable(testCtx, "expire_logs_days", "30")).To(Succeed())

			By("Expecting MariaDB to be ready eventually")
			Eventually(func() bool {
				if err := k8sClient.Get(testCtx, key, mdb); err != nil {
					return false
				}
				return mdb.IsReady()

				// return (mdb.Status.Replication.Roles)[statefulset.PodName(mdb.ObjectMeta, podIndex)] == mariadbv1alpha1.ReplicationRoleReplica
			}, testVeryHighTimeout, testInterval).Should(BeTrue())

			// Get current Physical backup age
			By("Expecting to get physical backup object")
			err = k8sClient.Get(testCtx, pbRecoveryKey, &existingPhysicalBackup)
			Expect(err).To(Succeed())
			secondPhysicalBackupCreationTimestamp := existingPhysicalBackup.CreationTimestamp.Time

			// Physical backup should be updated as it's older than the master binlog retention period
			By("Expecting to have different CreationTimestamp on physical backup Object before and after the Pod recreation")
			Expect(firstPhysicalBackupCreationTimestamp).ShouldNot(Equal(secondPhysicalBackupCreationTimestamp))

			// Get current backup age
			By("Expecting to get backup object")
			err = k8sClient.Get(testCtx, logicalBackupKey, &existingLogicalBackup)
			Expect(err).To(Succeed())
			secondLogicalBackupCreationTimestamp := existingLogicalBackup.CreationTimestamp.Time

			// Logical backup should be updated as it's older than the master binlog retention period and no physical backup
			// is available and no valid replicas exist
			By("Expecting to have different CreationTimestamp on backup Object before and after the Pod recreation")
			Expect(firstLogicalBackupCreationTimestamp).ShouldNot(Equal(secondLogicalBackupCreationTimestamp))

			// Revert binlog_expire_logs_seconds to 30 days on the master server
			By("Expecting to set expire_logs_days to 30 on the master server")
			Expect(client.SetSystemVariable(testCtx, "expire_logs_days", "30")).To(Succeed())
		})

	It("scale out replicas", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Increasing MariaDB replicas to 4")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			mdb.Spec.Replicas = 4

			return k8sClient.Update(testCtx, mdb) == nil
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		var endpoints discoveryv1.EndpointSlice
		By("Expecting to create secondary Endpoints: 4")
		Eventually(func() bool {
			Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &endpoints)).To(Succeed())
			count := 0
			for _, address := range endpoints.Endpoints {
				if *address.Conditions.Ready {
					count++
				}
			}
			return count == 4
		}, testTimeout, testInterval).Should(BeTrue())

	})

	It("scale in replicas", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Decreasing MariaDB replicas to 3")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			mdb.Spec.Replicas = 3

			return k8sClient.Update(testCtx, mdb) == nil
		}, testTimeout, testInterval).Should(BeTrue())

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		var endpoints discoveryv1.EndpointSlice
		By("Expecting secondary Endpoints: 3")
		Eventually(func() bool {
			Expect(k8sClient.Get(testCtx, mdb.SecondaryServiceKey(), &endpoints)).To(Succeed())
			count := 0
			for _, address := range endpoints.Endpoints {
				if *address.Conditions.Ready {
					count++
				}
			}
			return count == 3
		}, testTimeout, testInterval).Should(BeTrue())

	})

	It("use the server_id offset", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testHighTimeout, testInterval).Should(BeTrue())

		offset := mdb.Replication().ReplicaFromExternal.ServerIdOffset
		replicas := int(mdb.Spec.Replicas)
		refResolver := refresolver.New(k8sClient)
		for i := 0; i < replicas; i++ {

			client, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
			By("Expecting to get SqlClient from Pod " + strconv.Itoa(i))
			Expect(err).To(Succeed())

			server_id, err := client.SystemVariable(testCtx, "server_id")
			By("Expecting to get server_id from Pod " + strconv.Itoa(i))
			Expect(err).To(Succeed())

			server_id_int, _ := strconv.Atoi(server_id)

			By("Expecting server_id to be equal to podIndex + ServerIdOffset on Pod " + strconv.Itoa(i))
			Expect(server_id_int).To(Equal(i + *offset))

		}
	})

	It("should update", func() {
		By("Updating MariaDB")
		testMariadbUpdate(mdb)
	})

	It("should resize PVCs", func() {
		By("Resizing MariaDB PVCs")
		testMariadbVolumeResize(mdb, "400Mi")
	})

	It("should heal external master connection drift", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady() && mdb.IsExternalReplInitialized() && !mdb.IsRecoveringReplicas()
		}, testHighTimeout, testInterval).Should(BeTrue())

		By("Getting the desired external master host")
		var emdb mariadbv1alpha1.ExternalMariaDB
		Expect(k8sClient.Get(testCtx, testEMdbkey, &emdb)).To(Succeed())
		desiredHost := emdb.GetHost()
		Expect(desiredHost).NotTo(BeEmpty())

		// RFC 5737 TEST-NET-1 address, guaranteed not to be the real external master.
		const bogusHost = "192.0.2.123"

		By("Pointing every replica at a bogus master to simulate connection drift")
		refResolver := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {

			var podClient *sqlClient.Client
			Eventually(func() error {
				var err error
				podClient, err = sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
				return err
			}, testTimeout, testInterval).Should(Succeed())
			defer podClient.Close()

			Expect(podClient.StopAllSlaves(testCtx)).To(Succeed())
			Expect(podClient.Exec(testCtx, fmt.Sprintf("CHANGE MASTER TO MASTER_HOST='%s';", bogusHost))).To(Succeed())

			By(fmt.Sprintf("Verifying Pod %d master host has drifted", i))
			status, err := podClient.QueryColumnMap(testCtx, "SHOW REPLICA STATUS")
			Expect(err).To(Succeed())
			Expect(status["Master_Host"]).To(Equal(bogusHost))
		}

		By("Expecting the operator to re-point every replica at the external master and resume replication")
		refResolver2 := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver2, i)
			Expect(err).To(Succeed())
			defer podClient.Close()

			Eventually(func(g Gomega) {
				status, err := podClient.QueryColumnMap(testCtx, "SHOW REPLICA STATUS")
				g.Expect(err).To(Succeed())
				g.Expect(status["Master_Host"]).To(Equal(desiredHost))
				g.Expect(status["Slave_IO_Running"]).To(Equal("Yes"))
				g.Expect(status["Slave_SQL_Running"]).To(Equal("Yes"))
			}, testHighTimeout, testInterval).Should(Succeed(),
				fmt.Sprintf("Pod %d should be re-pointed at the external master", i))
		}
	})

	It("should re-apply the replication password on an authentication error", func() {

		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady() && mdb.IsExternalReplInitialized()
		}, testHighTimeout, testInterval).Should(BeTrue())

		// The master host and user are left untouched: only the password is broken. This exercises
		// the authentication-error repair path specifically, since no host/port/user drift exists.
		By("Breaking the replication credentials on every replica to trigger an authentication error")
		refResolver := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
			Expect(err).To(Succeed())
			defer podClient.Close()

			Expect(podClient.StopAllSlaves(testCtx)).To(Succeed())
			Expect(podClient.Exec(testCtx, "CHANGE MASTER TO MASTER_PASSWORD='wrong-password';")).To(Succeed())
			Expect(podClient.StartSlave(testCtx)).To(Succeed())
		}

		By("Expecting the operator to re-apply the credentials and restore healthy replication")
		refResolver2 := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver2, i)
			Expect(err).To(Succeed())
			defer podClient.Close()

			Eventually(func(g Gomega) {
				status, err := podClient.QueryColumnMap(testCtx, "SHOW REPLICA STATUS")
				g.Expect(err).To(Succeed())
				g.Expect(status["Slave_IO_Running"]).To(Equal("Yes"))
				g.Expect(status["Slave_SQL_Running"]).To(Equal("Yes"))
			}, testHighTimeout, testInterval).Should(Succeed(),
				fmt.Sprintf("Pod %d should resume replication after credential repair", i))
		}
	})

})

var _ = Describe("MariaDB replication from external server with server_id offset auto-discovery", Ordered, func() {
	var (
		// Two ExternalMariaDBs referencing the same HA testEmulateExternalMdb: one at its primary
		// service (master endpoint) and one at its secondary service (slave endpoint). Both reuse the
		// emulated-external credentials/TLS, so the operator can follow the secondary endpoint to the
		// primary with the same connection settings.
		emdbMasterKey = types.NamespacedName{Name: "emdb-autodisc-master", Namespace: testNamespace}
		emdbSlaveKey  = types.NamespacedName{Name: "emdb-autodisc-slave", Namespace: testNamespace}

		// mdbFromMaster replicates from the primary endpoint and is fully bootstrapped, so its replica
		// server_ids register on the external primary. mdbFromSlave then replicates from the secondary
		// endpoint and must discover a higher, non-colliding offset by following to the same primary.
		mdbFromMasterKey = types.NamespacedName{Name: "mdb-autodisc-master", Namespace: testNamespace}
		mdbFromSlaveKey  = types.NamespacedName{Name: "mdb-autodisc-slave", Namespace: testNamespace}

		pbFromMasterKey = types.NamespacedName{Name: mdbFromMasterKey.Name + "-backup-template", Namespace: testNamespace}
		pbFromSlaveKey  = types.NamespacedName{Name: mdbFromSlaveKey.Name + "-backup-template", Namespace: testNamespace}

		externalPrimaryHost   = fmt.Sprintf("%s-primary.%s.svc.cluster.local", testEmulateExternalMdbkey.Name, testNamespace)
		externalSecondaryHost = fmt.Sprintf("%s-secondary.%s.svc.cluster.local", testEmulateExternalMdbkey.Name, testNamespace)

		// server_ids already in use on testEmulateExternalMdb (its own nodes), captured before any of
		// the clusters under test connect. The discovered offsets must not collide with these.
		externalServerIds []int
		masterOffset      int
	)

	BeforeAll(func() {
		By("Capturing the server_ids already in use on the external primary")
		var extMdb mariadbv1alpha1.MariaDB
		Expect(k8sClient.Get(testCtx, testEmulateExternalMdbkey, &extMdb)).To(Succeed())
		extClient, err := sqlClient.NewClientWithMariaDB(testCtx, &extMdb, testRefResolver)
		Expect(err).To(Succeed())
		defer extClient.Close()
		externalServerIds, err = extClient.InUseServerIds(testCtx)
		Expect(err).To(Succeed())
		Expect(externalServerIds).NotTo(BeEmpty())

		By("Creating the ExternalMariaDB pointing at the external primary service (master endpoint)")
		Expect(k8sClient.Create(testCtx, buildAutodiscExternalMariaDB(emdbMasterKey, externalPrimaryHost))).To(Succeed())
		expectExternalMariadbReady(testCtx, k8sClient, emdbMasterKey)

		By("Creating the physical backup template and the master-endpoint cluster (serverIdOffset unset)")
		Expect(k8sClient.Create(testCtx, buildAutodiscBackupTemplate(pbFromMasterKey, mdbFromMasterKey))).To(Succeed())
		Expect(k8sClient.Create(testCtx, buildAutodiscReplica(mdbFromMasterKey, emdbMasterKey, pbFromMasterKey))).To(Succeed())

		DeferCleanup(func() {
			deleteMariadb(mdbFromMasterKey, false)
			deleteMariadb(mdbFromSlaveKey, false)
			deleteExternalMariadbIfExists(emdbMasterKey)
			deleteExternalMariadbIfExists(emdbSlaveKey)
			deletePhysicalBackupIfExists(pbFromMasterKey)
			deletePhysicalBackupIfExists(pbFromSlaveKey)
		})
	})

	It("should auto-discover the offset from a master endpoint", func() {
		By("Expecting the discovered offset to be persisted to status")
		var mdb mariadbv1alpha1.MariaDB
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(testCtx, mdbFromMasterKey, &mdb)).To(Succeed())
			g.Expect(mdb.Status.ExternalReplication).NotTo(BeNil())
			g.Expect(mdb.Status.ExternalReplication.ServerIdOffset).NotTo(BeNil())
		}, testHighTimeout, testInterval).Should(Succeed())
		masterOffset = *mdb.Status.ExternalReplication.ServerIdOffset

		By("Expecting the offset to be the highest external server_id plus the gap")
		Expect(masterOffset >= slices.Max(externalServerIds)+externalReplServerIdGap).To(
			BeTrue(),
			"discovered offset %d should be higher than the highest external server_id %d plus the gap %d",
			masterOffset,
			slices.Max(externalServerIds),
			externalReplServerIdGap,
		)

		By("Expecting the spec serverIdOffset to remain unset (discovery lives in status)")
		Expect(mdb.Replication().ReplicaFromExternal.ServerIdOffset).To(BeNil())
		Expect(ptr.Deref(mdb.ExternalReplServerIdOffset(), 0)).To(Equal(masterOffset))

		By("Expecting the StatefulSet to carry the discovered offset as the server_id offset env var")
		Eventually(func(g Gomega) {
			var sts appsv1.StatefulSet
			g.Expect(k8sClient.Get(testCtx, mdbFromMasterKey, &sts)).To(Succeed())
			value, ok := autodiscContainerEnv(&sts, builder.MariadbContainerName, "MARIADB_EXTERNAL_REPL_SERVER_ID_OFFSET")
			g.Expect(ok).To(BeTrue())
			g.Expect(value).To(Equal(strconv.Itoa(masterOffset)))
		}, testHighTimeout, testInterval).Should(Succeed())
	})

	It("should apply the discovered offset to the replica server_ids", func() {
		By("Expecting the master-endpoint cluster to be ready eventually")
		var mdb mariadbv1alpha1.MariaDB
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, mdbFromMasterKey, &mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testVeryHighTimeout, testInterval).Should(BeTrue())

		By("Expecting server_id to equal podIndex + discovered offset on every replica")
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, &mdb, testRefResolver, i)
			Expect(err).To(Succeed())
			defer podClient.Close()

			serverID, err := podClient.SystemVariable(testCtx, "server_id")
			Expect(err).To(Succeed())
			serverIDInt, err := strconv.Atoi(serverID)
			Expect(err).To(Succeed())
			Expect(serverIDInt).To(Equal(i + masterOffset))
		}
	})

	It("should discover a higher, non-colliding offset from a slave endpoint by following it to the primary", func() {
		By("Waiting until the master-endpoint cluster's replica server_ids are registered on the external primary")
		var extMdb mariadbv1alpha1.MariaDB
		Expect(k8sClient.Get(testCtx, testEmulateExternalMdbkey, &extMdb)).To(Succeed())
		primaryClient, err := sqlClient.NewClientWithMariaDB(testCtx, &extMdb, testRefResolver)
		Expect(err).To(Succeed())
		defer primaryClient.Close()

		masterCluster := &mariadbv1alpha1.MariaDB{}
		Expect(k8sClient.Get(testCtx, mdbFromMasterKey, masterCluster)).To(Succeed())
		masterIds := make([]int, 0, masterCluster.Spec.Replicas)
		for i := 0; i < int(masterCluster.Spec.Replicas); i++ {
			masterIds = append(masterIds, i+masterOffset)
		}
		Eventually(func(g Gomega) {
			ids, err := primaryClient.InUseServerIds(testCtx)
			g.Expect(err).To(Succeed())
			g.Expect(ids).To(ContainElements(masterIds))
		}, testVeryHighTimeout, testInterval).Should(Succeed())

		By("Capturing the server_ids in use on the external primary before creating the slave-endpoint cluster")
		primaryIds, err := primaryClient.InUseServerIds(testCtx)
		Expect(err).To(Succeed())
		expectedSlaveOffset := slices.Max(primaryIds) + externalReplServerIdGap

		By("Creating the ExternalMariaDB pointing at the external secondary service (slave endpoint)")
		Expect(k8sClient.Create(testCtx, buildAutodiscExternalMariaDB(emdbSlaveKey, externalSecondaryHost))).To(Succeed())
		expectExternalMariadbReady(testCtx, k8sClient, emdbSlaveKey)

		By("Creating the physical backup template and the slave-endpoint cluster (serverIdOffset unset)")
		Expect(k8sClient.Create(testCtx, buildAutodiscBackupTemplate(pbFromSlaveKey, mdbFromSlaveKey))).To(Succeed())
		Expect(k8sClient.Create(testCtx, buildAutodiscReplica(mdbFromSlaveKey, emdbSlaveKey, pbFromSlaveKey))).To(Succeed())

		By("Expecting the slave-endpoint cluster to discover the offset derived from the primary")
		var mdb mariadbv1alpha1.MariaDB
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(testCtx, mdbFromSlaveKey, &mdb)).To(Succeed())
			g.Expect(mdb.Status.ExternalReplication).NotTo(BeNil())
			g.Expect(mdb.Status.ExternalReplication.ServerIdOffset).NotTo(BeNil())
		}, testHighTimeout, testInterval).Should(Succeed())
		slaveOffset := *mdb.Status.ExternalReplication.ServerIdOffset
		Expect(slaveOffset).To(Equal(expectedSlaveOffset))

		By("Expecting the slave-endpoint offset to be higher than the master-endpoint one")
		Expect(slaveOffset).To(BeNumerically(">", masterOffset))

		By("Expecting the server_ids of both clusters not to collide with each other nor with the external servers")
		slaveIds := make([]int, 0, mdb.Spec.Replicas)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			slaveIds = append(slaveIds, i+slaveOffset)
		}
		Expect(autodiscDisjoint(masterIds, slaveIds)).To(BeTrue(), "master and slave server_ids must not collide")
		Expect(autodiscDisjoint(masterIds, externalServerIds)).To(BeTrue(), "master server_ids must not collide with the external servers")
		Expect(autodiscDisjoint(slaveIds, externalServerIds)).To(BeTrue(), "slave server_ids must not collide with the external servers")

		By("Expecting the StatefulSet to carry the discovered offset as the server_id offset env var")
		Eventually(func(g Gomega) {
			var sts appsv1.StatefulSet
			g.Expect(k8sClient.Get(testCtx, mdbFromSlaveKey, &sts)).To(Succeed())
			value, ok := autodiscContainerEnv(&sts, builder.MariadbContainerName, "MARIADB_EXTERNAL_REPL_SERVER_ID_OFFSET")
			g.Expect(ok).To(BeTrue())
			g.Expect(value).To(Equal(strconv.Itoa(slaveOffset)))
		}, testHighTimeout, testInterval).Should(Succeed())
	})
})

// buildAutodiscExternalMariaDB builds an ExternalMariaDB pointing at host, reusing the emulated
// external credentials and TLS material. Both endpoints belong to the same testEmulateExternalMdb
// cluster (one CA), so the operator can follow the secondary endpoint to the primary with the same
// TLS settings.
func buildAutodiscExternalMariaDB(key types.NamespacedName, host string) *mariadbv1alpha1.ExternalMariaDB {
	return &mariadbv1alpha1.ExternalMariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Spec: mariadbv1alpha1.ExternalMariaDBSpec{
			Host:     host,
			Username: ptr.To("root"),
			PasswordSecretKeyRef: &mariadbv1alpha1.SecretKeySelector{
				LocalObjectReference: mariadbv1alpha1.LocalObjectReference{
					Name: testEmulatedExternalPwdKey.Name,
				},
				Key: testPwdSecretKey,
			},
			TLS: &mariadbv1alpha1.ExternalTLS{
				TLS: mariadbv1alpha1.TLS{
					Enabled:  true,
					Required: ptr.To(false),
					ServerCASecretRef: &mariadbv1alpha1.LocalObjectReference{
						Name: "mdb-emulate-external-test-ca",
					},
					ClientCertSecretRef: &mariadbv1alpha1.LocalObjectReference{
						Name: "mdb-emulate-external-test-client-cert",
					},
					ServerCertSecretRef: &mariadbv1alpha1.LocalObjectReference{
						Name: "mdb-emulate-external-test-server-cert",
					},
				},
			},
		},
	}
}

// buildAutodiscBackupTemplate builds the physical backup template that a cluster under test bootstraps
// from. External replication requires a bootstrap source; the offset is discovered in the status phase
// well before the bootstrap runs.
func buildAutodiscBackupTemplate(key, mdbKey types.NamespacedName) *mariadbv1alpha1.PhysicalBackup {
	return &mariadbv1alpha1.PhysicalBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Spec: mariadbv1alpha1.PhysicalBackupSpec{
			MariaDBRef: mariadbv1alpha1.MariaDBRef{
				ObjectReference: mariadbv1alpha1.ObjectReference{
					Name: mdbKey.Name,
				},
				Kind:      mariadbv1alpha1.ExternalMariaDBKind,
				WaitForIt: false,
			},
			Target:      ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
			Schedule:    &mariadbv1alpha1.PhysicalBackupSchedule{Suspend: true},
			Compression: mariadbv1alpha1.CompressBzip2,
			Storage: mariadbv1alpha1.PhysicalBackupStorage{
				PersistentVolumeClaim: &mariadbv1alpha1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				},
			},
			Timeout:     &metav1.Duration{Duration: 1 * time.Hour},
			PodAffinity: ptr.To(true),
			JobContainerTemplate: mariadbv1alpha1.JobContainerTemplate{
				Resources: &mariadbv1alpha1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("300m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
			},
		},
	}
}

// buildAutodiscReplica builds a MariaDB that replicates from the given external endpoint with
// serverIdOffset intentionally unset, so the operator must auto-discover it.
func buildAutodiscReplica(key, emdbKey, pbKey types.NamespacedName) *mariadbv1alpha1.MariaDB {
	mdb := &mariadbv1alpha1.MariaDB{
		ObjectMeta: metav1.ObjectMeta{
			Name:      key.Name,
			Namespace: key.Namespace,
		},
		Spec: mariadbv1alpha1.MariaDBSpec{
			Username: &testUser,
			PasswordSecretKeyRef: &mariadbv1alpha1.GeneratedSecretKeyRef{
				SecretKeySelector: mariadbv1alpha1.SecretKeySelector{
					LocalObjectReference: mariadbv1alpha1.LocalObjectReference{
						Name: testPwdKey.Name,
					},
					Key: testPwdSecretKey,
				},
			},
			Database: &testDatabase,
			MyCnf: ptr.To(`[mariadb]
bind-address=*
default_storage_engine=InnoDB
binlog_format=row
innodb_autoinc_lock_mode=2
max_allowed_packet=256M`),
			Replication: &mariadbv1alpha1.Replication{
				ReplicationSpec: mariadbv1alpha1.ReplicationSpec{
					ReplicaFromExternal: &mariadbv1alpha1.ReplicaFromExternal{
						MariaDBRef: mariadbv1alpha1.MariaDBRef{
							ObjectReference: mariadbv1alpha1.ObjectReference{
								Name: emdbKey.Name,
							},
							Kind: mariadbv1alpha1.ExternalMariaDBKind,
						},
						// ServerIdOffset intentionally left unset to exercise auto-discovery.
					},
					Replica: mariadbv1alpha1.ReplicaReplication{
						ReplicaBootstrapFrom: &mariadbv1alpha1.ReplicaBootstrapFrom{
							PhysicalBackupTemplateRef: mariadbv1alpha1.LocalObjectReference{
								Name: pbKey.Name,
							},
						},
						IgnoreMaxLagSeconds:             ptr.To(true),
						IgnoreReplicationLivenessProbes: ptr.To(true),
					},
				},
				Enabled: true,
			},
			Replicas: 2,
			Storage: mariadbv1alpha1.Storage{
				Size:             ptr.To(resource.MustParse("300Mi")),
				StorageClassName: "standard-resize",
			},
			TLS: &mariadbv1alpha1.TLS{
				Enabled:  true,
				Required: ptr.To(true),
			},
		},
	}
	return applyMariadbTestConfig(mdb)
}

// autodiscContainerEnv returns the value of the named env var on the named container of the StatefulSet.
func autodiscContainerEnv(sts *appsv1.StatefulSet, containerName, envName string) (string, bool) {
	for _, c := range sts.Spec.Template.Spec.Containers {
		if c.Name != containerName {
			continue
		}
		for _, e := range c.Env {
			if e.Name == envName {
				return e.Value, true
			}
		}
	}
	return "", false
}

// autodiscDisjoint reports whether the two server_id sets have no element in common.
func autodiscDisjoint(a, b []int) bool {
	for _, id := range a {
		if slices.Contains(b, id) {
			return false
		}
	}
	return true
}

// deleteExternalMariadbIfExists deletes an ExternalMariaDB, ignoring a not-found error.
func deleteExternalMariadbIfExists(key types.NamespacedName) {
	var emdb mariadbv1alpha1.ExternalMariaDB
	if err := k8sClient.Get(testCtx, key, &emdb); err == nil {
		Expect(k8sClient.Delete(testCtx, &emdb)).To(Succeed())
	}
}

// deletePhysicalBackupIfExists deletes a PhysicalBackup, ignoring a not-found error.
func deletePhysicalBackupIfExists(key types.NamespacedName) {
	var pb mariadbv1alpha1.PhysicalBackup
	if err := k8sClient.Get(testCtx, key, &pb); err == nil {
		Expect(k8sClient.Delete(testCtx, &pb)).To(Succeed())
	}
}
