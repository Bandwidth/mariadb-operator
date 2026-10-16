package controller

import (
	"fmt"
	"strconv"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/refresolver"
	sqlClient "github.com/mariadb-operator/mariadb-operator/v26/pkg/sql"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

var _ = Describe("MariaDB replication from external server with ignored tables", Ordered, func() {
	const (
		ignoredDB     = "ignoreddb"
		replicatedTbl = "replicated_table"
		ignoredTbl    = "ignored_table"
		otherDB       = "ignoredotherdb"
		otherTbl      = "other_table"
	)

	var (
		key = testMdbERIgnoredKey
		mdb = &mariadbv1alpha1.MariaDB{}
	)

	tableExists := func(c *sqlClient.Client, db, table string) bool {
		exists, err := c.Exists(testCtx, fmt.Sprintf(
			"SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'", db, table,
		))
		Expect(err).To(Succeed())
		return exists
	}

	waitForReady := func() {
		By("Expecting MariaDB to be ready eventually")
		Eventually(func() bool {
			if err := k8sClient.Get(testCtx, key, mdb); err != nil {
				return false
			}
			return mdb.IsReady()
		}, testVeryHighTimeout, testInterval).Should(BeTrue())
	}

	BeforeAll(func() {
		By("Getting the external MariaDB client")
		var emdb mariadbv1alpha1.ExternalMariaDB
		Expect(k8sClient.Get(testCtx, testEMdbkey, &emdb)).To(Succeed())
		refResolver := refresolver.New(k8sClient)
		externalClient, err := sqlClient.NewClientWithMariaDB(testCtx, &emdb, refResolver)
		Expect(err).To(Succeed())
		defer externalClient.Close()

		By("Creating tables on the external server")
		Expect(externalClient.Exec(testCtx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", ignoredDB))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (id INT PRIMARY KEY)",
			ignoredDB, replicatedTbl))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (id INT PRIMARY KEY)",
			ignoredDB, ignoredTbl))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", otherDB))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (id INT PRIMARY KEY)",
			otherDB, otherTbl))).To(Succeed())

		By("Creating PhysicalBackup template for ignored tables external replication recovery")
		backupTemplate := mariadbv1alpha1.PhysicalBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testPbTemplateERIgnoredKey.Name,
				Namespace: testPbTemplateERIgnoredKey.Namespace,
			},
			Spec: mariadbv1alpha1.PhysicalBackupSpec{
				MariaDBRef: mariadbv1alpha1.MariaDBRef{
					ObjectReference: mariadbv1alpha1.ObjectReference{
						Name: testMdbERIgnoredKey.Name,
					},
					Kind:      mariadbv1alpha1.ExternalMariaDBKind,
					WaitForIt: false,
				},
				Target: ptr.To(mariadbv1alpha1.PhysicalBackupTargetPreferReplica),
				Schedule: &mariadbv1alpha1.PhysicalBackupSchedule{
					Suspend: true,
				},
				Compression: mariadbv1alpha1.CompressBzip2,
				Storage: mariadbv1alpha1.PhysicalBackupStorage{
					PersistentVolumeClaim: &mariadbv1alpha1.PersistentVolumeClaimSpec{
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
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
		Expect(k8sClient.Create(testCtx, &backupTemplate)).To(Succeed())

		mdbIgnored := &mariadbv1alpha1.MariaDB{
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
									Name: testEMdbkey.Name,
								},
								Kind: mariadbv1alpha1.ExternalMariaDBKind,
							},
							ServerIdOffset: ptr.To(90),
							IgnoredReplicaTables: []string{
								fmt.Sprintf("%s.%s", ignoredDB, ignoredTbl),
							},
						},
						Replica: mariadbv1alpha1.ReplicaReplication{
							ReplicaBootstrapFrom: &mariadbv1alpha1.ReplicaBootstrapFrom{
								PhysicalBackupTemplateRef: mariadbv1alpha1.LocalObjectReference{
									Name: testPbTemplateERIgnoredKey.Name,
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
					Size:                ptr.To(resource.MustParse("300Mi")),
					StorageClassName:    "standard-resize",
					ResizeInUseVolumes:  ptr.To(true),
					WaitForVolumeResize: ptr.To(true),
				},
				TLS: &mariadbv1alpha1.TLS{
					Enabled:  true,
					Required: ptr.To(true),
				},
				Service: &mariadbv1alpha1.ServiceTemplate{
					Type: corev1.ServiceTypeLoadBalancer,
					Metadata: &mariadbv1alpha1.Metadata{
						Annotations: map[string]string{
							"metallb.universe.tf/loadBalancerIPs": testCidrPrefix + ".0.233",
						},
					},
				},
				Connection: &mariadbv1alpha1.ConnectionTemplate{
					SecretName: func() *string {
						s := "mdb-repl-ext-ignored-conn"
						return &s
					}(),
					SecretTemplate: &mariadbv1alpha1.SecretTemplate{
						Key: &testConnSecretKey,
					},
				},
				PrimaryService: &mariadbv1alpha1.ServiceTemplate{
					Type: corev1.ServiceTypeLoadBalancer,
					Metadata: &mariadbv1alpha1.Metadata{
						Annotations: map[string]string{
							"metallb.universe.tf/loadBalancerIPs": testCidrPrefix + ".0.234",
						},
					},
				},
				PrimaryConnection: &mariadbv1alpha1.ConnectionTemplate{
					SecretName: func() *string {
						s := "mdb-repl-ext-ignored-conn-primary"
						return &s
					}(),
					SecretTemplate: &mariadbv1alpha1.SecretTemplate{
						Key: &testConnSecretKey,
					},
				},
				SecondaryService: &mariadbv1alpha1.ServiceTemplate{
					Type: corev1.ServiceTypeLoadBalancer,
					Metadata: &mariadbv1alpha1.Metadata{
						Annotations: map[string]string{
							"metallb.universe.tf/loadBalancerIPs": testCidrPrefix + ".0.235",
						},
					},
				},
				SecondaryConnection: &mariadbv1alpha1.ConnectionTemplate{
					SecretName: func() *string {
						s := "mdb-repl-ext-ignored-conn-secondary"
						return &s
					}(),
					SecretTemplate: &mariadbv1alpha1.SecretTemplate{
						Key: &testConnSecretKey,
					},
				},
				UpdateStrategy: mariadbv1alpha1.UpdateStrategy{
					Type: mariadbv1alpha1.ReplicasFirstPrimaryLastUpdateType,
				},
			},
		}
		applyMariadbTestConfig(mdbIgnored)
		By("Creating MariaDB with ignored tables external replication")
		Expect(k8sClient.Create(testCtx, mdbIgnored)).To(Succeed())

		DeferCleanup(func() {
			var pbTemplate mariadbv1alpha1.PhysicalBackup
			if err := k8sClient.Get(testCtx, testPbTemplateERIgnoredKey, &pbTemplate); err == nil {
				Expect(k8sClient.Delete(testCtx, &pbTemplate)).To(Succeed())
			}
			var pbRecoveryPvc corev1.PersistentVolumeClaim
			if err := k8sClient.Get(testCtx, testMdbPbRecoveryERIgnoredKey, &pbRecoveryPvc); err == nil {
				Expect(k8sClient.Delete(testCtx, &pbRecoveryPvc)).To(Succeed())
			}
			deleteMariadb(key, false)
		})
	})

	It("should reconcile", func() {
		waitForReady()
	})

	It("should exclude the ignored table from the logical backup", func() {
		waitForReady()

		By("Expecting the logical Backup to skip the ignored table")
		var backup mariadbv1alpha1.Backup
		backupKey := types.NamespacedName{
			Name:      mdb.ExternalReplLogicalBackupName(),
			Namespace: key.Namespace,
		}
		Expect(k8sClient.Get(testCtx, backupKey, &backup)).To(Succeed())
		Expect(backup.Spec.Args).To(ContainElement(fmt.Sprintf("--ignore-table=%s.%s", ignoredDB, ignoredTbl)))
		Expect(backup.Spec.Tables).To(BeEmpty())
	})

	It("should have every table but the ignored one after initial restore", func() {
		waitForReady()

		refResolver := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
			By("Expecting to get SQL client for Pod " + strconv.Itoa(i))
			Expect(err).To(Succeed())
			defer podClient.Close()

			By(fmt.Sprintf("Expecting Pod %d to have the replicated table", i))
			Expect(tableExists(podClient, ignoredDB, replicatedTbl)).To(BeTrue())

			By(fmt.Sprintf("Expecting Pod %d to NOT have the ignored table", i))
			Expect(tableExists(podClient, ignoredDB, ignoredTbl)).To(BeFalse())

			By(fmt.Sprintf("Expecting Pod %d to have the table from the other database", i))
			Expect(tableExists(podClient, otherDB, otherTbl)).To(BeTrue())
		}
	})

	It("should replicate changes to every table but the ignored one", func() {
		waitForReady()

		By("Getting the external MariaDB client")
		var emdb mariadbv1alpha1.ExternalMariaDB
		Expect(k8sClient.Get(testCtx, testEMdbkey, &emdb)).To(Succeed())
		refResolver := refresolver.New(k8sClient)
		externalClient, err := sqlClient.NewClientWithMariaDB(testCtx, &emdb, refResolver)
		Expect(err).To(Succeed())
		defer externalClient.Close()

		// The ignored row goes first: replicas apply the binlog in order, so once the later row is
		// visible the ignored one has already been skipped or applied.
		By("Inserting rows into the ignored and the replicated tables on the external server")
		Expect(externalClient.Exec(testCtx, fmt.Sprintf(
			"INSERT IGNORE INTO `%s`.`%s` VALUES (43)", ignoredDB, ignoredTbl,
		))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf(
			"INSERT IGNORE INTO `%s`.`%s` VALUES (42)", ignoredDB, replicatedTbl,
		))).To(Succeed())
		Expect(externalClient.Exec(testCtx, fmt.Sprintf(
			"INSERT IGNORE INTO `%s`.`%s` VALUES (44)", otherDB, otherTbl,
		))).To(Succeed())

		By("Expecting the replicated rows to appear on all replicas")
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, pErr := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
			Expect(pErr).To(Succeed())
			defer podClient.Close()

			Eventually(func() bool {
				exists, eErr := podClient.Exists(testCtx, fmt.Sprintf(
					"SELECT 1 FROM `%s`.`%s` WHERE id = 42", ignoredDB, replicatedTbl,
				))
				return eErr == nil && exists
			}, testTimeout, testInterval).Should(BeTrue(),
				fmt.Sprintf("Pod %d should have the replicated row", i))
			Eventually(func() bool {
				exists, eErr := podClient.Exists(testCtx, fmt.Sprintf(
					"SELECT 1 FROM `%s`.`%s` WHERE id = 44", otherDB, otherTbl,
				))
				return eErr == nil && exists
			}, testTimeout, testInterval).Should(BeTrue(),
				fmt.Sprintf("Pod %d should have the row from the other database", i))

			By(fmt.Sprintf("Expecting Pod %d to still NOT have the ignored table", i))
			Expect(tableExists(podClient, ignoredDB, ignoredTbl)).To(BeFalse())
		}
	})

	It("should have GTID strict mode disabled", func() {
		waitForReady()

		refResolver := refresolver.New(k8sClient)
		for i := 0; i < int(mdb.Spec.Replicas); i++ {
			podClient, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
			Expect(err).To(Succeed())
			defer podClient.Close()

			By(fmt.Sprintf("Expecting GTID strict mode to be disabled on Pod %d", i))
			val, err := podClient.SystemVariable(testCtx, "gtid_strict_mode")
			Expect(err).To(Succeed())
			Expect(val).To(Equal("0"))
		}
	})
})
