package controller

import (
	"fmt"
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

// dbFilterExternalClient returns a client for the external server. A non-empty database becomes the default database
// of every connection: replicate_do_db and replicate_ignore_db decide statement events (all DDL, and DML under
// statement or mixed binary logging) by the default database, so changes made without one are not representative.
func dbFilterExternalClient(database string) *sqlClient.Client {
	var emdb mariadbv1alpha1.ExternalMariaDB
	Expect(k8sClient.Get(testCtx, testEMdbkey, &emdb)).To(Succeed())
	var opts []sqlClient.Opt
	if database != "" {
		opts = append(opts, sqlClient.WithDatabase(database))
	}
	c, err := sqlClient.NewClientWithMariaDB(testCtx, &emdb, refresolver.New(k8sClient), opts...)
	Expect(err).To(Succeed())
	return c
}

// dbFilterExecIn creates the database on the external server if needed and runs the statements with it as the
// default database.
func dbFilterExecIn(database string, statements ...string) {
	admin := dbFilterExternalClient("")
	defer admin.Close()
	Expect(admin.Exec(testCtx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", database))).To(Succeed())

	c := dbFilterExternalClient(database)
	defer c.Close()
	for _, statement := range statements {
		Expect(c.Exec(testCtx, statement)).To(Succeed())
	}
}

func dbFilterExists(c *sqlClient.Client, query string) bool {
	exists, err := c.Exists(testCtx, query)
	Expect(err).To(Succeed())
	return exists
}

func dbFilterSchemaExists(c *sqlClient.Client, schema string) bool {
	return dbFilterExists(c, fmt.Sprintf("SELECT 1 FROM information_schema.schemata WHERE schema_name='%s'", schema))
}

func dbFilterRoutineExists(c *sqlClient.Client, schema string) bool {
	return dbFilterExists(c, fmt.Sprintf("SELECT 1 FROM information_schema.routines WHERE routine_schema='%s'", schema))
}

func dbFilterRowExists(c *sqlClient.Client, schema, table string, id int) bool {
	exists, err := c.Exists(testCtx, fmt.Sprintf("SELECT 1 FROM `%s`.`%s` WHERE id = %d", schema, table, id))
	return err == nil && exists
}

func dbFilterReplicaStatus(c *sqlClient.Client) map[string]string {
	rows, err := c.QueryColumnMaps(testCtx, "SHOW REPLICA STATUS")
	Expect(err).To(Succeed())
	Expect(rows).To(HaveLen(1))
	return rows[0]
}

func dbFilterWaitForReady(key types.NamespacedName, mdb *mariadbv1alpha1.MariaDB) {
	By("Expecting MariaDB to be ready eventually")
	Eventually(func() bool {
		if err := k8sClient.Get(testCtx, key, mdb); err != nil {
			return false
		}
		return mdb.IsReady()
	}, testVeryHighTimeout, testInterval).Should(BeTrue())
}

func dbFilterForEachPod(mdb *mariadbv1alpha1.MariaDB, fn func(podIndex int, c *sqlClient.Client)) {
	refResolver := refresolver.New(k8sClient)
	for i := 0; i < int(mdb.Spec.Replicas); i++ {
		By(fmt.Sprintf("Getting SQL client for Pod %d", i))
		c, err := sqlClient.NewInternalClientWithPodIndex(testCtx, mdb, refResolver, i)
		Expect(err).To(Succeed())
		func() {
			defer c.Close()
			fn(i, c)
		}()
	}
}

func dbFilterGetLogicalBackup(mdb *mariadbv1alpha1.MariaDB) mariadbv1alpha1.Backup {
	var backup mariadbv1alpha1.Backup
	Expect(k8sClient.Get(testCtx, types.NamespacedName{
		Name:      mdb.ExternalReplLogicalBackupName(),
		Namespace: mdb.Namespace,
	}, &backup)).To(Succeed())
	return backup
}

// newDbFilterMariaDB builds a MariaDB replicating from the external server with the given database lists.
// It exposes the Service, the primary and secondary Services and one Service per Pod on five consecutive
// MetalLB addresses of the 1.x pool, starting at firstIP: see hack/manifests/metallb/mariadb-repl-service.yaml and
// make/net.mk.
func newDbFilterMariaDB(key, pbTemplateKey types.NamespacedName, serverIdOffset, firstIP int,
	filteredDatabases, ignoredDatabases []string) *mariadbv1alpha1.MariaDB {
	loadBalancer := func(ipIndex int) *mariadbv1alpha1.ServiceTemplate {
		return &mariadbv1alpha1.ServiceTemplate{
			Type: corev1.ServiceTypeLoadBalancer,
			Metadata: &mariadbv1alpha1.Metadata{
				Annotations: map[string]string{
					"metallb.universe.tf/loadBalancerIPs": fmt.Sprintf("%s.1.%d", testCidrPrefix, firstIP+ipIndex),
				},
			},
		}
	}
	connection := func(suffix string) *mariadbv1alpha1.ConnectionTemplate {
		return &mariadbv1alpha1.ConnectionTemplate{
			SecretName: ptr.To(key.Name + "-conn" + suffix),
			SecretTemplate: &mariadbv1alpha1.SecretTemplate{
				Key: &testConnSecretKey,
			},
		}
	}

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
								Name: testEMdbkey.Name,
							},
							Kind: mariadbv1alpha1.ExternalMariaDBKind,
						},
						ServerIdOffset:           ptr.To(serverIdOffset),
						FilteredReplicaDatabases: filteredDatabases,
						IgnoredReplicaDatabases:  ignoredDatabases,
					},
					Replica: mariadbv1alpha1.ReplicaReplication{
						ReplicaBootstrapFrom: &mariadbv1alpha1.ReplicaBootstrapFrom{
							PhysicalBackupTemplateRef: mariadbv1alpha1.LocalObjectReference{
								Name: pbTemplateKey.Name,
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
			Service:             loadBalancer(0),
			Connection:          connection(""),
			PrimaryService:      loadBalancer(1),
			PrimaryConnection:   connection("-primary"),
			SecondaryService:    loadBalancer(2),
			SecondaryConnection: connection("-secondary"),
			UpdateStrategy: mariadbv1alpha1.UpdateStrategy{
				Type: mariadbv1alpha1.ReplicasFirstPrimaryLastUpdateType,
			},
		},
	}
	applyMariadbTestConfig(mdb)
	return mdb
}

func newDbFilterPhysicalBackupTemplate(key, mdbKey types.NamespacedName) *mariadbv1alpha1.PhysicalBackup {
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
}

// createDbFilterMariaDB creates the PhysicalBackup template and the MariaDB, and removes them when the spec ends.
func createDbFilterMariaDB(key, pbTemplateKey, pbRecoveryKey types.NamespacedName, serverIdOffset, firstIP int,
	filteredDatabases, ignoredDatabases []string) {
	By("Creating PhysicalBackup template for external replication recovery")
	Expect(k8sClient.Create(testCtx, newDbFilterPhysicalBackupTemplate(pbTemplateKey, key))).To(Succeed())

	By("Creating MariaDB with database filters external replication")
	Expect(k8sClient.Create(testCtx,
		newDbFilterMariaDB(key, pbTemplateKey, serverIdOffset, firstIP, filteredDatabases, ignoredDatabases))).To(Succeed())

	DeferCleanup(func() {
		var pbTemplate mariadbv1alpha1.PhysicalBackup
		if err := k8sClient.Get(testCtx, pbTemplateKey, &pbTemplate); err == nil {
			Expect(k8sClient.Delete(testCtx, &pbTemplate)).To(Succeed())
		}
		var pbRecoveryPvc corev1.PersistentVolumeClaim
		if err := k8sClient.Get(testCtx, pbRecoveryKey, &pbRecoveryPvc); err == nil {
			Expect(k8sClient.Delete(testCtx, &pbRecoveryPvc)).To(Succeed())
		}
		deleteMariadb(key, false)
	})
}

var _ = Describe("MariaDB replication from external server with filtered databases", Ordered, func() {
	const (
		keptDB      = "dbfilterkept"
		keptOtherDB = "dbfilterkeptother"
		skippedDB   = "dbfilterskipped"
		keptTbl     = "kept_table"
		skippedTbl  = "skipped_table"
		laterTbl    = "created_later"
	)

	var (
		key = testMdbERFilteredDbsKey
		mdb = &mariadbv1alpha1.MariaDB{}
	)

	BeforeAll(func() {
		By("Creating databases on the external server")
		for _, db := range []string{keptDB, keptOtherDB} {
			dbFilterExecIn(db,
				fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT PRIMARY KEY)", keptTbl),
				fmt.Sprintf("INSERT IGNORE INTO `%s` VALUES (1)", keptTbl),
			)
		}
		dbFilterExecIn(skippedDB,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT PRIMARY KEY)", skippedTbl),
			fmt.Sprintf("INSERT IGNORE INTO `%s` VALUES (1)", skippedTbl),
			"CREATE PROCEDURE IF NOT EXISTS skipped_proc() SELECT 1",
		)

		createDbFilterMariaDB(key, testPbTemplateERFilteredDbsKey, testMdbPbRecoveryERFilteredDbsKey, 100, 30,
			[]string{keptDB, keptOtherDB}, nil)
	})

	It("should reconcile", func() {
		dbFilterWaitForReady(key, mdb)
	})

	It("should only dump the filtered databases", func() {
		dbFilterWaitForReady(key, mdb)

		By("Expecting the logical Backup to select the filtered databases")
		backup := dbFilterGetLogicalBackup(mdb)
		Expect(backup.Spec.Databases).To(ConsistOf(keptDB, keptOtherDB))
		Expect(backup.Spec.Tables).To(BeEmpty())
		Expect(backup.Spec.Args).NotTo(ContainElement(HavePrefix("--operator-exclude-database=")))
	})

	It("should only have the filtered databases after initial restore", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			By(fmt.Sprintf("Expecting Pod %d to have the filtered databases", i))
			Expect(dbFilterRowExists(c, keptDB, keptTbl, 1)).To(BeTrue())
			Expect(dbFilterRowExists(c, keptOtherDB, keptTbl, 1)).To(BeTrue())

			By(fmt.Sprintf("Expecting Pod %d to NOT have the other database", i))
			Expect(dbFilterSchemaExists(c, skippedDB)).To(BeFalse())
			Expect(dbFilterRoutineExists(c, skippedDB)).To(BeFalse())
		})
	})

	It("should configure replicate_do_db", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			status := dbFilterReplicaStatus(c)
			Expect(status["Replicate_Do_DB"]).To(ContainSubstring(keptDB))
			Expect(status["Replicate_Do_DB"]).To(ContainSubstring(keptOtherDB))
			Expect(status["Replicate_Do_DB"]).NotTo(ContainSubstring(skippedDB))
			Expect(status["Replicate_Ignore_DB"]).To(BeEmpty())
		})
	})

	It("should replicate new tables in the filtered databases and skip the other databases", func() {
		dbFilterWaitForReady(key, mdb)

		// A different id on every run, as the external server keeps the rows of previous runs.
		id := int(time.Now().Unix())

		By("Changing the databases on the external server")
		dbFilterExecIn(keptDB,
			fmt.Sprintf("DROP TABLE IF EXISTS `%s`", laterTbl),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", laterTbl),
			fmt.Sprintf("INSERT INTO `%s` VALUES (%d)", laterTbl, id),
		)
		dbFilterExecIn(keptOtherDB, fmt.Sprintf("INSERT INTO `%s` VALUES (%d)", keptTbl, id))
		dbFilterExecIn(skippedDB,
			fmt.Sprintf("INSERT INTO `%s` VALUES (%d)", skippedTbl, id),
			"CREATE TABLE IF NOT EXISTS skipped_later (id INT PRIMARY KEY)",
		)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			By(fmt.Sprintf("Expecting Pod %d to get the new table of a filtered database", i))
			Eventually(func() bool {
				return dbFilterRowExists(c, keptDB, laterTbl, id)
			}, testTimeout, testInterval).Should(BeTrue())

			By(fmt.Sprintf("Expecting Pod %d to get the new row of the other filtered database", i))
			Eventually(func() bool {
				return dbFilterRowExists(c, keptOtherDB, keptTbl, id)
			}, testTimeout, testInterval).Should(BeTrue())

			By(fmt.Sprintf("Expecting Pod %d to still NOT have the other database, and replication to be healthy", i))
			Expect(dbFilterSchemaExists(c, skippedDB)).To(BeFalse())
			status := dbFilterReplicaStatus(c)
			Expect(status["Slave_IO_Running"]).To(Equal("Yes"))
			Expect(status["Slave_SQL_Running"]).To(Equal("Yes"))
			Expect(status["Last_SQL_Errno"]).To(Equal("0"))
		})
	})

	It("should have GTID strict mode disabled", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			val, err := c.SystemVariable(testCtx, "gtid_strict_mode")
			Expect(err).To(Succeed())
			Expect(val).To(Equal("0"))
		})
	})
})

var _ = Describe("MariaDB replication from external server with ignored databases", Ordered, func() {
	const (
		keptDB     = "dbignorekept"
		skippedDB  = "dbignoreskipped"
		keptTbl    = "kept_table"
		skippedTbl = "skipped_table"
		laterTbl   = "created_later"
	)

	var (
		key = testMdbERIgnoredDbsKey
		mdb = &mariadbv1alpha1.MariaDB{}
	)

	BeforeAll(func() {
		By("Creating databases on the external server")
		dbFilterExecIn(keptDB,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT PRIMARY KEY)", keptTbl),
			fmt.Sprintf("INSERT IGNORE INTO `%s` VALUES (1)", keptTbl),
		)
		dbFilterExecIn(skippedDB,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (id INT PRIMARY KEY)", skippedTbl),
			fmt.Sprintf("INSERT IGNORE INTO `%s` VALUES (1)", skippedTbl),
			"CREATE PROCEDURE IF NOT EXISTS skipped_proc() SELECT 1",
		)

		createDbFilterMariaDB(key, testPbTemplateERIgnoredDbsKey, testMdbPbRecoveryERIgnoredDbsKey, 110, 35,
			nil, []string{skippedDB})
	})

	It("should reconcile", func() {
		dbFilterWaitForReady(key, mdb)
	})

	It("should leave the ignored database out of the logical backup", func() {
		dbFilterWaitForReady(key, mdb)

		By("Expecting the logical Backup to exclude the ignored database and to dump everything else")
		backup := dbFilterGetLogicalBackup(mdb)
		Expect(backup.Spec.Args).To(ContainElement("--operator-exclude-database=" + skippedDB))
		Expect(backup.Spec.Databases).To(BeEmpty())
		Expect(backup.Spec.Tables).To(BeEmpty())
	})

	It("should have every database but the ignored one after initial restore", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			By(fmt.Sprintf("Expecting Pod %d to have the other database", i))
			Expect(dbFilterRowExists(c, keptDB, keptTbl, 1)).To(BeTrue())

			// The mysql system schema is dumped like in an unfiltered backup, so the definition row that MariaDB keeps
			// in mysql.proc for a stored routine is copied even though its database is not. That row is inert, as the
			// routine cannot be called without its database, so it is not asserted here.
			By(fmt.Sprintf("Expecting Pod %d to NOT have the ignored database nor its tables", i))
			Expect(dbFilterSchemaExists(c, skippedDB)).To(BeFalse())
			Expect(dbFilterExists(c, fmt.Sprintf(
				"SELECT 1 FROM information_schema.tables WHERE table_schema='%s'", skippedDB,
			))).To(BeFalse())
		})
	})

	It("should configure replicate_ignore_db", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			status := dbFilterReplicaStatus(c)
			Expect(status["Replicate_Ignore_DB"]).To(ContainSubstring(skippedDB))
			Expect(status["Replicate_Do_DB"]).To(BeEmpty())
		})
	})

	It("should replicate new tables in the other databases and skip the ignored database", func() {
		dbFilterWaitForReady(key, mdb)

		// A different id on every run, as the external server keeps the rows of previous runs.
		id := int(time.Now().Unix())

		By("Changing the databases on the external server")
		dbFilterExecIn(keptDB,
			fmt.Sprintf("DROP TABLE IF EXISTS `%s`", laterTbl),
			fmt.Sprintf("CREATE TABLE `%s` (id INT PRIMARY KEY)", laterTbl),
			fmt.Sprintf("INSERT INTO `%s` VALUES (%d)", laterTbl, id),
		)
		dbFilterExecIn(skippedDB,
			fmt.Sprintf("INSERT INTO `%s` VALUES (%d)", skippedTbl, id),
			"CREATE TABLE IF NOT EXISTS skipped_later (id INT PRIMARY KEY)",
		)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			By(fmt.Sprintf("Expecting Pod %d to get the new table of the other database", i))
			Eventually(func() bool {
				return dbFilterRowExists(c, keptDB, laterTbl, id)
			}, testTimeout, testInterval).Should(BeTrue())

			By(fmt.Sprintf("Expecting Pod %d to still NOT have the ignored database, and replication to be healthy", i))
			Expect(dbFilterSchemaExists(c, skippedDB)).To(BeFalse())
			status := dbFilterReplicaStatus(c)
			Expect(status["Slave_IO_Running"]).To(Equal("Yes"))
			Expect(status["Slave_SQL_Running"]).To(Equal("Yes"))
			Expect(status["Last_SQL_Errno"]).To(Equal("0"))
		})
	})

	It("should have GTID strict mode disabled", func() {
		dbFilterWaitForReady(key, mdb)

		dbFilterForEachPod(mdb, func(i int, c *sqlClient.Client) {
			val, err := c.SystemVariable(testCtx, "gtid_strict_mode")
			Expect(err).To(Succeed())
			Expect(val).To(Equal("0"))
		})
	})
})
