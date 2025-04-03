// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/errors"
)

// registerBackupS3Clones validates backup/restore compatibility with S3 clones.
func registerBackupS3Clones(r registry.Registry) {
	// Running against a microceph cluster deployed on a GCE instance.
	for _, cephVersion := range []string{"reef", "squid"} {
		r.Add(registry.TestSpec{
			Name:                      fmt.Sprintf("backup/ceph/%s", cephVersion),
			Owner:                     registry.OwnerFieldEng,
			Cluster:                   r.MakeClusterSpec(4, spec.WorkloadNodeCount(1)),
			EncryptionSupport:         registry.EncryptionMetamorphic,
			Leases:                    registry.MetamorphicLeases,
			CompatibleClouds:          registry.Clouds(spec.GCE),
			Suites:                    registry.Suites(registry.Nightly),
			TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				v := s3BackupRestoreValidator{
					t:            t,
					c:            c,
					crdbNodes:    c.CRDBNodes(),
					csvPort:      8081,
					importNode:   c.Node(1),
					rows:         1000,
					workloadNode: c.WorkloadNode(),
				}
				v.startCluster(ctx)
				ceph := cephManager{
					t:      t,
					c:      c,
					bucket: backupTestingBucket,
					// For now, we use the workload node as the cephNode
					cephNodes: c.Node(c.Spec().NodeCount),
					key:       randomString(32),
					secret:    randomString(64),
					// reef `microceph enable rgw` does not support `--ssl-certificate`
					// so we'll test a non-secure version.
					secure:  cephVersion != "reef",
					version: cephVersion,
				}
				ceph.install(ctx)
				defer ceph.cleanup(ctx)
				v.validateBackupRestore(ctx, ceph)
			},
		})
	}

	r.Add(registry.TestSpec{
		Name:                      "backup/minio",
		Owner:                     registry.OwnerFieldEng,
		Cluster:                   r.MakeClusterSpec(4, spec.WorkloadNodeCount(1)),
		EncryptionSupport:         registry.EncryptionMetamorphic,
		Leases:                    registry.MetamorphicLeases,
		CompatibleClouds:          registry.Clouds(spec.GCE),
		Suites:                    registry.Suites(registry.Nightly),
		TestSelectionOptOutSuites: registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			v := s3BackupRestoreValidator{
				t:            t,
				c:            c,
				crdbNodes:    c.CRDBNodes(),
				csvPort:      8081,
				importNode:   c.Node(1),
				rows:         1000,
				workloadNode: c.WorkloadNode(),
			}
			v.startCluster(ctx)
			mgr := minioManager{
				t:      t,
				c:      c,
				bucket: backupTestingBucket,
				// For now, we use the workload node as the minio cluster
				minioNodes: c.Node(c.Spec().NodeCount),
				key:        randomString(32),
				secret:     randomString(64),
			}
			mgr.install(ctx)
			defer mgr.cleanup(ctx)
			v.validateBackupRestore(ctx, mgr)
		},
	})

}

// s3Provider defines the methods that the S3 object store has to provide
// in order to run the backup/restore validation tests.
type s3Provider interface {
	// getBackupURI returns the storage specific destination URI
	getBackupURI(ctx context.Context, dest string) (string, error)
}

// s3BackupRestoreValidator verifies backup/restore functionality against
// an S3 vendor.
type s3BackupRestoreValidator struct {
	t            test.Test
	c            cluster.Cluster
	crdbNodes    option.NodeListOption
	csvPort      int
	importNode   option.NodeListOption
	rows         int
	workloadNode option.NodeListOption
}

// checkBackups verifies that there is exactly one full and one incremental backup.
func (v *s3BackupRestoreValidator) checkBackups(ctx context.Context, conn *gosql.DB) {
	backups := conn.QueryRowContext(ctx, "SHOW BACKUPS IN 'external://backup_bucket'")
	var path string
	if err := backups.Scan(&path); err != nil {
		v.t.Fatal(err)
	}

	rows, err := conn.QueryContext(ctx,
		"SELECT backup_type from [SHOW BACKUP $1 IN 'external://backup_bucket'] WHERE object_type='table'",
		path)

	if err != nil {
		v.t.Fatal(err)
	}
	var foundFull, foundIncr bool
	var rowCount int
	for rows.Next() {
		var backupType string
		if err := rows.Scan(&backupType); err != nil {
			v.t.Fatal(err)
		}
		if backupType == "full" {
			foundFull = true
		}
		if backupType == "incremental" {
			foundIncr = true
		}
		rowCount++
	}
	if !foundFull {
		v.t.Fatal(errors.Errorf("full backup not found"))
	}
	if !foundIncr {
		v.t.Fatal(errors.Errorf("incremental backup not found"))
	}
	if rowCount > 2 {
		v.t.Fatal(errors.Errorf("found more than 2 backups"))
	}

}

// runImportForS3CloneTesting import the data used to test the S3 clone backup/restore
// functionality.
func (v *s3BackupRestoreValidator) importData(ctx context.Context) {
	csvCmd := importBankCSVServerCommand("./cockroach", v.csvPort)
	v.c.Run(ctx, option.WithNodes(v.crdbNodes), csvCmd+` &> logs/workload-csv-server.log < /dev/null &`)
	if err := waitForPort(ctx, v.t.L(), v.crdbNodes, v.csvPort, v.c); err != nil {
		v.t.Fatal(err)
	}
	v.c.Run(ctx, option.WithNodes(v.importNode),
		importBankCommand("./cockroach", v.rows, 0, v.csvPort, v.importNode[0]))
}

// startCluster starts the Cockroach cluster.
func (v *s3BackupRestoreValidator) startCluster(ctx context.Context) {
	settings := install.MakeClusterSettings()
	settings.Secure = true
	v.c.Start(ctx, v.t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, v.crdbNodes)
}

// validateS3BackupRestore performs a backup/restore against a storage provider
// to asses minimum compatibility at the functional level.
// This does not imply that a storage provider passing the test is supported.
func (v *s3BackupRestoreValidator) validateBackupRestore(ctx context.Context, s s3Provider) {
	dest := destinationName(v.c)
	v.importData(ctx)

	var backupPath string
	var err error
	if backupPath, err = s.getBackupURI(ctx, dest); err != nil {
		v.t.Fatal(err)
	}
	conn := v.c.Conn(ctx, v.t.L(), 1)
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		fmt.Sprintf("CREATE EXTERNAL CONNECTION backup_bucket AS '%s'",
			backupPath)); err != nil {
		v.t.Fatal(err)
	}

	// Run a full backup while running the workload
	m := v.c.NewMonitor(ctx, v.c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		v.t.Status(`running backup `)
		_, err := conn.ExecContext(ctx,
			"BACKUP bank.bank INTO 'external://backup_bucket'")
		return err
	})
	m.Go(func(ctx context.Context) error {
		v.t.Status(`running workload`)
		return v.runWorload(ctx, 10*time.Second)
	})
	m.Wait()

	// Run an incremental backup
	v.t.Status(`running incremental backup `)
	if _, err := conn.ExecContext(ctx,
		"BACKUP bank.bank INTO LATEST IN 'external://backup_bucket'"); err != nil {
		v.t.Fatal(err)
	}

	// Verify that we have the backups, then restore in a separate database.
	v.checkBackups(ctx, conn)
	v.t.Status(`restoring from backup`)
	if _, err := conn.ExecContext(ctx, "CREATE DATABASE restoreDB"); err != nil {
		v.t.Fatal(err)
	}

	if _, err := conn.ExecContext(ctx,
		`RESTORE bank.bank FROM LATEST IN 'external://backup_bucket' WITH into_db=restoreDB`); err != nil {
		v.t.Fatal(err)
	}

	// Check that the content of the original database and the restored database
	// are the same.
	table := "bank"
	originalBank, err := roachtestutil.Fingerprint(ctx, conn, "bank" /* db */, table)
	if err != nil {
		v.t.Fatal(err)
	}
	restore, err := roachtestutil.Fingerprint(ctx, conn, "restoreDB" /* db */, table)
	if err != nil {
		v.t.Fatal(err)
	}

	if originalBank != restore {
		v.t.Fatal(errors.Errorf("got %s, expected %s while comparing restoreDB with originalBank",
			restore, originalBank))
	}
}

// runWorload runs the bank workload for the specified duration.
func (v *s3BackupRestoreValidator) runWorload(ctx context.Context, duration time.Duration) error {
	cmd := roachtestutil.
		NewCommand("./cockroach workload run bank").
		Arg("{pgurl%s}", v.crdbNodes).
		Flag("duration", duration.String()).
		String()
	return v.c.RunE(ctx, option.WithNodes(v.workloadNode), cmd)
}

func installCa(ctx context.Context, t test.Test, c cluster.Cluster) error {
	localCertsDir, err := os.MkdirTemp("", "roachtest-certs")
	if err != nil {
		return err
	}
	// get the ca file from one of the nodes.
	caFile := path.Join(localCertsDir, "ca.crt")
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	if err := c.Get(ctx, t.L(), "certs/ca.crt", caFile, c.Node(1)); err != nil {
		return err
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}
	// Disabling caching for Custom CA, see https://github.com/cockroachdb/cockroach/issues/125051.
	if _, err := conn.ExecContext(ctx, "set cluster setting cloudstorage.s3.session_reuse.enabled = false"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "set cluster setting cloudstorage.http.custom_ca=$1", caCert); err != nil {
		return err
	}
	return nil
}

// randomString returns a random string with the given size.
func randomString(size int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, size)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
