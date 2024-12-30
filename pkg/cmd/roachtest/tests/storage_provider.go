// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
)

// storageProvider defines the methods used to validate the functionality
// of a storage provider.
type storageProvider interface {
	// getBackupURI returns the storage specific destination URI
	getBackupURI(ctx context.Context, dest string) (string, error)
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

// validateBackupRestore performs a backup/restore against a storage provider
// to asses minimum compatibility at the functional level.
// This does not imply that a storage provider passing the test is supported.
func validateBackupRestore(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	s storageProvider,
) {
	rows := 100
	dest := importBankData(ctx, rows, t, c)

	var backupPath string
	var err error
	if backupPath, err = s.getBackupURI(ctx, dest); err != nil {
		t.Fatal(err)
	}

	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	m := c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		t.Status(`running backup`, backupPath)
		_, err := conn.ExecContext(ctx, "BACKUP bank.bank INTO $1",
			backupPath)
		return err
	})
	m.Wait()

	m = c.NewMonitor(ctx)
	m.Go(func(ctx context.Context) error {
		t.Status(`restoring from backup`)
		if _, err := conn.ExecContext(ctx, "CREATE DATABASE restoreDB"); err != nil {
			return err
		}

		if _, err := conn.ExecContext(ctx,
			`RESTORE bank.bank FROM LATEST IN $1 WITH into_db=restoreDB`,
			backupPath,
		); err != nil {
			return err
		}

		table := "bank"
		originalBank, err := fingerprint(ctx, conn, "bank" /* db */, table)
		if err != nil {
			return err
		}
		restore, err := fingerprint(ctx, conn, "restoreDB" /* db */, table)
		if err != nil {
			return err
		}

		if originalBank != restore {
			return errors.Errorf("got %s, expected %s while comparing restoreDB with originalBank", restore, originalBank)
		}
		return nil
	})
	m.Wait()
}

// cephDisksScript creates 3 4GB loop devices, e.g. virtual block devices that allows
// a computer file to be accessed as if it were a physical disk or partition.
// These loop devices will be used by the Ceph Object Storage Daemon (OSD) as
// disks.
const cephDisksScript = `
#!/bin/bash
for l in  a b c; do
  loop_file="$(sudo mktemp -p /mnt/data1 XXXX.img)"
  sudo truncate -s 4G "${loop_file}"
  loop_dev="$(sudo losetup --show -f "${loop_file}")"
  # the block-devices plug doesn't allow accessing /dev/loopX
  # devices so we make those same devices available under alternate
  # names (/dev/sdiY)
  minor="${loop_dev##/dev/loop}"
  sudo mknod -m 0660 "/dev/sdi${l}" b 7 "${minor}"
  sudo microceph disk add --wipe "/dev/sdi${l}"
done`

const s3cmd = `sudo s3cmd --host localhost --host-bucket="localhost/%%(bucket)" --access_key=%s --secret_key=%s --no-ssl %s`

// cephManager manages a single node microCeph cluster, used to
// validate the backup and restore functionality.
type cephManager struct {
	t         test.Test
	c         cluster.Cluster
	bucket    string
	cephNodes option.NodeListOption // The nodes within the cluster used by Ceph.
	key       string
	secret    string
	version   string
}

// cephManager implements storageProvider
var _ storageProvider = &cephManager{}

// install a single node microCeph cluster.
// See https://canonical-microceph.readthedocs-hosted.com/en/squid-stable/how-to/single-node/
// It is fatal on errors.
func (m cephManager) install(ctx context.Context) {
	// TODO (silvano): add TLS support.
	folder := "/tmp/"
	// Install MicroCeph.
	m.t.Status("installing microceph ", m.version)
	installCmd := fmt.Sprintf(`sudo snap install microceph --channel %s/stable`, m.version)
	m.c.Run(ctx, option.WithNodes(m.cephNodes), installCmd)
	// Prevent the software from being auto-updated.
	m.c.Run(ctx, option.WithNodes(m.cephNodes), `sudo snap refresh --hold microceph`)
	// Initialize the cluster.
	m.t.Status("initialize microceph")
	m.c.Run(ctx, option.WithNodes(m.cephNodes), `sudo microceph cluster bootstrap`)
	// Add disks to the cluster.
	cephDisksScriptPath := filepath.Join(folder, "cephDisks.sh")
	err := m.c.PutString(ctx, cephDisksScript, cephDisksScriptPath, 0700, m.cephNodes)
	if err != nil {
		m.t.Fatal(err)
	}
	m.t.Status("adding disks")
	m.c.Run(ctx, option.WithNodes(m.cephNodes), cephDisksScriptPath, folder)

	// Start the Ceph Object Gateway, also known as RADOS Gateway (RGW). RGW is
	// an object storage interface to provide applications with a RESTful
	// gateway to Ceph storage clusters, compatible with the S3 APIs.
	m.t.Status("starting object gateway")
	m.c.Run(ctx, option.WithNodes(m.cephNodes), `sudo microceph enable rgw`)

	// Create a user and a key.
	m.t.Status("creating backup user")
	m.c.Run(ctx, option.WithNodes(m.cephNodes),
		`sudo radosgw-admin user create --uid=backup --display-name=backup`)
	m.t.Status("add keys to the user")
	m.c.Run(ctx, option.WithNodes(m.cephNodes),
		fmt.Sprintf(`sudo radosgw-admin key create --uid=backup --key-type=s3 --access-key=%s --secret-key=%s`,
			m.key, m.secret))
	// Install s3cmd.
	m.t.Status("installing s3cmd")
	m.c.Run(ctx, option.WithNodes(m.cephNodes), `sudo apt install -y s3cmd`)
	// Create the destination bucket.
	m.t.Status("creating bucket ", m.bucket)
	m.c.Run(ctx, option.WithNodes(m.cephNodes),
		fmt.Sprintf(s3cmd, m.key, m.secret, "mb s3://"+m.bucket))
}

// getBackupURI implements storageProvider.
func (m cephManager) getBackupURI(ctx context.Context, dest string) (string, error) {
	ips, err := m.c.InternalIP(ctx, m.t.L(), m.cephNodes)
	if err != nil {
		m.t.Fatal(err)
	}
	endpointURL := `http://` + ips[0]
	q := make(url.Values)
	q.Add(amazon.AWSAccessKeyParam, m.key)
	q.Add(amazon.AWSSecretParam, m.secret)
	q.Add(amazon.AWSUsePathStyle, "true")
	q.Add(amazon.S3RegionParam, "dummy")
	q.Add(amazon.AWSEndpointParam, endpointURL)
	uri := fmt.Sprintf("s3://%s/%s?%s", backupTestingBucket, dest, q.Encode())
	return uri, nil
}
