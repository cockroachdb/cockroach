// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// cephDisksScript creates 3 4GB loop devices, e.g. virtual block devices that allows
// a computer file to be accessed as if it were a physical disk or partition.
// These loop devices will be used by the Ceph Object Storage Daemon (OSD) as
// disks.
const cephDisksScript = `
#!/bin/bash
for l in  a b c; do
  mkdir -p /mnt/data1/disks
  loop_file="$(sudo mktemp -p /mnt/data1/disks XXXX.img)"
  sudo truncate -s 4G "${loop_file}"
  loop_dev="$(sudo losetup --show -f "${loop_file}")"
  # the block-devices plug doesn't allow accessing /dev/loopX
  # devices so we make those same devices available under alternate
  # names (/dev/sdiY)
  minor="${loop_dev##/dev/loop}"
  sudo mknod -m 0660 "/dev/sdi${l}" b 7 "${minor}"
  sudo microceph disk add --wipe "/dev/sdi${l}"
done`

// cephCleanup removes microceph and the loop devices.
const cephCleanup = `
#!/bin/bash
sudo microceph disable rgw
sudo snap remove microceph --purge
for l in  a b c; do
  sudo rm  -f /dev/sdi${l}
done
sudo rm -rf /mnt/data1/disks
`

const s3cmdSsl = `sudo s3cmd --host localhost --host-bucket="localhost/%%(bucket)" \
               --access_key=%s --secret_key=%s --ca-certs=./certs/ca.crt %s`

const s3cmdNoSsl = `sudo s3cmd --host localhost --host-bucket="localhost/%%(bucket)" \
               --access_key=%s --secret_key=%s --no-ssl %s`

// cephManager manages a single node microCeph cluster, used to
// validate the backup and restore functionality.
type cephManager struct {
	t         test.Test
	c         cluster.Cluster
	bucket    string
	cephNodes option.NodeListOption // The nodes within the cluster used by Ceph.
	key       string
	secret    string
	secure    bool
	version   string
}

// cephManager implements s3Provider
var _ s3Provider = &cephManager{}

// getBackupURI implements s3Provider.
func (m cephManager) getBackupURI(ctx context.Context, dest string) (string, error) {
	addr, err := m.c.InternalIP(ctx, m.t.L(), m.cephNodes)
	if err != nil {
		return "", err
	}
	m.t.Status("cephNode: ", addr)
	endpointURL := `http://` + addr[0]
	if m.secure {
		endpointURL = `https://` + addr[0]
	}
	q := make(url.Values)
	q.Add(amazon.AWSAccessKeyParam, m.key)
	q.Add(amazon.AWSSecretParam, m.secret)
	q.Add(amazon.AWSUsePathStyle, "true")
	// Region is required in the URL, but not used in Ceph.
	q.Add(amazon.S3RegionParam, "dummy")
	q.Add(amazon.AWSEndpointParam, endpointURL)
	uri := fmt.Sprintf("s3://%s/%s?%s", m.bucket, dest, q.Encode())
	return uri, nil
}

func (m cephManager) cleanup(ctx context.Context) {
	tmpDir := "/tmp/"
	cephCleanupPath := filepath.Join(tmpDir, "cleanup.sh")
	m.put(ctx, cephCleanup, cephCleanupPath)
	m.run(ctx, "removing ceph", cephCleanupPath, tmpDir)
}

// install a single node microCeph cluster.
// See https://canonical-microceph.readthedocs-hosted.com/en/squid-stable/how-to/single-node/
// It is fatal on errors.
func (m cephManager) install(ctx context.Context) {
	tmpDir := "/tmp/"
	m.run(ctx, `installing microceph`,
		fmt.Sprintf(`sudo snap install microceph --channel %s/stable`, m.version))
	m.run(ctx, `preventing upgrades`, `sudo snap refresh --hold microceph`)
	m.run(ctx, `initialize microceph`, `sudo microceph cluster bootstrap`)

	cephDisksScriptPath := filepath.Join(tmpDir, "cephDisks.sh")
	m.put(ctx, cephDisksScript, cephDisksScriptPath)
	m.run(ctx, "adding disks", cephDisksScriptPath, tmpDir)

	// Start the Ceph Object Gateway, also known as RADOS Gateway (RGW). RGW is
	// an object storage interface to provide applications with a RESTful
	// gateway to Ceph storage clusters, compatible with the S3 APIs.
	// We are leveraging the node certificates created by cockroach.
	rgwCmd := "sudo microceph enable rgw "
	if m.secure {
		rgwCmd = rgwCmd + ` --ssl-certificate="$(base64 -w0 certs/node.crt)" --ssl-private-key="$(base64 -w0 certs/node.key)"`
	}
	m.run(ctx, `starting object gateway`, rgwCmd)

	m.run(ctx, `creating backup user`,
		`sudo radosgw-admin user create --uid=backup --display-name=backup`)
	m.run(ctx, `add keys to the user`,
		fmt.Sprintf(`sudo radosgw-admin key create --uid=backup --key-type=s3 --access-key=%s --secret-key=%s`,
			m.key, m.secret))

	m.run(ctx, `install s3cmd`, `sudo apt install -y s3cmd`)
	s3cmd := s3cmdNoSsl
	if m.secure {
		s3cmd = s3cmdSsl
	}
	m.run(ctx, `creating bucket`,
		fmt.Sprintf(s3cmd, m.key, m.secret, "mb s3://"+m.bucket))
	if err := m.maybeInstallCa(ctx); err != nil {
		m.t.Fatal(err)
	}
}

// maybeInstallCa adds a custom ca in the CockroachDB cluster.
func (m cephManager) maybeInstallCa(ctx context.Context) error {
	if !m.secure {
		return nil
	}
	return installCa(ctx, m.t, m.c)
}

// put creates a file in the ceph node with the given content.
func (m cephManager) put(ctx context.Context, content string, dest string) {
	err := m.c.PutString(ctx, content, dest, 0700, m.cephNodes)
	if err != nil {
		m.t.Fatal(err)
	}
}

// run the given command on the ceph node.
func (m cephManager) run(ctx context.Context, msg string, cmd ...string) {
	m.t.Status(msg, "...")
	m.t.Status(cmd)
	m.c.Run(ctx, option.WithNodes(m.cephNodes), cmd...)
	m.t.Status(msg, " done")
}
