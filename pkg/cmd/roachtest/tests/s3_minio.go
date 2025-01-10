// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/url"
	"path"

	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

// minioDir is the directory for supporting files.
var minioDir = "/tmp/minio"

// minioManager manages a single node minio cluster, used to
// validate the backup and restore functionality.
type minioManager struct {
	t          test.Test
	c          cluster.Cluster
	bucket     string
	minioNodes option.NodeListOption // The nodes within the cluster used by Minio.
	key        string
	secret     string
}

// minioManager implements s3Provider
var _ s3Provider = &minioManager{}

// getBackupURI implements s3Provider.
func (m minioManager) getBackupURI(ctx context.Context, dest string) (string, error) {
	addr, err := m.c.InternalIP(ctx, m.t.L(), m.minioNodes)
	if err != nil {
		return "", err
	}
	m.t.Status("minio: ", addr)
	endpointURL := `https://` + addr[0]

	q := make(url.Values)
	q.Add(amazon.AWSAccessKeyParam, m.key)
	q.Add(amazon.AWSSecretParam, m.secret)
	q.Add(amazon.AWSUsePathStyle, "true")
	// Region is required in the URL, but not used in Minio.
	q.Add(amazon.S3RegionParam, "dummy")
	q.Add(amazon.AWSEndpointParam, endpointURL)
	uri := fmt.Sprintf("s3://%s/%s?%s", m.bucket, dest, q.Encode())
	return uri, nil
}

func (m minioManager) cleanup(ctx context.Context) {
	m.run(ctx, "removing minio", "sudo docker rm -f minio")
	m.run(ctx, "removing minio dir", fmt.Sprintf(`rm  -rf %s`, minioDir))
}

// install a single node minio cluster within a docker container.
// It is fatal on errors.
func (m minioManager) install(ctx context.Context) {
	if err := m.c.Install(ctx, m.t.L(), m.minioNodes, "docker"); err != nil {
		m.t.Fatalf("failed to install docker: %v", err)
	}
	certsDir := path.Join(minioDir, "certs")
	m.run(ctx, `copy CA`,
		fmt.Sprintf(`mkdir -p %[1]s/CAs ; cp certs/ca.crt %[1]s/CAs/ca.crt; `, certsDir))
	m.run(ctx, `copy certs/key`,
		fmt.Sprintf(`cp certs/node.crt %[1]s/public.crt; cp certs/node.key  %[1]s/private.key; `,
			certsDir))
	m.run(ctx, `installing minio`,
		fmt.Sprintf(`sudo docker run --name minio -d -p 443:9000 -e "MINIO_ROOT_USER=%s" -e "MINIO_ROOT_PASSWORD=%s" --privileged -v %s:/root/.minio minio/minio server  /data`,
			m.key, m.secret, minioDir))

	m.run(ctx, `install s3cmd`, `sudo apt install -y s3cmd`)
	m.run(ctx, `creating bucket`,
		fmt.Sprintf(s3cmdSsl, m.key, m.secret, "mb s3://"+m.bucket))

	if err := installCa(ctx, m.t, m.c); err != nil {
		m.t.Fatal(err)
	}
}

// run the given command on the minio node.
func (m minioManager) run(ctx context.Context, msg string, cmd ...string) {
	m.t.Status(msg, "...")
	m.t.Status(cmd)
	m.c.Run(ctx, option.WithNodes(m.minioNodes), cmd...)
	m.t.Status(msg, " done")
}
