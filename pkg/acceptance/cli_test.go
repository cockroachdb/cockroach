// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package acceptance

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/acceptance/cluster"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestDockerUnixSocket verifies that CockroachDB initializes a unix
// socket useable by 'psql', even when the server runs insecurely.
// TODO(knz): Replace this with a roachtest when roachtest/roachprod
// know how to start secure clusters.
func TestDockerUnixSocket(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	ctx := context.Background()

	if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
		skip.IgnoreLintf(t, "TODO(dt): No binary in one-shot container, see #6086: %s", err)
	}

	containerConfig.Env = []string{fmt.Sprintf("PGUSER=%s", username.RootUser)}
	containerConfig.Cmd = append(cmdBase,
		"/mnt/data/psql/test-psql-unix.sh "+cluster.CockroachBinaryInContainer)
	if err := testDockerOneShot(ctx, t, "unix_socket_test", containerConfig); err != nil {
		t.Error(err)
	}
}

// TestSQLWithoutTLS verifies that CockroachDB can accept clients
// without a TLS handshake in secure mode.
// TODO(knz): Replace this with a roachtest when roachtest/roachprod
// know how to start secure clusters.
func TestSQLWithoutTLS(t *testing.T) {
	s := log.Scope(t)
	defer s.Close(t)

	containerConfig := defaultContainerConfig()
	containerConfig.Cmd = []string{"stat", cluster.CockroachBinaryInContainer}
	ctx := context.Background()

	if err := testDockerOneShot(ctx, t, "cli_test", containerConfig); err != nil {
		skip.IgnoreLintf(t, "TODO(dt): No binary in one-shot container, see #6086: %s", err)
	}

	containerConfig.Env = []string{fmt.Sprintf("PGUSER=%s", username.RootUser)}
	containerConfig.Cmd = append(cmdBase,
		"/mnt/data/psql/test-psql-notls.sh "+cluster.CockroachBinaryInContainer)
	if err := testDockerOneShot(ctx, t, "notls_secure_test", containerConfig); err != nil {
		t.Error(err)
	}
}
