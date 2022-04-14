// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/stretchr/testify/require"
)

// TODO: move this below cluster interface, maybe into roachprod.

// tenantNode corresponds to a running tenant.
type tenantNode struct {
	tenantID          int
	httpPort, sqlPort int
	kvAddrs           []string
	pgURL             string

	binary string // the binary last passed to start()
	errCh  chan error
	node   int
}

func CreateTenantNode(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	kvAddrs []string,
	tenantID, node, httpPort, sqlPort int,
) *tenantNode {
	tn := &tenantNode{
		tenantID: tenantID,
		httpPort: httpPort,
		kvAddrs:  kvAddrs,
		node:     node,
		sqlPort:  sqlPort,
	}
	tn.createTenantCert(ctx, t, c)
	return tn
}

func (tn *tenantNode) createTenantCert(ctx context.Context, t test.Test, c cluster.Cluster) {
	var names []string
	eips, err := c.ExternalIP(ctx, t.L(), c.Node(tn.node))
	require.NoError(t, err)
	names = append(names, eips...)
	iips, err := c.InternalIP(ctx, t.L(), c.Node(tn.node))
	require.NoError(t, err)
	names = append(names, iips...)

	names = append(names, "localhost", "127.0.0.1")

	cmd := fmt.Sprintf(
		"./cockroach cert create-tenant-client --certs-dir=certs --ca-key=certs/ca.key %d %s",
		tn.tenantID, strings.Join(names, " "))
	c.Run(ctx, c.Node(tn.node), cmd)
}

func (tn *tenantNode) stop(ctx context.Context, t test.Test, c cluster.Cluster) {
	if tn.errCh == nil {
		return
	}
	// Must use pkill because the context cancellation doesn't wait for the
	// process to exit.
	c.Run(ctx, c.Node(tn.node),
		fmt.Sprintf("pkill -o -f '^%s mt start.*tenant-id=%d'", tn.binary, tn.tenantID))
	t.L().Printf("mt cluster exited: %v", <-tn.errCh)
	tn.errCh = nil
}

func (tn *tenantNode) logDir() string {
	return fmt.Sprintf("logs/mt-%d", tn.tenantID)
}

func (tn *tenantNode) storeDir() string {
	return fmt.Sprintf("cockroach-data-mt-%d", tn.tenantID)
}

func (tn *tenantNode) start(ctx context.Context, t test.Test, c cluster.Cluster, binary string) {
	require.True(t, c.IsSecure())

	tn.binary = binary
	extraArgs := []string{
		"--log=\"file-defaults: {dir: '" + tn.logDir() + "', exit-on-error: false}\"",
		"--store=" + tn.storeDir()}
	tn.errCh = startTenantServer(
		ctx, c, c.Node(tn.node), binary, tn.kvAddrs, tn.tenantID,
		tn.httpPort, tn.sqlPort,
		extraArgs...,
	)
	externalUrls, err := c.ExternalPGUrl(ctx, t.L(), c.Node(tn.node))
	require.NoError(t, err)
	u, err := url.Parse(externalUrls[0])
	require.NoError(t, err)
	internalUrls, err := c.ExternalIP(ctx, t.L(), c.Node(tn.node))
	require.NoError(t, err)
	u.Host = internalUrls[0] + ":" + strconv.Itoa(tn.sqlPort)
	tn.pgURL = u.String()

	// The tenant is usually responsive ~right away, but it has on occasions
	// taken more than 3s for it to connect to the KV layer, and it won't open
	// the SQL port until it has.
	testutils.SucceedsSoon(t, func() error {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case err := <-tn.errCh:
			t.Fatal(err)
		default:
		}

		db, err := gosql.Open("postgres", tn.pgURL)
		if err != nil {
			return err
		}
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		_, err = db.ExecContext(ctx, `SELECT 1`)
		return err
	})

	t.L().Printf("sql server for tenant %d running at %s", tn.tenantID, tn.pgURL)
}

func startTenantServer(
	tenantCtx context.Context,
	c cluster.Cluster,
	node option.NodeListOption,
	binary string,
	kvAddrs []string,
	tenantID int,
	httpPort int,
	sqlPort int,
	extraFlags ...string,
) chan error {
	args := []string{
		"--certs-dir", "certs",
		"--tenant-id=" + strconv.Itoa(tenantID),
		"--http-addr", ifLocal(c, "127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(httpPort),
		"--kv-addrs", strings.Join(kvAddrs, ","),
		// Don't bind to external interfaces when running locally.
		"--sql-addr", ifLocal(c, "127.0.0.1", "0.0.0.0") + ":" + strconv.Itoa(sqlPort),
	}
	args = append(args, extraFlags...)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.RunE(tenantCtx, node,
			append([]string{binary, "mt", "start-sql"}, args...)...,
		)
		close(errCh)
	}()
	return errCh
}
