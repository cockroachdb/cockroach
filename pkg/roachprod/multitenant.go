// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachprod

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// StartServiceForVirtualCluster starts SQL/HTTP instances for a
// virtual cluster. This runs processes on an underlying
// roachprod-created cluster of VMs. The SQL/HTTP instances connect to
// a storage cluster, which must be running alrady. The metadata for
// the virtual cluster is created on the storage cluster if it doesn't
// exist already.
//
// The storage cluster and the virtual cluster instances can use the
// same underlying roachprod cluster, as long as different subsets of
// nodes are selected (e.g. "local:1,2" and "local:3,4").
func StartServiceForVirtualCluster(
	ctx context.Context,
	l *logger.Logger,
	virtualCluster string,
	storageCluster string,
	startOpts install.StartOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	tc, err := newCluster(l, virtualCluster, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	// TODO(radu): do we need separate clusterSettingsOpts for the storage cluster?
	hc, err := newCluster(l, storageCluster, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	startOpts.Target = install.StartServiceForVirtualCluster
	if startOpts.VirtualClusterID < 2 {
		return errors.Errorf("invalid tenant ID %d (must be 2 or higher)", startOpts.VirtualClusterID)
	}
	startOpts.VirtualClusterName = defaultVirtualClusterName(startOpts.VirtualClusterID)

	// Create virtual cluster, if necessary. We only need to run this
	// SQL against a single connection to the storage cluster.
	l.Printf("Creating tenant metadata")
	if err := hc.ExecSQL(ctx, l, hc.Nodes[:1], "", 0, []string{
		`-e`,
		fmt.Sprintf(createVirtualClusterIfNotExistsQuery, startOpts.VirtualClusterID),
	}); err != nil {
		return err
	}

	l.Printf("Starting SQL/HTTP instances for the virtual cluster")
	var kvAddrs []string
	for _, node := range hc.Nodes {
		port, err := hc.NodePort(ctx, node)
		if err != nil {
			return err
		}
		kvAddrs = append(kvAddrs, fmt.Sprintf("%s:%d", hc.Host(node), port))
	}
	startOpts.KVAddrs = strings.Join(kvAddrs, ",")
	startOpts.KVCluster = hc
	return tc.Start(ctx, l, startOpts)
}

// StopServiceForVirtualCluster stops SQL instance processes on the virtualCluster given.
func StopServiceForVirtualCluster(
	ctx context.Context, l *logger.Logger, virtualCluster string, stopOpts StopOpts,
) error {
	tc, err := newCluster(l, virtualCluster)
	if err != nil {
		return err
	}

	stopOpts.VirtualClusterName = defaultVirtualClusterName(stopOpts.VirtualClusterID)
	vc := install.VirtualClusterLabel(stopOpts.VirtualClusterName, stopOpts.SQLInstance)
	return tc.Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.MaxWait, vc)
}

// defaultVirtualClusterName returns the virtual cluster name used for
// the virtual cluster with ID given.
//
// TODO(herko): Allow users to pass in a virtual cluster name.
func defaultVirtualClusterName(virtualClusterID int) string {
	return fmt.Sprintf("virtual-cluster-%d", virtualClusterID)
}

// createVirtualClusterIfNotExistsQuery is used to initialize the
// metadata for the virtual cluster, if it's not initialized already.
// We set up the tvirtual cluster with a lot of initial RUs so that we
// don't encounter throttling by default.
const createVirtualClusterIfNotExistsQuery = `
SELECT
  CASE (SELECT 1 FROM system.tenants WHERE id = %[1]d) IS NULL
  WHEN true
  THEN (
    crdb_internal.create_tenant(%[1]d),
    crdb_internal.update_tenant_resource_limits(%[1]d, 1000000000, 10000, 0, now(), 0)
  )::STRING
  ELSE 'already exists'
  END;`
