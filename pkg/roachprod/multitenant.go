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
)

// StartServiceForVirtualCluster starts SQL/HTTP instances for a
// virtual cluster. If the `startOpts` indicate that the service is
// external, this will create processes on an underlying
// roachprod-created cluster of VMs. The SQL/HTTP instances connect to
// a storage cluster, which must be running already.
func StartServiceForVirtualCluster(
	ctx context.Context,
	l *logger.Logger,
	externalCluster string,
	storageCluster string,
	startOpts install.StartOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	// TODO(radu): do we need separate clusterSettingsOpts for the storage cluster?
	sc, err := newCluster(l, storageCluster, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	var kvAddrs []string
	for _, node := range sc.Nodes {
		port, err := sc.NodePort(ctx, node)
		if err != nil {
			return err
		}
		kvAddrs = append(kvAddrs, fmt.Sprintf("%s:%d", sc.Host(node), port))
	}
	startOpts.KVAddrs = strings.Join(kvAddrs, ",")
	startOpts.KVCluster = sc

	var startCluster *install.SyncedCluster
	if externalCluster == "" {
		// If we are starting a service in shared process mode, `Start` is
		// called on the storage cluster itself.
		startCluster = sc
	} else {
		// If we are starting a service in external process mode, `Start`
		// is called on the nodes where the SQL server processed should be
		// created.
		ec, err := newCluster(l, externalCluster, clusterSettingsOpts...)
		if err != nil {
			return err
		}

		startCluster = ec
	}

	if startOpts.Target == install.StartServiceForVirtualCluster {
		l.Printf("Starting SQL/HTTP instances for the virtual cluster")
	}
	return startCluster.Start(ctx, l, startOpts)
}

// StopServiceForVirtualCluster stops SQL instance processes on the virtualCluster given.
func StopServiceForVirtualCluster(
	ctx context.Context, l *logger.Logger, clusterName string, secure bool, stopOpts StopOpts,
) error {
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return err
	}

	label := install.VirtualClusterLabel(stopOpts.VirtualClusterName, stopOpts.SQLInstance)
	return c.Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.MaxWait, label)
}
