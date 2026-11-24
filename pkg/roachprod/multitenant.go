// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

// StartServiceForVirtualCluster starts SQL/HTTP instances for a
// virtual cluster. If the `startOpts` indicate that the service is
// external, this will create processes on an underlying
// roachprod-created cluster of VMs. The SQL/HTTP instances connect to
// a storage cluster, which must be running already.
func StartServiceForVirtualCluster(
	ctx context.Context,
	l *logger.Logger,
	storageCluster string,
	startOpts install.StartOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	// TODO(radu): do we need separate clusterSettingsOpts for the storage cluster?
	sc, err := newCluster(l, storageCluster, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	startOpts.StorageCluster = sc

	// If we are starting a service in shared process mode, `Start` is
	// called on the storage cluster itself.
	startCluster := sc

	if startOpts.Target == install.StartServiceForVirtualCluster {
		l.Printf("Starting SQL/HTTP instances for the virtual cluster")
		// If we are starting a service in external process mode, `Start`
		// is called on the nodes where the SQL server processed should be
		// created.
		ec, err := newCluster(l, startOpts.VirtualClusterLocation, clusterSettingsOpts...)
		if err != nil {
			return err
		}

		startCluster = ec
	}
	return startCluster.Start(ctx, l, startOpts)
}

// StopServiceForVirtualCluster stops SQL instance processes on the virtualCluster given.
func StopServiceForVirtualCluster(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	secure install.SecureOption,
	stopOpts StopOpts,
) error {
	c, err := newCluster(l, clusterName, secure)
	if err != nil {
		return err
	}

	label := install.VirtualClusterLabel(stopOpts.VirtualClusterName, stopOpts.SQLInstance)
	return c.Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.GracePeriod, label)
}

// StartSQLProxy starts a SQL proxy process on the specified node. The proxy
// routes connections to virtual clusters based on the provided routing rules.
// This is useful for testing multi-tenant scenarios with connection pooling
// and routing. Returns a database connection to the proxy.
func StartSQLProxy(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	node install.Node,
	proxyOpts install.SQLProxyOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	c, err := newCluster(l, clusterName, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	return c.StartSQLProxy(ctx, l, node, proxyOpts)
}

// StopSQLProxy stops the SQL proxy process on all nodes in the cluster.
func StopSQLProxy(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	node install.Node,
	secure install.SecureOption,
	opts install.SQLProxyOpts,
) error {
	c, err := newCluster(l, clusterName, secure)
	if err != nil {
		return err
	}

	return c.StopSQLProxy(ctx, l, node, opts)
}

func SQLProxyURL(
	l *logger.Logger,
	clusterName string,
	secure install.SecureOption,
	virtualClusterName string,
	tenantID int,
	certsDir string,
	opts install.SQLProxyOpts,
) (string, error) {
	c, err := GetClusterFromCache(l, clusterName, secure)
	if err != nil {
		return "", err
	}

	if len(c.Nodes) != 1 {
		return "", errors.Errorf("expected 1 node, got %d", len(c.Nodes))
	}

	return c.SQLProxyURL(c.Nodes[0], virtualClusterName, tenantID, certsDir, opts), nil
}

// StartProxyDirectory starts a directory server process on the specified node.
// The directory server provides a static pod registry for SQL proxies to discover tenant pods.
func StartProxyDirectory(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	node install.Node,
	dirOpts install.DirectoryServerOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	c, err := newCluster(l, clusterName, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	return c.StartDirectoryServer(ctx, l, node, dirOpts)
}

// StopProxyDirectory stops the directory server process on the specified node.
func StopProxyDirectory(
	ctx context.Context,
	l *logger.Logger,
	clusterName string,
	node install.Node,
	dirOpts install.DirectoryServerOpts,
	clusterSettingsOpts ...install.ClusterSettingOption,
) error {
	c, err := newCluster(l, clusterName, clusterSettingsOpts...)
	if err != nil {
		return err
	}

	return c.StopDirectoryServer(ctx, l, node, dirOpts)
}
