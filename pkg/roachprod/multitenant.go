// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"

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
	ctx context.Context, l *logger.Logger, clusterName string, secure bool, stopOpts StopOpts,
) error {
	c, err := newCluster(l, clusterName, install.SecureOption(secure))
	if err != nil {
		return err
	}

	label := install.VirtualClusterLabel(stopOpts.VirtualClusterName, stopOpts.SQLInstance)
	return c.Stop(ctx, l, stopOpts.Sig, stopOpts.Wait, stopOpts.GracePeriod, label)
}
