// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestutil

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// SystemInterfaceSystemdUnitName is a convenience function that
// returns the systemd unit name for the system interface
func SystemInterfaceSystemdUnitName() string {
	return install.VirtualClusterLabel(install.SystemInterfaceName, 0)
}

// SetDefaultSQLPort sets the SQL port to the default of 26257 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultSQLPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.SQLPort = config.DefaultSQLPort
	}
}

// SetDefaultAdminUIPort sets the AdminUI port to the default of 26258 if it is
// a non-local cluster. Local clusters don't support changing the port.
func SetDefaultAdminUIPort(c cluster.Cluster, opts *install.StartOpts) {
	if !c.IsLocal() {
		opts.AdminUIPort = config.DefaultAdminUIPort
	}
}
