// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import (
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// StartOpts is a type that combines the start options needed by roachprod and roachtest.
type StartOpts struct {
	RoachprodOpts install.StartOpts
	RoachtestOpts struct {
		Worker bool
	}
}

// DefaultStartOpts returns a StartOpts populated with default values.
func DefaultStartOpts() StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.ScheduleBackups = true
	return startOpts
}

// DefaultStartOptsInMemory returns a StartOpts populated with default values,
// and with in-memory storage
func DefaultStartOptsInMemory() StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.ScheduleBackups = true
	// size=0.3 means 30% of available RAM.
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--store=type=mem,size=0.3")
	return startOpts
}

// DefaultStartOptsNoBackups returns a StartOpts with default values,
// but a scheduled backup will not begin at the start of the roachtest.
func DefaultStartOptsNoBackups() StartOpts {
	return StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
}

// DefaultStartSingleNodeOpts returns StartOpts with default values,
// but no init. This is helpful if node is not going to start gracefully or
// will be terminated as init could fail even if it is a noop for a running
// cluster.
func DefaultStartSingleNodeOpts() StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.SkipInit = true
	return startOpts
}

// DefaultStartVirtualClusterOpts returns StartOpts for starting an external
// process virtual cluster with the given tenant name and SQL instance.
func DefaultStartVirtualClusterOpts(tenantName string, sqlInstance int) StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.Target = install.StartServiceForVirtualCluster
	startOpts.RoachprodOpts.VirtualClusterName = tenantName
	startOpts.RoachprodOpts.SQLInstance = sqlInstance
	// TODO(DarrylWong): remove once #117125 is addressed.
	startOpts.RoachprodOpts.AdminUIPort = 0
	return startOpts
}

// DefaultStartSharedVirtualClusterOpts returns StartOpts for starting a shared
// process virtual cluster with the given tenant name.
func DefaultStartSharedVirtualClusterOpts(tenantName string) StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.Target = install.StartSharedProcessForVirtualCluster
	startOpts.RoachprodOpts.VirtualClusterName = tenantName
	return startOpts
}

// StopOpts is a type that combines the stop options needed by roachprod and roachtest.
type StopOpts struct {
	// TODO(radu): we should use a higher-level abstraction instead of
	// roachprod.StopOpts so we don't have to pass around signal values etc.
	RoachprodOpts roachprod.StopOpts
	RoachtestOpts struct {
		Worker bool
	}
}

// DefaultStopOpts returns a StopOpts populated with default values.
func DefaultStopOpts() StopOpts {
	return StopOpts{RoachprodOpts: roachprod.DefaultStopOpts()}
}
