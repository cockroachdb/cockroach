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

// StopOpts is a type that combines the stop options needed by roachprod and roachtest.
type StopOpts struct {
	RoachprodOpts roachprod.StopOpts
	RoachtestOpts struct {
		Worker bool
	}
}

// DefaultStopOpts returns a StopOpts populated with default values.
func DefaultStopOpts() StopOpts {
	return StopOpts{RoachprodOpts: roachprod.DefaultStopOpts()}
}
