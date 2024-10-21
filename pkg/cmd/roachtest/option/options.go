// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

// StartOpts is a type that combines the start options needed by roachprod and roachtest.
type StartOpts struct {
	// StorageNodes is used when starting a virtual cluster, indicating
	// the nodes that should be used as storage nodes. When not set, all
	// nodes should be considered part of the storage cluster.
	StorageNodes NodeListOption
	// SeparateProcessNode is used when starting a virtual cluster,
	// indicating the nodes in which the virtual cluster should be
	// started.
	SeparateProcessNodes NodeListOption
	// WaitForReplication indicates if we should wait for the
	// corresponding replication factor after starting a cockroach
	// process on a node.
	WaitForReplicationFactor int

	RoachprodOpts install.StartOpts
	RoachtestOpts struct {
		Worker bool
	}
}

// StartStopOption allows us to apply optional customizations to
// start or stop options.
type StartStopOption func(interface{})

// DefaultStartOpts returns a StartOpts populated with default values.
func DefaultStartOpts() StartOpts {
	return NewStartOpts()
}

// NewStartOpts returns a StartOpts populated with default values when
// called with no options. Pass customization functions to change the
// start options.
func NewStartOpts(opts ...StartStopOption) StartOpts {
	startOpts := StartOpts{RoachprodOpts: roachprod.DefaultStartOpts()}
	startOpts.RoachprodOpts.ScheduleBackups = true

	for _, opt := range opts {
		opt(&startOpts)
	}

	return startOpts
}

// StartVirtualClusterOpts returns StartOpts for starting an external
// process virtual cluster with the given name and on the given
// nodes. By default, this assigns a fixed SQL instance to the new
// processes. To change this (and allow multiple instances of the same
// virtual cluster to coexist in the same node), use the
// `VirtualClusterInstance` option.
func StartVirtualClusterOpts(name string, nodes NodeListOption, opts ...StartStopOption) StartOpts {
	startOpts := DefaultStartOpts()
	startOpts.RoachprodOpts.Target = install.StartServiceForVirtualCluster
	startOpts.RoachprodOpts.VirtualClusterName = name
	startOpts.SeparateProcessNodes = nodes
	// TODO(DarrylWong): remove once #117125 is addressed.
	startOpts.RoachprodOpts.AdminUIPort = 0

	for _, opt := range opts {
		opt(&startOpts)
	}

	return startOpts
}

// DefaultStartSharedVirtualClusterOpts returns StartOpts for starting a shared
// process virtual cluster with the given name.
func StartSharedVirtualClusterOpts(name string, opts ...StartStopOption) StartOpts {
	startOpts := DefaultStartOpts()
	startOpts.RoachprodOpts.Target = install.StartSharedProcessForVirtualCluster
	startOpts.RoachprodOpts.VirtualClusterName = name

	for _, opt := range opts {
		opt(&startOpts)
	}

	return startOpts
}

// StopOpts is a type that combines the stop options needed by roachprod and roachtest.
type StopOpts struct {
	// SeparateProcessNodes is used when stopping virtual clusters. It
	// indicates the nodes in which we should stop the virtual cluster.
	SeparateProcessNodes NodeListOption

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

// NewStopOpts returns a StopOpts populated with default values when
// called with no options. Pass customization functions to change the
// stop options.
func NewStopOpts(opts ...StartStopOption) StopOpts {
	stopOpts := StopOpts{RoachprodOpts: roachprod.DefaultStopOpts()}
	for _, opt := range opts {
		opt(&stopOpts)
	}

	return stopOpts
}

// StopSharedVirtualClusterOpts creates StopOpts that can be used to
// stop the shared process virtual cluster with the given name.
func StopSharedVirtualClusterOpts(virtualClusterName string) StopOpts {
	stopOpts := DefaultStopOpts()
	stopOpts.RoachprodOpts.VirtualClusterName = virtualClusterName

	return stopOpts
}

// StopVirtualClusterOpts returns stop options that can be used to
// stop the SQL instance process serving the virtual cluster with the
// given name. If more than one instance of the same virtual cluster
// is running on the same node, a specific instance can be passed with
// the `VirtualClusterInstance` option.
func StopVirtualClusterOpts(
	virtualClusterName string, nodes NodeListOption, opts ...StartStopOption,
) StopOpts {
	stopOpts := DefaultStopOpts()
	stopOpts.RoachprodOpts.VirtualClusterName = virtualClusterName
	stopOpts.SeparateProcessNodes = nodes

	for _, opt := range opts {
		opt(&stopOpts)
	}

	return stopOpts
}

// InMemoryDB can be used to configure StartOpts that start in-memory
// cockroach processes. The `size` argument must be in [0,1) and
// indicates the percentage of RAM to be used by the process.
func InMemoryDB(size float64) StartStopOption {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StartOpts:
			opts.RoachprodOpts.ExtraArgs = append(
				opts.RoachprodOpts.ExtraArgs,
				fmt.Sprintf("--store=type=mem,size=%.1f", size),
			)
		}
	}
}

// WaitForReplication tells the start process to wait for at least 3X
// replication after starting a cockroach process.
func WaitForReplication() StartStopOption {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StartOpts:
			opts.WaitForReplicationFactor = 3
		}
	}
}

func SkipInit(opts interface{}) {
	switch opts := opts.(type) {
	case *StartOpts:
		opts.RoachprodOpts.SkipInit = true
	}
}

// Tag sets a process tag when stopping processes. Useful if we want
// to kill a cockroach process that was started with `settings.TagOption`.
func Tag(tag string) func(opts interface{}) {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StopOpts:
			opts.RoachprodOpts.ProcessTag = tag
		}
	}
}

// WithInitTarget allows the caller to configure which node is used as
// `InitTarget` when starting cockroach. Specially useful when
// starting clusters in a subset of VMs in the cluster that doesn't
// include the default init target (node 1).
func WithInitTarget(node int) StartStopOption {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StartOpts:
			opts.RoachprodOpts.InitTarget = node
		}
	}
}

// VirtualClusterInstance can be used to indicate the SQL instance to
// start or stop. Only used when starting multiple instances (SQL
// processes) of the same virtual cluster on the same node.
func VirtualClusterInstance(instance int) StartStopOption {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StartOpts:
			opts.RoachprodOpts.SQLInstance = instance
		case *StopOpts:
			opts.RoachprodOpts.SQLInstance = instance
		}
	}
}

// StorageCluster indicates the set of nodes in the cluster that
// should be used as storage cluster when starting a separate process
// virtual cluster. By default, all nodes are considered part of the
// storage cluster.
func StorageCluster(nodes NodeListOption) StartStopOption {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StartOpts:
			opts.StorageNodes = nodes
		}
	}
}

// NoBackupSchedule can be used to generate StartOpts that skip the
// creation of the default backup schedule.
func NoBackupSchedule(opts interface{}) {
	switch opts := opts.(type) {
	case *StartOpts:
		opts.RoachprodOpts.ScheduleBackups = false
	}
}

// DisableWALFailover can be used to generate StartOpts that disable use of WAL
// failover.
func DisableWALFailover(opts interface{}) {
	switch opts := opts.(type) {
	case *StartOpts:
		opts.RoachprodOpts.WALFailover = ""
	}
}

// Graceful performs a graceful stop of the cockroach process.
func Graceful(gracePeriodSeconds int) func(interface{}) {
	return func(opts interface{}) {
		switch opts := opts.(type) {
		case *StopOpts:
			opts.RoachprodOpts.Sig = 15 // SIGTERM
			opts.RoachprodOpts.Wait = true
			opts.RoachprodOpts.GracePeriod = gracePeriodSeconds
		}
	}
}

// WithNodes returns a RunOptions that will run on the given nodes.
func WithNodes(nodes NodeListOption) install.RunOptions {
	return install.WithNodes(nodes.InstallNodes())
}
