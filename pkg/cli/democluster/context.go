// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package democluster

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

// Context represents the input configuration and current state
// of a demo cluster.
type Context struct {
	// CliCtx links this demo context to a CLI configuration
	// environment.
	CliCtx *clicfg.Context

	// NumNodes is the requested number of nodes, and is also
	// modified when adding new nodes.
	NumNodes int

	// SQLPoolMemorySize is the size of the memory pool for each SQL
	// server.
	SQLPoolMemorySize int64

	// CacheSize is the size of the storage cache for each KV server.
	CacheSize int64

	// UseEmptyDatabase prevents the auto-creation of a demo database
	// from a workload.
	UseEmptyDatabase bool

	// RunWorkload indicates whether to run a workload in the background
	// after the demo cluster has been initialized.
	RunWorkload bool

	// ExpandSchema indicates whether to expand the schema of the
	// workload. The expansion stops when this number of extra
	// descriptors has been reached.
	ExpandSchema int

	// NameGenOptions configures the name generation options to use
	// during schema expansion.
	NameGenOptions string

	// WorkloadGenerator is the desired workload generator.
	WorkloadGenerator workload.Generator

	// WorkloadMaxQPS controls the amount of queries that can be run per
	// second.
	WorkloadMaxQPS int

	// Localities configures the list of localities available for use
	// by instantiated servers.
	Localities DemoLocalityList

	// GeoPartitionedReplicas requests that the executed workload
	// partition its data across localities. Requires an enterprise
	// license.
	GeoPartitionedReplicas bool

	// SimulateLatency requests that cross-region latencies be simulated
	// across region localities.
	SimulateLatency bool

	// DefaultKeySize is the default size of TLS private keys to use.
	DefaultKeySize int

	// DefaultCALifetime is the default lifetime of CA certs that are
	// generated for the transient cluster.
	DefaultCALifetime time.Duration

	// DefaultCertLifetime is the default lifetime of client certs that
	// are generated for the transient cluster.
	DefaultCertLifetime time.Duration

	// insecure requests that the server be started in "insecure mode".
	// NB: This is obsolete.
	Insecure bool

	// SQLPort is the first SQL port number to use when instantiating
	// servers. Use zero for auto-allocated random ports.
	SQLPort int

	// HTTPPort is the first HTTP port number to use when instantiating
	// servers. Use zero for auto-allocated random ports.
	HTTPPort int

	// ListeningURLFile can be set to a file which is written to after
	// the demo cluster has started, to contain a valid connection URL.
	ListeningURLFile string

	// Multitenant is true if we're starting the demo cluster in
	// multi-tenant mode.
	Multitenant bool

	// DefaultEnableRangefeeds is true if rangefeeds should start
	// out enabled.
	DefaultEnableRangefeeds bool

	// DisableServerController is true if we want to avoid the server
	// controller to instantiate tenant secondary servers.
	DisableServerController bool
}

// IsInteractive returns true if the demo cluster configuration
// is for an interactive session. This exposes the field
// from clicfg.Context if available.
func (demoCtx *Context) IsInteractive() bool {
	return demoCtx.CliCtx != nil && demoCtx.CliCtx.IsInteractive
}
