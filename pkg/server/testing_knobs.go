// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TestingKnobs groups testing knobs for the Server.
type TestingKnobs struct {
	// DisableAutomaticVersionUpgrade, if set, temporarily disables the server's
	// automatic version upgrade mechanism (until the channel is closed).
	DisableAutomaticVersionUpgrade chan struct{}
	// DefaultZoneConfigOverride, if set, overrides the default zone config
	// defined in `pkg/config/zone.go`.
	DefaultZoneConfigOverride *zonepb.ZoneConfig
	// DefaultSystemZoneConfigOverride, if set, overrides the default system
	// zone config defined in `pkg/config/zone.go`
	DefaultSystemZoneConfigOverride *zonepb.ZoneConfig
	// SignalAfterGettingRPCAddress, if non-nil, is closed after the server gets
	// an RPC server address, and prior to waiting on PauseAfterGettingRPCAddress below.
	SignalAfterGettingRPCAddress chan struct{}
	// PauseAfterGettingRPCAddress, if non-nil, instructs the server to wait until
	// the channel is closed after determining its RPC serving address, and after
	// closing SignalAfterGettingRPCAddress.
	PauseAfterGettingRPCAddress chan struct{}
	// ContextTestingKnobs allows customization of the RPC context testing knobs.
	ContextTestingKnobs rpc.ContextTestingKnobs
	// DiagnosticsTestingKnobs allows customization of diagnostics testing knobs.
	DiagnosticsTestingKnobs diagnostics.TestingKnobs

	// If set, use this listener for RPC (and possibly SQL, depending on
	// the SplitListenSQL setting), instead of binding a new listener.
	// This is useful in tests that need an ephemeral listening port but
	// must know it before the server starts.
	//
	// When this is used, the advertise address should also be set to
	// match.
	//
	// The Server takes responsibility for closing this listener.
	// TODO(bdarnell): That doesn't give us a good way to clean up if the
	// server fails to start.
	RPCListener net.Listener

	// ClusterVersionOverride can be used to override the version of the cluster
	// (assuming that one has to be created).
	//
	// Normally (when this knob isn't used), the cluster is initialized at
	// cluster.Settings.Version.LatestVersion().
	//
	// When ClusterVersionOverride is set, the cluster will be at this version
	// once initialization is complete. Note that we cannot bootstrap clusters at
	// arbitrary versions - we can only bootstrap clusters at the Latest version
	// and at final versions of previous supported releases. The cluster will be
	// bootstrapped at the most recent bootstrappable version that is at most
	// ClusterVersionOverride; after all the servers in the test cluster have been
	// started, `SET CLUSTER SETTING version = ClusterVersionOverride` will be run
	// to step through the upgrades until the specified version.
	//
	// ClusterVersionOverride is also used when advertising this server's binary
	// version when sending out join requests.
	//
	// NB: When setting this, you probably also want to set
	// DisableAutomaticVersionUpgrade.
	ClusterVersionOverride roachpb.Version
	// An (additional) callback invoked whenever a
	// node is permanently removed from the cluster.
	OnDecommissionedCallback func(id roachpb.NodeID)
	// StickyVFSRegistry manages the lifecycle of sticky in memory engines,
	// which can be enabled via base.StoreSpec.StickyVFSID.
	//
	// When supplied to a TestCluster, StickyVFSIDs will be associated auto-
	// matically to the StoreSpecs used.
	StickyVFSRegistry fs.StickyRegistry
	// WallClock is used to inject a custom clock for testing the server. It is
	// typically either an hlc.HybridManualClock or hlc.ManualClock.
	WallClock hlc.WallClock

	// ImportTimeseriesFile, if set, is a file created via `DumpRaw` that written
	// back to the KV layer upon server start.
	ImportTimeseriesFile string
	// ImportTimeseriesMappingFile points to a file containing a YAML map from storeID
	// to nodeID, for use with ImportTimeseriesFile.
	ImportTimeseriesMappingFile string
	// DrainSleepFn used in testing to override the usual sleep function with
	// a custom function that counts the number of times the sleep function is called.
	DrainSleepFn func(time.Duration)

	// BlobClientFactory supplies a BlobClientFactory for
	// use by servers.
	BlobClientFactory blobs.BlobClientFactory

	// StubTimeNow allows tests to override the timeutil.Now() function used
	// in the jobs endpoint to calculate earliest_retained_time.
	StubTimeNow func() time.Time

	// RequireGracefulDrain, if set, causes a shutdown to fail with a log.Fatal
	// if the server is not gracefully drained prior to its stopper shutting down.
	RequireGracefulDrain bool

	// DrainReportCh, if set, is a channel that will be notified when
	// the SQL service shuts down.
	DrainReportCh chan struct{}

	// ShutdownTenantConnectorEarlyIfNoRecordPresent, if set, will cause the
	// tenant connector to be shut down early if no record is present in the
	// system.tenants table. This is useful for tests that want to verify that
	// the tenant connector can't start when the record doesn't exist.
	ShutdownTenantConnectorEarlyIfNoRecordPresent bool

	// IterateNodesDialCallback is used to mock dial errors in a cluster
	// fan-out. It is invoked by the dialFn argument of server.iterateNodes.
	IterateNodesDialCallback func(nodeID roachpb.NodeID) error

	// IterateNodesNodeCallback is used to mock errors of the rpc invoked
	// on a remote node in a cluster fan-out. It is invoked by the nodeFn argument
	// of server.iterateNodes.
	IterateNodesNodeCallback func(ctx context.Context, nodeID roachpb.NodeID) error

	// DialNodeCallback is used to mock dial errors when dialing a node. It is
	// invoked by the dialNode method of server.serverIterator.
	DialNodeCallback func(ctx context.Context, nodeID roachpb.NodeID) error

	// DisableSettingsWatcher disables the watcher that monitors updates
	// to system.settings.
	DisableSettingsWatcher bool

	TenantAutoUpgradeInfo chan struct {
		Status    int
		UpgradeTo roachpb.Version
	}

	// TenantAutoUpgradeLoopFrequency indicates how often the tenant
	// auto upgrade loop will check if the tenant can be auto-upgraded.
	TenantAutoUpgradeLoopFrequency time.Duration

	// EnvironmentSampleInterval overrides base.DefaultMetricsSampleInterval when used to construct sampleEnvironmentCfg.
	EnvironmentSampleInterval time.Duration
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
