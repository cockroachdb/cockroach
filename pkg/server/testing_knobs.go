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

	// BinaryVersionOverride overrides the binary version that the CRDB server
	// will end up running. This value could also influence what version the
	// cluster is bootstrapped at.
	//
	// This value, when set, influences test cluster/server creation in two
	// different ways:
	//
	// Case 1:
	// ------
	// If the test has not overridden the
	// `cluster.Settings.Version.MinSupportedVersion`, then the cluster will be
	// bootstrapped at `minSupportedVersion`  (if this server is the one
	// bootstrapping the cluster). After all the servers in the test cluster have
	// been started, `SET CLUSTER SETTING version = BinaryVersionOverride` will be
	// run to step through the upgrades until the specified override.
	//
	// Case 2:
	// ------
	// If the test has overridden the
	// `cluster.Settings.Version.MinSupportedVersion` then it is not safe for us
	// to bootstrap at `minSupportedVersion` as it might be less than the
	// overridden minimum supported version. Furthermore, we do not have the
	// initial cluster data (system tables etc.) to bootstrap at the overridden
	// minimum supported version. In this case we bootstrap at
	// `BinaryVersionOverride` and populate the cluster with initial data
	// corresponding to the `binaryVersion`. In other words no upgrades are
	// *really* run and the server only thinks that it is running at
	// `BinaryVersionOverride`. Tests that fall in this category should be audited
	// for correctness.
	//
	// The version that we bootstrap at is also used when advertising this
	// server's binary version when sending out join requests.
	//
	// NB: When setting this, you probably also want to set
	// DisableAutomaticVersionUpgrade.
	BinaryVersionOverride roachpb.Version
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

	// As of September 2023, only `v23.1` and master support shared process tenants. `v23.2` is not
	// cut yet so the difference between the current binary version on master and v23.1 is only in the
	// Internal version (both are major=23 minor=1). We only trigger shared process tenant auto upgrade
	// on changes to major/minor versions but since we can only start shared process tenants in `v23.1`,
	// there will not be any change to major/minor versions when upgrading from `v23.1` to master and
	// we won't be able to test this new feature. This testing knob allows `TestTenantAutoUpgrade` to
	// auto upgrade on changes to the Internal version.
	// // TODO(ahmad/healthy-pod): Remove this once `v23.2` is cut and update `TestTenantAutoUpgrade`
	// to reflect the changes.
	AllowTenantAutoUpgradeOnInternalVersionChanges bool

	// EnvironmentSampleInterval overrides base.DefaultMetricsSampleInterval when used to construct sampleEnvironmentCfg.
	EnvironmentSampleInterval time.Duration
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
