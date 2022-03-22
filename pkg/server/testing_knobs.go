// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
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

	// BinaryVersionOverride overrides the binary version the CRDB server thinks
	// it's running.
	//
	// This is consulted when bootstrapping clusters, opting to do it at the
	// override instead of clusterversion.BinaryVersion (if this server is the
	// one bootstrapping the cluster). This can als be used by tests to
	// essentially that a new cluster is not starting from scratch, but instead
	// is "created" by a node starting up with engines that had already been
	// bootstrapped, at this BinaryVersionOverride. For example, it allows
	// convenient creation of a cluster from a 2.1 binary, but that's running at
	// version 2.0.
	//
	// It's also used when advertising this server's binary version when sending
	// out join requests.
	//
	// NB: When setting this, you probably also want to set
	// DisableAutomaticVersionUpgrade.
	//
	// TODO(irfansharif): Update users of this testing knob to use the
	// appropriate clusterversion.Handle instead.
	BinaryVersionOverride roachpb.Version
	// An (additional) callback invoked whenever a
	// node is permanently removed from the cluster.
	OnDecommissionedCallback func(livenesspb.Liveness)
	// StickyEngineRegistry manages the lifecycle of sticky in memory engines,
	// which can be enabled via base.StoreSpec.StickyInMemoryEngineID.
	//
	// When supplied to a TestCluster, StickyEngineIDs will be associated auto-
	// matically to the StoreSpecs used.
	StickyEngineRegistry StickyInMemEnginesRegistry
	// Clock Source used to an inject a custom clock for testing the server. It is
	// typically either an hlc.HybridManualClock or hlc.ManualClock.
	ClockSource func() int64

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
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
