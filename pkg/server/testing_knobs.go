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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
)

// TestingKnobs groups testing knobs for the Server.
type TestingKnobs struct {
	// DisableAutomaticVersionUpgrade, if set, temporarily disables the server's
	// automatic version upgrade mechanism.
	DisableAutomaticVersionUpgrade int32 // accessed atomically
	// DefaultZoneConfigOverride, if set, overrides the default zone config defined in `pkg/config/zone.go`
	DefaultZoneConfigOverride *zonepb.ZoneConfig
	// DefaultSystemZoneConfigOverride, if set, overrides the default system zone config defined in `pkg/config/zone.go`
	DefaultSystemZoneConfigOverride *zonepb.ZoneConfig
	// PauseAfterGettingRPCAddress, if non-nil, instructs the server to wait until
	// the channel is closed after getting an RPC serving address.
	PauseAfterGettingRPCAddress chan struct{}
	// SignalAfterGettingRPCAddress, if non-nil, is closed after the server gets
	// an RPC server address.
	SignalAfterGettingRPCAddress chan struct{}
	// ContextTestingKnobs allows customization of the RPC context testing knobs.
	ContextTestingKnobs rpc.ContextTestingKnobs
	// DiagnosticsTestingKnobs allows customization of diagnostics testing knobs.
	DiagnosticsTestingKnobs diagnosticspb.TestingKnobs

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

	// BootstrapVersionOverride, if not empty, will be used for bootstrapping
	// clusters instead of clusterversion.BinaryVersion (if this server is the
	// one bootstrapping the cluster).
	//
	// This can be used by tests to essentially pretend that a new cluster is
	// not starting from scratch, but instead is "created" by a node starting up
	// with engines that had already been bootstrapped, at this
	// BootstrapVersionOverride. For example, it allows convenient creation of a
	// cluster from a 2.1 binary, but that's running at version 2.0.
	//
	// NB: When setting this, you probably also want to set
	// DisableAutomaticVersionUpgrade.
	//
	// TODO(irfansharif): Update users of this testing knob to use the
	// appropriate clusterversion.Handle instead.
	BootstrapVersionOverride roachpb.Version
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
