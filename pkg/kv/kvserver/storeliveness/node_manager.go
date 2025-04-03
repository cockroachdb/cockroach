// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// NodeContainer is a container for a node's storeliveness state which is shared
// across all per-store SupportManagers.
//
// The container currently serves as merely a convenient way to pass around
// these dependencies and hand them to the SupportManagers upon construction,
// but it could be made to be a more functional abstraction in the future. For
// example, StoreManagers could be given a reference to the NodeContainer, and
// it could be responsible for creating SupportManagers.
type NodeContainer struct {
	Options         Options
	Transport       *Transport
	Knobs           *TestingKnobs
	HeartbeatTicker *timeutil.BroadcastTicker
}

// NewNodeContainer creates a new NodeContainer.
func NewNodeContainer(
	stopper *stop.Stopper, options Options, transport *Transport, knobs *TestingKnobs,
) *NodeContainer {
	ticker := timeutil.NewBroadcastTicker(options.HeartbeatInterval)
	stopper.AddCloser(stop.CloserFn(ticker.Stop))
	return &NodeContainer{
		Options:         options,
		Transport:       transport,
		Knobs:           knobs,
		HeartbeatTicker: ticker,
	}
}

// SupportManagerKnobs returns the SupportManagerKnobs from the TestingKnobs..
func (n *NodeContainer) SupportManagerKnobs() *SupportManagerKnobs {
	if n.Knobs != nil {
		return &n.Knobs.SupportManagerKnobs
	}
	return nil
}
