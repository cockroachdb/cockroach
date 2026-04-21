// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ldrLoopbackKey uniquely identifies a dep resolver → applier backchannel
// within a DistSQL flow.
type ldrLoopbackKey struct {
	flowID    execinfrapb.FlowID
	applierID ldrdecoder.ApplierID
}

// ldrLoopbackChannels holds the backchannel from a dep resolver processor to
// its co-located applier processor.
type ldrLoopbackChannels struct {
	// updateCh carries DependencyUpdates from the dep resolver back to the
	// applier. The dep resolver writes to this channel; the applier's
	// DistDepResolverClient reads from it.
	updateCh chan txnapply.DependencyUpdate
}

// ldrLoopbackMap maps (FlowID, ApplierID) pairs to backchannel resources.
// This enables dep resolver processors to send DependencyUpdates back to their
// co-located applier processors without a DistSQL stream.
type ldrLoopbackMap struct {
	syncutil.Mutex
	channels map[ldrLoopbackKey]*ldrLoopbackChannels
}

var ldrLoopback = &ldrLoopbackMap{
	channels: make(map[ldrLoopbackKey]*ldrLoopbackChannels),
}

// create allocates a backchannel for the given flow and applier and returns a
// cleanup function that removes and closes the channel.
func (m *ldrLoopbackMap) create(
	flowCtx *execinfra.FlowCtx, applierID ldrdecoder.ApplierID,
) (*ldrLoopbackChannels, func()) {
	m.Lock()
	defer m.Unlock()

	key := ldrLoopbackKey{flowID: flowCtx.ID, applierID: applierID}
	chs := &ldrLoopbackChannels{
		updateCh: make(chan txnapply.DependencyUpdate, 1000),
	}
	m.channels[key] = chs
	return chs, func() {
		m.Lock()
		defer m.Unlock()
		delete(m.channels, key)
		close(chs.updateCh)
	}
}

// lookup returns the backchannel for the given flow and applier.
func (m *ldrLoopbackMap) lookup(
	flowCtx *execinfra.FlowCtx, applierID ldrdecoder.ApplierID,
) (*ldrLoopbackChannels, bool) {
	m.Lock()
	defer m.Unlock()
	key := ldrLoopbackKey{flowID: flowCtx.ID, applierID: applierID}
	chs, ok := m.channels[key]
	return chs, ok
}
