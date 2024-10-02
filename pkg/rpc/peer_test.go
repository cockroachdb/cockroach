// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

type pmb map[peerKey]*peer // peer map builder

func (m pmb) k(k peerKey) pmb {
	return m.kh(k, true /* healthy */)
}

func (m pmb) kh(k peerKey, healthy bool) pmb {
	if m == nil {
		m = pmb{}
	}
	p := &peer{}
	p.b = circuit.NewBreaker(circuit.Options{
		Name: "test",
	})
	if !healthy {
		circuit.TestingSetTripped(p.b, errors.New("boom"))
	}
	m[k] = p
	return m
}

func (m pmb) p(nodeID roachpb.NodeID, targetAddr string, class ConnectionClass, healthy bool) pmb {
	k := peerKey{TargetAddr: targetAddr, NodeID: nodeID, Class: class}
	return m.kh(k, healthy)
}

func Test_hasSiblingConn(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		t1  = "1.1.1.1:1111"
		t2  = "2.2.2.2:2222"
		t3  = "3.3.3.3:3333"
		n1  = roachpb.NodeID(1)
		n2  = roachpb.NodeID(2)
		n3  = roachpb.NodeID(3)
		def = DefaultClass
		sys = SystemClass
	)

	defSelf := func() peerKey {
		return peerKey{
			TargetAddr: t1,
			NodeID:     n1,
			Class:      def,
		}
	}

	defZeroSelf := func() peerKey {
		return peerKey{
			TargetAddr: t1,
			Class:      def,
		}
	}

	tests := []struct {
		name        string
		m           pmb
		self        peerKey
		wantHealthy bool
		wantOk      bool
	}{
		// In the empty map, defSelf has no sibling.
		{
			name: "empty",
		},

		// First, we test various combinations where our peerKey has a NodeID.
		// We'll test the NodeID==0 case below.

		// In a map that contains only itself, defSelf has no sibling.
		{
			name: "single",
			m:    (&pmb{}).k(defSelf()),
		},
		// In a map that contains defSelf and another healthy but unrelated peer,
		// defSelf finds no sibling.
		{
			name: "unrelated",
			m:    (&pmb{}).k(defSelf()).p(n2, t2, sys, true /* healthy */),
		},
		// Same as previous, but with a curveball: n2 also hails from defSelf's
		// address. (In other words, n2 must have restarted with a listener
		// formerly used by n1, or vice versa). Not a sibling - we're looking
		// for the same NodeID on a different TargetAddr.
		{
			name: "unrelated-switcheroo",
			m:    (&pmb{}).k(defSelf()).p(n2, t1, def, true /* healthy */),
		},
		// Unrelated peer that has a zero NodeID.
		{
			name: "unrelated-switcheroo",
			m:    (&pmb{}).k(defSelf()).p(0, t3, def, true /* healthy */),
		},
		// If a healthy sibling is present, it is found.
		{
			name:   "related-healthy",
			m:      (&pmb{}).k(defSelf()).p(n1, t2, def, true /* healthy */),
			wantOk: true, wantHealthy: true,
		},
		// Like last, but sibling has a different connection class - outcome is the
		// same.
		{
			name:   "related-healthy-sys",
			m:      (&pmb{}).k(defSelf()).p(n1, t2, sys, true /* healthy */),
			wantOk: true, wantHealthy: true,
		},
		// If an unhealthy sibling is present, it is found.
		{
			name:   "related-unhealthy",
			m:      (&pmb{}).k(defSelf()).p(n1, t2, def, false /* healthy */),
			wantOk: true, wantHealthy: false,
		},
		// Ditto with other class.
		{
			name:   "related-unhealthy-sys",
			m:      (&pmb{}).k(defSelf()).p(n1, t2, sys, false /* healthy */),
			wantOk: true, wantHealthy: false,
		},

		// Now we test the NodeID == 0 case.

		// Peer is not sibling of self.
		{
			name: "zero-single",
			m:    (&pmb{}).k(defZeroSelf()),
			self: defZeroSelf(),
		},
		// Peer is not sibling of other peers that have non-matching target addrs.
		{
			name: "zero-unrelated",
			m: (&pmb{}).
				k(defZeroSelf()).
				p(n2, t2, def, true).
				p(n3, t3, def, true).
				p(0, t2, sys, true),
			self: defZeroSelf(),
		},
		// Peer is sibling of NodeID-peer with matching addr.
		{
			name: "zero-matching",
			m: (&pmb{}).
				k(defZeroSelf()).
				p(1, t1, sys, true),
			self:        defZeroSelf(),
			wantOk:      true,
			wantHealthy: true,
		},
		// Ditto but peer is not healthy.
		{
			name: "zero-matching-unhealthy",
			m: (&pmb{}).
				k(defZeroSelf()).
				p(1, t1, sys, false),
			self:        defZeroSelf(),
			wantOk:      true,
			wantHealthy: false,
		},
		// Peer is sibling of zero-peer with matching addr. (It necessarily has
		// a different class, or it would be the same peer).
		{
			name: "zero-matching-zero",
			m: (&pmb{}).
				k(defZeroSelf()).
				p(0, t1, sys, true),
			self:        defZeroSelf(),
			wantOk:      true,
			wantHealthy: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			self := tt.self
			if self.TargetAddr == "" {
				self = defSelf()
			}
			gotHealthy, gotOk := hasSiblingConn(tt.m, self)
			if gotHealthy != tt.wantHealthy {
				t.Errorf("hasSiblingConn() gotHealthy = %v, want %v", gotHealthy, tt.wantHealthy)
			}
			if gotOk != tt.wantOk {
				t.Errorf("hasSiblingConn() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
