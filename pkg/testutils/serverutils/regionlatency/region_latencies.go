// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package regionlatency has testing utilities for injecting artificial latency
// into test clusters.
package regionlatency

import (
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/errors"
)

// LatencyMap contains mapping for the speed-of-light delay between a pair
// of regions. Note that this is not a round-trip, but rather it's one-way.
type LatencyMap struct {
	m map[Region]map[Region]OneWayLatency
}

// Region is the name of a region.
type Region = string

// Pair is a pair of regions.
type Pair struct{ A, B Region }

func (p Pair) reversed() Pair { return Pair{A: p.B, B: p.A} }

// TestCluster is implemented both by testcluster.TestCluster and
// democluster.transientCluster.
type TestCluster interface {
	NumServers() int
	Server(i int) serverutils.TestServerInterface
}

// RoundTripLatency is the time to go from a region to another region and back.
type RoundTripLatency = time.Duration

// OneWayLatency is the time to go from a region to another region.
type OneWayLatency = time.Duration

// Apply is used to inject the latency pairs into the InjectedLatencyOracle.
// This step must be done after the servers have been created but before they
// have been allowed to issue RPCs. It is intended to be paired with the
// server testing knob PauseAfterGettingRPCAddresses. If there is no
// InjectedLatencyOracle testing knob or if it does not implement AddrMap, an
// error will be returned.
func (m LatencyMap) Apply(tc TestCluster) error {
	for i, n := 0, tc.NumServers(); i < n; i++ {
		// TODO(#109869): This seems incorrect; what of the latency between
		// SQL servers which use their own, separate RPC service?
		serv := tc.Server(i).SystemLayer()
		serverKnobs, ok := serv.TestingKnobs().Server.(*server.TestingKnobs)
		if !ok {
			return errors.AssertionFailedf(
				"failed to inject latencies: no server testing knobs for server %d", i,
			)
		}
		latencyMap, ok := serverKnobs.ContextTestingKnobs.InjectedLatencyOracle.(AddrMap)
		if !ok {
			return errors.AssertionFailedf(
				"failed to inject latencies: InjectedLatencyOracle for server %d is %T",
				i, serverKnobs.ContextTestingKnobs.InjectedLatencyOracle,
			)
		}
		cfg := serv.ExecutorConfig().(sql.ExecutorConfig)
		srcLocality, ok := cfg.Locality.Find("region")
		if !ok {
			continue
		}
		for j := 0; j < tc.NumServers(); j++ {
			dst := tc.Server(j)
			if dst == serv {
				continue
			}
			dstCfg := dst.ExecutorConfig().(sql.ExecutorConfig)
			dstLocality, ok := dstCfg.Locality.Find("region")
			if !ok {
				continue
			}
			l, ok := m.getLatency(srcLocality, dstLocality)
			if !ok {
				continue
			}
			latencyMap.SetLatency(dst.AdvRPCAddr(), l)
		}
	}
	return nil
}

// ForEachLatencyFrom iterates the LatencyMap for each other region from the
// requested region.
func (m LatencyMap) ForEachLatencyFrom(a Region, f func(b Region, l OneWayLatency)) {
	for r, l := range m.m[a] {
		f(r, l)
	}
}

func (m LatencyMap) getLatency(a, b Region) (OneWayLatency, bool) {
	fromA, ok := m.m[a]
	if !ok {
		return 0, false
	}
	toB, ok := fromA[b]
	return toB, ok
}

// GetRegions returns the set of regions in the map.
func (m LatencyMap) GetRegions() []Region {
	var regions []Region
	for r := range m.m {
		regions = append(regions, r)
	}
	sort.Strings(regions)
	return regions
}

// RoundTripPairs are pairs of round-trip latency between regions.
type RoundTripPairs map[Pair]RoundTripLatency

// ToLatencyMap converts the pairs to a mapping between each two region to
// the respective one-way latency between them.
func (t RoundTripPairs) ToLatencyMap() LatencyMap {
	return makeRegionLatencyMap(t)
}

func makeRegionLatencyMap(pairs RoundTripPairs) LatencyMap {
	m := LatencyMap{
		m: make(map[string]map[string]time.Duration),
	}
	for pair, latency := range pairs {
		insertPair(m, pair, latency/2)
		insertPair(m, pair.reversed(), latency/2)
	}
	return m
}

func insertPair(m LatencyMap, pair Pair, latency RoundTripLatency) {
	regionToLatency, ok := m.m[pair.A]
	if !ok {
		regionToLatency = make(map[string]time.Duration)
		m.m[pair.A] = regionToLatency
	}
	regionToLatency[pair.B] = latency
}

// AddrMap implements rpc.InjectedLatencyOracle.
type AddrMap interface {
	rpc.InjectedLatencyOracle
	SetLatency(addr string, l time.Duration)
}

// MakeAddrMap constructs a new AddrMap for use as the rpc.InjectedLatencyOracle.
func MakeAddrMap() AddrMap { return addrMap{} }

// AddrMap implements rpc.InjectedLatencyOracle.
type addrMap map[string]time.Duration

func (am addrMap) GetLatency(addr string) time.Duration    { return am[addr] }
func (am addrMap) SetLatency(addr string, l time.Duration) { am[addr] = l }
