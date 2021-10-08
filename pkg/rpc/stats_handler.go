// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/sync/syncmap"
	"google.golang.org/grpc/stats"
)

type remoteAddrKey struct{}

// Stats stores network statistics between this node and another.
type Stats struct {
	count    int64
	incoming int64
	outgoing int64
}

// Count returns the total number of RPCs.
func (s *Stats) Count() int64 {
	return atomic.LoadInt64(&s.count)
}

// Incoming returns the total bytes of incoming network traffic.
func (s *Stats) Incoming() int64 {
	return atomic.LoadInt64(&s.incoming)
}

// Outgoing returns the total bytes of outgoing network traffic.
func (s *Stats) Outgoing() int64 {
	return atomic.LoadInt64(&s.outgoing)
}

func (s *Stats) record(rpcStats stats.RPCStats) {
	switch v := rpcStats.(type) {
	case *stats.InHeader:
		atomic.AddInt64(&s.incoming, int64(v.WireLength))
	case *stats.InPayload:
		// TODO(spencer): remove the +5 offset on wire length here, which
		// is a temporary stand-in for the missing GRPC framing offset.
		// See: https://github.com/grpc/grpc-go/issues/1647.
		atomic.AddInt64(&s.incoming, int64(v.WireLength+5))
	case *stats.InTrailer:
		atomic.AddInt64(&s.incoming, int64(v.WireLength))
	case *stats.OutHeader:
		// No wire length.
	case *stats.OutPayload:
		atomic.AddInt64(&s.outgoing, int64(v.WireLength))
	case *stats.OutTrailer:
		atomic.AddInt64(&s.outgoing, int64(v.WireLength))
	case *stats.End:
		atomic.AddInt64(&s.count, 1)
	}
}

type clientStatsHandler struct {
	stats *Stats
}

var _ stats.Handler = &clientStatsHandler{}

// TagRPC implements the grpc.stats.Handler interface.
func (cs *clientStatsHandler) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC implements the grpc.stats.Handler interface.
func (cs *clientStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	cs.stats.record(rpcStats)
}

// TagConn implements the grpc.stats.Handler interface.
func (cs *clientStatsHandler) TagConn(ctx context.Context, cti *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn implements the grpc.stats.Handler interface.
func (cs *clientStatsHandler) HandleConn(context.Context, stats.ConnStats) {
}

// StatsHandler manages a map of Stats objects, one per connection.
// It implements grpc.stats.Handler, and is used directly for any
// incoming connections which connect to this node's server. It uses
// the newClient() method to return handlers for use with outgoing
// client connections from this node to remote nodes.
type StatsHandler struct {
	// stats is a map from remote targets to Stats objects. Note that we
	// never remove items from this map; because we don't expect to add
	// and remove sufficiently many nodes, this should be fine in practice.
	stats syncmap.Map
}

var _ stats.Handler = &StatsHandler{}

// newClient returns a new clientStatsHandler which references the stats
// object bound to the specified target remote address.
func (sh *StatsHandler) newClient(target string) stats.Handler {
	value, _ := sh.stats.LoadOrStore(target, &Stats{})
	return &clientStatsHandler{
		stats: value.(*Stats),
	}
}

// TagRPC implements the grpc.stats.Handler interface. This
// interface is used directly for server-side stats recording.
func (sh *StatsHandler) TagRPC(ctx context.Context, rti *stats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC implements the grpc.stats.Handler interface. This
// interface is used directly for server-side stats recording. We
// consult the provided context for the remote address and use that
// to key into our stats map in order to properly update incoming
// and outgoing throughput for the implicated remote node.
func (sh *StatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	remoteAddr, ok := ctx.Value(remoteAddrKey{}).(string)
	if !ok {
		log.Health.Warningf(ctx, "unable to record stats (%+v); remote addr not found in context", rpcStats)
		return
	}
	// There is a race here, but it's meaningless in practice. Worst
	// case is we fail to record a handful of observations. We do
	// this to avoid creating a new Stats object on every invocation.
	value, ok := sh.stats.Load(remoteAddr)
	if !ok {
		value, _ = sh.stats.LoadOrStore(remoteAddr, &Stats{})
	}
	value.(*Stats).record(rpcStats)
}

// TagConn implements the grpc.stats.Handler interface. This interface
// is used directly for server-side stats recording. We tag the
// provided context with the remote address provided by the
// ConnTagInfo, and use that to properly update the Stats object
// belonging to that remote address.
func (sh *StatsHandler) TagConn(ctx context.Context, cti *stats.ConnTagInfo) context.Context {
	return context.WithValue(ctx, remoteAddrKey{}, cti.RemoteAddr.String())
}

// HandleConn implements the grpc.stats.Handler interface. This
// interface is used directly for server-side stats recording.
func (sh *StatsHandler) HandleConn(context.Context, stats.ConnStats) {
}
