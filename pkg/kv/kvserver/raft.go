// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftlogger"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// maxRaftMsgType is the maximum value in the raft.MessageType enum.
const maxRaftMsgType = raftpb.MsgDeFortifyLeader

func init() {
	for v := range raftpb.MessageType_name {
		typ := raftpb.MessageType(v)
		if typ > maxRaftMsgType {
			panic(fmt.Sprintf("raft.MessageType (%s) with value larger than maxRaftMsgType", typ))
		}
	}
}

// init installs an adapter to use clog for log messages from raft which
// don't belong to any range.
func init() {
	raftlogger.SetLogger(&raftLogger{ctx: context.Background()})
}

// *clogLogger implements the raft.Logger interface. Note that all methods
// must be defined on the pointer type rather than the value type because
// (at least in the go 1.4 compiler), methods on a value type called through
// an interface pointer go through an additional layer of indirection that
// appears on the stack, and would make all our stack frame offsets incorrect.
//
// Raft is fairly verbose at the "info" level, so we map "info" messages to
// clog.V(1) and "debug" messages to clog.V(2).
//
// This file is named raft.go instead of something like logger.go because this
// file's name is used to determine the vmodule parameter: --vmodule=raft=1
type raftLogger struct {
	ctx context.Context
}

func (r *raftLogger) Debug(v ...interface{}) {
	if log.V(3) {
		log.InfofDepth(r.ctx, 1, "", v...)
	}
}

func (r *raftLogger) Debugf(format string, v ...interface{}) {
	if log.V(3) {
		log.InfofDepth(r.ctx, 1, format, v...)
	}
}

func (r *raftLogger) Info(v ...interface{}) {
	if log.V(2) {
		log.InfofDepth(r.ctx, 1, "", v...)
	}
}

func (r *raftLogger) Infof(format string, v ...interface{}) {
	if log.V(2) {
		log.InfofDepth(r.ctx, 1, format, v...)
	}
}

func (r *raftLogger) Warning(v ...interface{}) {
	log.WarningfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Warningf(format string, v ...interface{}) {
	log.WarningfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Error(v ...interface{}) {
	log.ErrorfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Errorf(format string, v ...interface{}) {
	log.ErrorfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Fatal(v ...interface{}) {
	wrapNumbersAsSafe(v)
	log.FatalfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Fatalf(format string, v ...interface{}) {
	wrapNumbersAsSafe(v)
	log.FatalfDepth(r.ctx, 1, format, v...)
}

func (r *raftLogger) Panic(v ...interface{}) {
	wrapNumbersAsSafe(v)
	log.FatalfDepth(r.ctx, 1, "", v...)
}

func (r *raftLogger) Panicf(format string, v ...interface{}) {
	wrapNumbersAsSafe(v)
	log.FatalfDepth(r.ctx, 1, format, v...)
}

func wrapNumbersAsSafe(v ...interface{}) {
	for i := range v {
		switch v[i].(type) {
		case uint:
			v[i] = redact.Safe(v[i])
		case uint8:
			v[i] = redact.Safe(v[i])
		case uint16:
			v[i] = redact.Safe(v[i])
		case uint32:
			v[i] = redact.Safe(v[i])
		case uint64:
			v[i] = redact.Safe(v[i])
		case int:
			v[i] = redact.Safe(v[i])
		case int8:
			v[i] = redact.Safe(v[i])
		case int16:
			v[i] = redact.Safe(v[i])
		case int32:
			v[i] = redact.Safe(v[i])
		case int64:
			v[i] = redact.Safe(v[i])
		case float32:
			v[i] = redact.Safe(v[i])
		case float64:
			v[i] = redact.Safe(v[i])
		default:
		}
	}
}

func verboseRaftLoggingEnabled() bool {
	return log.V(5)
}

func logRaftReady(ctx context.Context, ready raft.Ready) {
	if !verboseRaftLoggingEnabled() {
		return
	}

	var buf bytes.Buffer
	if ready.SoftState != nil {
		fmt.Fprintf(&buf, "  SoftState updated: %+v\n", *ready.SoftState)
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		fmt.Fprintf(&buf, "  HardState updated: %+v\n", ready.HardState)
	}
	for i, e := range ready.Entries {
		fmt.Fprintf(&buf, "  New Entry[%d]: %.200s\n",
			i, raft.DescribeEntry(e, raftEntryFormatter))
	}
	for i, e := range ready.CommittedEntries {
		fmt.Fprintf(&buf, "  Committed Entry[%d]: %.200s\n",
			i, raft.DescribeEntry(e, raftEntryFormatter))
	}
	if !raft.IsEmptySnap(ready.Snapshot) {
		snap := ready.Snapshot
		snap.Data = nil
		fmt.Fprintf(&buf, "  Snapshot updated: %v\n", snap)
	}
	for i, m := range ready.Messages {
		fmt.Fprintf(&buf, "  Outgoing Message[%d]: %.200s\n",
			i, raft.DescribeMessage(m, raftEntryFormatter))
	}
	log.Infof(ctx, "raft ready (must-sync=%t)\n%s", ready.MustSync, buf.String())
}

func raftEntryFormatter(data []byte) string {
	if len(data) == 0 {
		return "[empty]"
	}
	// NB: a raft.EntryFormatter is only invoked for EntryNormal (raft methods
	// that call this take care of unwrapping the ConfChange), and since
	// len(data)>0 it has to be {Deprecated,}EntryEncoding{Standard,Sideloaded}
	// and they are encoded identically.
	cmdID, data := raftlog.DecomposeRaftEncodingStandardOrSideloaded(data)
	return fmt.Sprintf("[%x] [%d]", cmdID, len(data))
}

var raftMessageRequestPool = sync.Pool{
	New: func() interface{} {
		return &kvserverpb.RaftMessageRequest{}
	},
}

func newRaftMessageRequest() *kvserverpb.RaftMessageRequest {
	return raftMessageRequestPool.Get().(*kvserverpb.RaftMessageRequest)
}

func releaseRaftMessageRequest(m *kvserverpb.RaftMessageRequest) {
	*m = kvserverpb.RaftMessageRequest{}
	raftMessageRequestPool.Put(m)
}

// traceEntries records the provided event for all proposals corresponding
// to the entries contained in ents. The vmodule level for raft must be at
// least 1.
func (r *Replica) traceEntries(ents []raftpb.Entry, event string) {
	if log.V(1) || r.store.TestingKnobs().TraceAllRaftEvents {
		ids := extractIDs(nil, ents)
		traceProposals(r, ids, event)
	}
}

// traceMessageSends records the provided event for all proposals contained in
// in entries contained in msgs. The vmodule level for raft must be at
// least 1.
func (r *Replica) traceMessageSends(msgs []raftpb.Message, event string) {
	if log.V(1) || r.store.TestingKnobs().TraceAllRaftEvents {
		var ids []kvserverbase.CmdIDKey
		for _, m := range msgs {
			ids = extractIDs(ids, m.Entries)
		}
		traceProposals(r, ids, event)
	}
}

// extractIDs decodes and appends each of the ids corresponding to the entries
// in ents to ids and returns the result.
func extractIDs(ids []kvserverbase.CmdIDKey, ents []raftpb.Entry) []kvserverbase.CmdIDKey {
	for _, e := range ents {
		typ, _, err := raftlog.EncodingOf(e)
		if err != nil {
			continue
		}
		switch typ {
		case raftlog.EntryEncodingStandardWithAC,
			raftlog.EntryEncodingSideloadedWithAC,
			raftlog.EntryEncodingStandardWithoutAC,
			raftlog.EntryEncodingSideloadedWithoutAC:
			id, _ := raftlog.DecomposeRaftEncodingStandardOrSideloaded(e.Data)
			ids = append(ids, id)
		case raftlog.EntryEncodingRaftConfChange, raftlog.EntryEncodingRaftConfChangeV2:
			// Configuration changes don't have the CmdIDKey easily accessible but are
			// rare, so fully decode the entry.
			ent, err := raftlog.NewEntry(e)
			if err != nil {
				continue
			}
			ids = append(ids, ent.ID)
		}
	}
	return ids
}

// traceProposals logs a trace event with the provided string for each proposed
// command which corresponds to an id in ids.
func traceProposals(r *Replica, ids []kvserverbase.CmdIDKey, event string) {
	ctxs := make([]context.Context, 0, len(ids))
	r.mu.RLock()
	for _, id := range ids {
		if prop, ok := r.mu.proposals[id]; ok {
			ctxs = append(ctxs, prop.Context())
		}
	}
	r.mu.RUnlock()
	for _, ctx := range ctxs {
		log.Eventf(ctx, "%v", event)
	}
}
