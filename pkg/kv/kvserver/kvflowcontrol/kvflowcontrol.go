// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvflowcontrol provides flow control for replication traffic in KV.
// It's part of the integration layer between KV and admission control.
package kvflowcontrol

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/redact"
)

// Enabled determines whether we use flow control for replication traffic in KV.
//
// TODO(sumeer): changing this to false does not affect requests that are
// already waiting for tokens for eval in RACv1. Consider fixing and
// back-porting.
var Enabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kvadmission.flow_control.enabled",
	"determines whether we use flow control for replication traffic in KV",
	true,
)

// Mode determines the 'mode' of flow control we use for replication traffic in
// KV, if enabled.
var Mode = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"kvadmission.flow_control.mode",
	"determines the 'mode' of flow control we use for replication traffic in KV, if enabled",
	metamorphic.ConstantWithTestChoice(
		"kvadmission.flow_control.mode",
		modeDict[ApplyToAll],     /* default value */
		modeDict[ApplyToElastic], /* other value */
	),
	modeDict,
)

var modeDict = map[ModeT]string{
	ApplyToElastic: "apply_to_elastic",
	ApplyToAll:     "apply_to_all",
}

// ModeT represents the various modes of flow control for replication traffic.
type ModeT int64

const (
	// ApplyToElastic uses flow control for only elastic traffic, i.e. only
	// elastic work will wait for flow tokens to be available. All work is
	// virtually enqueued in below-raft admission queues and dequeued in
	// priority order, but only empty elastic flow token buckets above-raft will
	// block further elastic traffic from being admitted.
	ApplyToElastic ModeT = iota
	// ApplyToAll uses flow control for both elastic and regular traffic,
	// i.e. all work will wait for flow tokens to be available.
	ApplyToAll
)

func (m ModeT) String() string {
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m ModeT) SafeFormat(p redact.SafePrinter, _ rune) {
	if s, ok := modeDict[m]; ok {
		p.SafeString(redact.SafeString(s))
		return
	}
	p.SafeString("unknown-mode")
}

// RegularTokensPerStream determines the flow tokens available for regular work
// on a per-stream basis.
var RegularTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.regular_tokens_per_stream",
	"flow tokens available for regular work on a per-stream basis",
	16<<20, // 16 MiB
	validateTokenRange,
)

// ElasticTokensPerStream determines the flow tokens available for elastic work
// on a per-stream basis.
var ElasticTokensPerStream = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.elastic_tokens_per_stream",
	"flow tokens available for elastic work on a per-stream basis",
	8<<20, // 8 MiB
	validateTokenRange,
)

const (
	minTokensPerStream Tokens = 1 << 20  // 1 MiB
	maxTokensPerStream Tokens = 64 << 20 // 64 MiB
)

var validateTokenRange = settings.WithValidateInt(func(b int64) error {
	t := Tokens(b)
	if t < minTokensPerStream {
		return fmt.Errorf("minimum flowed tokens allowed is %s, got %s", minTokensPerStream, t)
	}
	if t > maxTokensPerStream {
		return fmt.Errorf("maximum flow tokens allowed is %s, got %s", maxTokensPerStream, t)
	}
	return nil
})

// TokenCounterResetEpoch is an escape hatch for administrators that should
// never be needed. By incrementing this epoch (or changing it to a value
// different than before), an administrator can restore all RACv2 token
// counters to their default (full) state. This can be used to counteract a
// token leakage bug, but note that if there is indeed a bug, the leakage may
// resume, and tokens may again be exhausted. So it is expected that this will
// be used together with disabling replication admission control by setting
// kvadmission.flow_control.enabled=false. Note that disabling replication
// admission control should be sufficient, since it should unblock work that
// is waiting-for-eval. But in case there is another bug that is preventing
// such work from unblocking, this setting may be useful.
var TokenCounterResetEpoch = settings.RegisterIntSetting(
	settings.SystemOnly,
	"kvadmission.flow_controller.token_reset_epoch",
	"escape hatch for administrators to reset all token counters to their default (full) state",
	0)

// V2EnabledWhenLeaderLevel captures the level at which RACv2 is enabled when
// this replica is the leader.
//
// State transitions are V2NotEnabledWhenLeader =>
// V2EnabledWhenLeaderV1Encoding => V2EnabledWhenLeaderV2Encoding, i.e., the
// level will never regress.
type V2EnabledWhenLeaderLevel = uint32

const (
	V2NotEnabledWhenLeader V2EnabledWhenLeaderLevel = iota
	V2EnabledWhenLeaderV1Encoding
	V2EnabledWhenLeaderV2Encoding
)

// GetV2EnabledWhenLeaderLevel returns the level at which RACV2 is enabled when
// this replica is the leader.
//
// The level is determined by the cluster version, and is ratcheted up as the
// cluster version advances. The level is used to determine:
//
//  1. Whether the leader should use the RACv2 protocol.
//  2. Whether the leader should use the V1 or V2 entry encoding iff (1) is
//     true.
//
// Upon the leader first seeing V24_3_UseRACV2WithV1EntryEncoding, it will
// create a RangeController and use the V1 entry encoding, operating in Push
// mode. Upon the leader first seeing V24_3_UseRACV2Full, it will continue
// using the RACV2 protocol, but will switch to the V2 entry encoding. Note the
// necessary migration for V2NotEnabledWhenLeader =>
// V2EnabledWhenLeaderV1Encoding occurs before anything else in
// kvserver.handleRaftReadyRaftMuLocked.
func GetV2EnabledWhenLeaderLevel(
	ctx context.Context, st *cluster.Settings, knobs *TestingKnobs,
) V2EnabledWhenLeaderLevel {
	if knobs != nil && knobs.OverrideV2EnabledWhenLeaderLevel != nil {
		return knobs.OverrideV2EnabledWhenLeaderLevel()
	}
	// Full RACv2 can be enabled: RACv2 protocol with V2 entry encoding.
	return V2EnabledWhenLeaderV2Encoding
}

// Stream models the stream over which we replicate data traffic, the
// transmission for which we regulate using flow control. It's segmented by the
// specific store the traffic is bound for and the tenant driving it. Despite
// the underlying link/transport being shared across tenants, modeling streams
// on a per-tenant basis helps provide inter-tenant isolation.
type Stream struct {
	TenantID roachpb.TenantID
	StoreID  roachpb.StoreID
}

// ConnectedStream models a stream over which we're actively replicating data
// traffic. The embedded channel is signaled when the stream is disconnected,
// for example when (i) the remote node has crashed, (ii) bidirectional gRPC
// streams break, (iii) we've paused replication traffic to it, (iv) truncated
// our raft log ahead it, and more. Whenever that happens, we unblock inflight
// requests waiting for flow tokens.
type ConnectedStream interface {
	Stream() Stream
	Disconnected() <-chan struct{}
}

// Tokens represent the finite capacity of a given stream, expressed in bytes
// for data we're looking to replicate. Use of replication streams are
// predicated on tokens being available.
//
// NB: We use a signed integer to accommodate data structures that deal with
// token deltas, or buckets that are allowed to go into debt.
type Tokens int64

// Controller provides flow control for replication traffic in KV, held at the
// node-level.
type Controller interface {
	InspectController
	// Admit seeks admission to replicate data, regardless of size, for work with
	// the given priority, create-time, and over the given stream. This blocks
	// until there are flow tokens available or the stream disconnects, subject to
	// context cancellation. This returns true if the request was admitted through
	// flow control. Ignore the first return type if err != nil. admitted ==
	// false && err == nil is a valid return, when something (e.g.
	// configuration) caused the callee to not care whether flow tokens were
	// available.
	Admit(context.Context, admissionpb.WorkPriority, time.Time, ConnectedStream) (admitted bool, _ error)
	// DeductTokens deducts (without blocking) flow tokens for replicating work
	// with given priority over the given stream. Requests are expected to
	// have been Admit()-ed first.
	DeductTokens(context.Context, admissionpb.WorkPriority, Tokens, Stream)
	// ReturnTokens returns flow tokens for the given stream. These tokens are
	// expected to have been deducted earlier with the same priority provided
	// here.
	ReturnTokens(context.Context, admissionpb.WorkPriority, Tokens, Stream)
	// InspectStream returns a snapshot of a specific underlying stream and its
	// available {regular,elastic} tokens. It's used to power /inspectz.
	InspectStream(context.Context, Stream) kvflowinspectpb.Stream

	// TODO(irfansharif): We might need the ability to "disable" specific
	// streams/corresponding token buckets when there are failures or
	// replication to a specific store is paused due to follower-pausing.
	// That'll have to show up between the Handler and the Controller somehow.
	// See I2, I3a and [^7] in kvflowcontrol/doc.go.
}

type InspectController interface {
	// Inspect returns a snapshot of all underlying streams and their available
	// {regular,elastic} tokens. It's used to power /inspectz.
	Inspect(context.Context) []kvflowinspectpb.Stream
}

// ReplicationAdmissionHandle abstracts waiting for admission across RACv1 and RACv2.
type ReplicationAdmissionHandle interface {
	// Admit seeks admission to replicate data, regardless of size, for work
	// with the given priority and create-time. This blocks until there are flow
	// tokens available for connected streams. This returns true if the request
	// was admitted through flow control. Ignore the first return type if err !=
	// nil. admitted == false && err == nil is a valid return, when something
	// caused the callee to not care whether flow tokens were available. This
	// can happen for at least the following reasons:
	// - Configuration specifies the given WorkPriority is not subject to
	//   replication AC.
	// - The callee doesn't think it is the leader or has been closed/destroyed.
	//
	// The latter can happen in the midst of a transition from RACv1 => RACv2.
	// In this case if the callee waited on at least one connectedStream and was
	// admitted, it will return (true, nil). This includes the case where the
	// connectedStream was closed while waiting. If there were no
	// connectedStreams (because they were already closed) it will return
	// (false, nil).
	Admit(context.Context, admissionpb.WorkPriority, time.Time) (admitted bool, _ error)
}

// Handle is used to interface with replication flow control; it's typically
// backed by a node-level kvflowcontrol.Controller. Handles are held on replicas
// initiating replication traffic, i.e. are both the leaseholder and raft
// leader, and manage multiple streams underneath (typically one per active
// member of the raft group).
//
// When replicating log entries, these replicas choose the log position
// (term+index) the data is to end up at, and use this handle to track the token
// deductions on a per log position basis. When informed of admitted log entries
// on the receiving end of the stream, we free up tokens by specifying the
// highest log position up to which we've admitted (below-raft admission, for a
// given priority, takes log position into account -- see
// kvflowcontrolpb.AdmittedRaftLogEntries for more details).
type Handle interface {
	ReplicationAdmissionHandle
	InspectHandle
	// DeductTokensFor deducts (without blocking) flow tokens for replicating
	// work with given priority along connected streams. The deduction is
	// tracked with respect to the specific raft log position it's expecting it
	// to end up in, log positions that monotonically increase. Requests are
	// assumed to have been Admit()-ed first.
	DeductTokensFor(
		context.Context, admissionpb.WorkPriority,
		kvflowcontrolpb.RaftLogPosition, Tokens,
	)
	// ReturnTokensUpto returns all previously deducted tokens of a given
	// priority for all log positions less than or equal to the one specified.
	// It does for the specific stream. Once returned, subsequent attempts to
	// return tokens upto the same position or lower are no-ops. It's used when
	// entries at specific log positions have been admitted below-raft.
	//
	// NB: Another use is during successive lease changes (out and back) within
	// the same raft term -- we want to both free up tokens from when we lost
	// the lease, and also ensure we discard attempts to return them (on hearing
	// about AdmittedRaftLogEntries replicated under the earlier lease).
	ReturnTokensUpto(
		context.Context, admissionpb.WorkPriority,
		kvflowcontrolpb.RaftLogPosition, Stream,
	)
	// ConnectStream connects a stream (typically pointing to an active member
	// of the raft group) to the handle. Subsequent calls to Admit() will block
	// until flow tokens are available for the stream, or for it to be
	// disconnected via DisconnectStream. DeductTokensFor will also deduct
	// tokens for all connected streams. The log position is used as a lower
	// bound, beneath which all token deductions/returns are rendered no-ops.
	ConnectStream(context.Context, kvflowcontrolpb.RaftLogPosition, Stream)
	// DisconnectStream disconnects a stream from the handle. When disconnecting
	// a stream, (a) all previously held flow tokens are released and (b) we
	// unblock all requests waiting in Admit() for this stream's flow tokens in
	// particular. It's a no-op if disconnecting something we're not connected
	// to.
	//
	// This is typically used when we're no longer replicating data to a member
	// of the raft group, because (a) it crashed, (b) it's no longer part of the
	// raft group, (c) we've decided to pause it, (d) we've truncated the raft
	// log ahead of it and expect it to be caught up via snapshot, and more. In
	// all these cases we don't expect dispatches for individual
	// AdmittedRaftLogEntries between what it admitted last and its latest
	// RaftLogPosition.
	DisconnectStream(context.Context, Stream)
	// ResetStreams resets all connected streams, i.e. it disconnects and
	// re-connects to each one. It effectively unblocks all requests waiting in
	// Admit(). It's only used when cluster settings change, settings that
	// affect all work waiting for flow tokens.
	ResetStreams(ctx context.Context)
	// Close closes the handle and returns all held tokens back to the
	// underlying controller. Typically used when the replica loses its lease
	// and/or raft leadership, or ends up getting GC-ed (if it's being
	// rebalanced, merged away, etc).
	Close(context.Context)
}

type InspectHandle interface {
	// Inspect returns a serialized form of the underlying handle. It's used to
	// power /inspectz.
	Inspect(context.Context) kvflowinspectpb.Handle
}

// Handles represent a set of flow control handles. Note that handles are
// typically held on replicas initiating replication traffic, so on a given node
// they're uniquely identified by their range ID.
type Handles interface {
	InspectHandles
	// Lookup the kvflowcontrol.Handle for the specific range (or rather, the
	// replica of the specific range that's locally held).
	Lookup(roachpb.RangeID) (Handle, bool)
	// ResetStreams resets all underlying streams for all underlying
	// kvflowcontrol.Handles, i.e. disconnect and reconnect each one. It
	// effectively unblocks all requests waiting in Admit().
	ResetStreams(ctx context.Context)
	// TODO(irfansharif): When fixing I1 and I2 from kvflowcontrol/node.go,
	// we'll want to disconnect all streams for a specific node. Expose
	// something like the following to disconnect all replication streams bound
	// to the specific node. Back it by a reverse-lookup dictionary, keyed by
	// StoreID (or NodeID, if we also maintain a mapping between NodeID ->
	// []StoreID) and the set of Handles currently connected to it. Do it as
	// part of #95563.
	//
	//   Iterate(roachpb.StoreID, func(context.Context, Handle, Stream))

	// LookupReplicationAdmissionHandle looks up the ReplicationAdmissionHandle
	// for the specific range (or rather, the replica of the specific range
	// that's locally held). The bool is false if no handle was found, in which
	// case the caller must use the pre-replication-admission-control path.
	LookupReplicationAdmissionHandle(roachpb.RangeID) (ReplicationAdmissionHandle, bool)
}

type InspectHandles interface {
	// LookupInspect the serialized form of a handle for the specific range (or
	// rather, the replica of the specific range that's locally held).
	LookupInspect(roachpb.RangeID) (kvflowinspectpb.Handle, bool)
	// Inspect returns the set of ranges that have an embedded
	// kvflowcontrol.Handle. It's used to power /inspectz.
	Inspect() []roachpb.RangeID
}

// HandleFactory is used to construct new Handles.
type HandleFactory interface {
	NewHandle(roachpb.RangeID, roachpb.TenantID) Handle
}

// Dispatch is used (i) to dispatch information about admitted raft log entries
// to specific nodes, and (ii) to read pending dispatches.
type Dispatch interface {
	DispatchWriter
	DispatchReader
}

// DispatchWriter is used to dispatch information about admitted raft log
// entries to specific nodes (typically where said entries originated, where
// flow tokens were deducted and waiting to be returned).
type DispatchWriter interface {
	Dispatch(context.Context, roachpb.NodeID, kvflowcontrolpb.AdmittedRaftLogEntries)
}

// DispatchReader is used to read pending dispatches. It's used in the raft
// transport layer when looking to piggyback information on traffic already
// bound to specific nodes. It's also used when timely dispatching (read:
// piggybacking) has not taken place.
//
// NB: PendingDispatchFor is expected to remove dispatches from the pending
// list. If the gRPC stream we're sending it over happens to break, we drop
// these dispatches. The node waiting these dispatches is expected to react to
// the stream breaking by freeing up all held tokens.
type DispatchReader interface {
	PendingDispatch() []roachpb.NodeID
	PendingDispatchFor(nodeID roachpb.NodeID, maxBytes int64) (admittedRaftLogEntries []kvflowcontrolpb.AdmittedRaftLogEntries, remainingDispatches int)
}

func (t Tokens) String() string {
	return redact.StringWithoutMarkers(t)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (t Tokens) SafeFormat(p redact.SafePrinter, _ rune) {
	if t < 0 {
		p.SafeString(humanizeutil.IBytes(int64(t)))
		return
	}
	p.Printf("+%s", humanizeutil.IBytes(int64(t)))
}

func (s Stream) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s Stream) SafeFormat(p redact.SafePrinter, _ rune) {
	tenantSt := s.TenantID.String()
	if s.TenantID.IsSystem() {
		tenantSt = "1"
	}
	p.Printf("t%s/s%s", redact.SafeString(tenantSt), s.StoreID)
}

var raftAdmissionMetaKey = ctxutil.RegisterFastValueKey()

// ContextWithMeta returns a Context wrapping the supplied raft admission meta,
// if any.
//
// TODO(irfansharif,aaditya): This causes a heap allocation. Revisit as part of
// #104154.
func ContextWithMeta(ctx context.Context, meta *kvflowcontrolpb.RaftAdmissionMeta) context.Context {
	if meta != nil {
		ctx = ctxutil.WithFastValue(ctx, raftAdmissionMetaKey, meta)
	}
	return ctx
}

// MetaFromContext returns the raft admission meta embedded in the Context, if
// any.
func MetaFromContext(ctx context.Context) *kvflowcontrolpb.RaftAdmissionMeta {
	val := ctxutil.FastValue(ctx, raftAdmissionMetaKey)
	h, ok := val.(*kvflowcontrolpb.RaftAdmissionMeta)
	if !ok {
		return nil
	}
	return h
}
