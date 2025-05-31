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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/redact"
)

// Enabled determines whether we use flow control for replication traffic in KV.
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

// Stream models the stream over which we replicate data traffic, the
// transmission for which we regulate using flow control. It's segmented by the
// specific store the traffic is bound for and the tenant driving it. Despite
// the underlying link/transport being shared across tenants, modeling streams
// on a per-tenant basis helps provide inter-tenant isolation.
type Stream struct {
	TenantID roachpb.TenantID
	StoreID  roachpb.StoreID
}

// Tokens represent the finite capacity of a given stream, expressed in bytes
// for data we're looking to replicate. Use of replication streams are
// predicated on tokens being available.
//
// NB: We use a signed integer to accommodate data structures that deal with
// token deltas, or buckets that are allowed to go into debt.
type Tokens int64

type InspectController interface {
	// Inspect returns a snapshot of all underlying streams and their available
	// {regular,elastic} tokens. It's used to power /inspectz.
	Inspect(context.Context) []kvflowinspectpb.Stream
}

// ReplicationAdmissionHandle abstracts waiting for admission.
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
	Admit(context.Context, admissionpb.WorkPriority, time.Time) (admitted bool, _ error)
}

// ReplicationAdmissionHandles is used to look up a
// ReplicationAdmissionHandle.
type ReplicationAdmissionHandles interface {
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
