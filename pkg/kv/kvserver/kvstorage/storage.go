// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

// The following are type aliases that help annotating various storage
// interaction functions in this package and its clients, accordingly to the
// logical type of the storage engine being used.
//
// From the perspective of this package, the storage engine is logically
// separated into state machine engine and raft engine. The raft engine contains
// raft state: mainly HardState, raft log, and RaftTruncatedState.
//
// The state machine engine contains mainly the replicated state machine (RSM)
// of all ranges hosted by the store, as well as the supporting unreplicated
// metadata of these ranges, such as RaftReplicaID and RangeTombstone.
//
// TODO(pav-kv): as of now, these types are transparently convertible to one
// another and storage package interfaces. It is possible to mis-use them
// without compilation errors. This is fine while separated storage is not
// supported, but we want to make type-checking stronger eventually.
type (
	StateRO storage.Reader
	StateWO storage.Writer
	StateRW storage.ReadWriter
	RaftRO  storage.Reader
	RaftWO  storage.Writer
	RaftRW  storage.ReadWriter
)

// State gives read and write access to the state machine. Note that writes may
// or may not be visible to reads, depending on the user's semantics.
type State struct {
	RO StateRO
	WO StateWO
}

// Raft gives read and write access to the raft engine. Note that writes may or
// may not be visible to reads, depending on the user's semantics.
type Raft struct {
	RO RaftRO
	WO RaftWO
}

// ReadWriter gives read and write access to both engines. Note that writes may
// or may not be visible to reads, depending on the user's semantics.
type ReadWriter struct {
	State State
	Raft  Raft
}

// TODOState interprets the provided storage accessor as the State engine.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns,
// and switched to WrapState() or other explicitly defined engine types.
func TODOState(rw storage.ReadWriter) State {
	return State{RO: rw, WO: rw}
}

// WrapState interprets the provided storage accessor as the State engine.
func WrapState(rw StateRW) State {
	return State{RO: rw, WO: rw}
}

// WrapRaft interprets the provided storage accessor as the Raft engine.
func WrapRaft(rw RaftRW) Raft {
	return Raft{RO: rw, WO: rw}
}

// TODORaft interprets the provided storage accessor as the Raft engine.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODORaft(rw storage.ReadWriter) Raft {
	return Raft{RO: rw, WO: rw}
}

// TODOReaderWriter interprets the given reader and writer as if they are able
// to read and write both engines.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODOReaderWriter(r storage.Reader, w storage.Writer) ReadWriter {
	return ReadWriter{
		State: State{RO: r, WO: w},
		Raft:  Raft{RO: r, WO: w},
	}
}

// TODOReadWriter interprets the given accessor as reader and writer for both
// engines.
//
// TODO(pav-kv): remove when all callers have clarified their access patterns.
func TODOReadWriter(rw storage.ReadWriter) ReadWriter {
	return TODOReaderWriter(rw, rw)
}

// Engines contains the engines that support the operations of the Store. At the
// time of writing, all three fields will be populated with the same Engine. As
// work on separate raft log proceeds, we will be able to experimentally run
// with a separate log engine, and ultimately allow doing so in production
// deployments.
type Engines struct {
	// stateEngine is the state machine engine, in which the committed raft state
	// materializes after being "applied".
	stateEngine storage.Engine
	// todoEngine is a placeholder used in cases where:
	// - the code does not yet cleanly separate between state and log engine
	// - it is still unclear which of the two engines is the better choice for a
	//   particular write, or there is a candidate, but it needs to be verified.
	todoEngine storage.Engine
	// logEngine is the engine holding mainly the raft state, such as HardState
	// and logs, and the Store-local keys. This engine provides timely
	// durability, by frequent and on-demand syncing.
	logEngine storage.Engine
	// separated is true iff the engines are logically or physically separated.
	// Can be true only in tests.
	separated bool
}

// MakeEngines creates an Engines handle in which both state machine and log
// engine reside in the same physical engine.
func MakeEngines(eng storage.Engine) Engines {
	if spanset.EnableAssertions {
		// Wrap the engines with span set engines to catch incorrect engine
		// accesses.
		return Engines{
			stateEngine: spanset.NewEngine(eng, validateIsStateEngineSpan),
			logEngine:   spanset.NewEngine(eng, validateIsRaftEngineSpan),
			todoEngine:  eng,
		}
	}
	return Engines{
		stateEngine: eng,
		todoEngine:  eng,
		logEngine:   eng,
	}
}

// MakeSeparatedEnginesForTesting creates an Engines handle in which the state
// machine and log engines are logically (or physically) separated. To be used
// only in tests, until separated engines are correctly supported.
func MakeSeparatedEnginesForTesting(state, log storage.Engine) Engines {
	if !buildutil.CrdbTestBuild {
		panic("separated engines are not supported")
	}
	if spanset.EnableAssertions {
		// Wrap the engines with span set engines to catch incorrect engine
		// accesses.
		return Engines{
			stateEngine: spanset.NewEngine(state, validateIsStateEngineSpan),
			todoEngine:  nil,
			logEngine:   spanset.NewEngine(log, validateIsRaftEngineSpan),
			separated:   true,
		}
	}
	return Engines{
		stateEngine: state,
		todoEngine:  nil,
		logEngine:   log,
		separated:   true,
	}
}

// Engine returns the single engine. Used when the caller implements backwards
// compatible code and neither StateEngine nor LogEngine can be used. This is
// different from TODOEngine in that the caller explicitly acknowledges the fact
// that they are using a combined engine.
func (e *Engines) Engine() storage.Engine {
	if buildutil.CrdbTestBuild && e.separated {
		panic("engines are separated")
	}
	return e.todoEngine
}

// StateEngine returns the state machine engine.
func (e *Engines) StateEngine() storage.Engine {
	return e.stateEngine
}

// LogEngine returns the raft/log engine.
func (e *Engines) LogEngine() storage.Engine {
	return e.logEngine
}

// TODOEngine returns the combined engine, used in the code which currently does
// not support separated engines. The caller must eventually "resolve" this call
// to one of StateEngine, LogEngine, or Engine.
func (e *Engines) TODOEngine() storage.Engine {
	return e.todoEngine
}

// Separated returns true iff the engines are logically or physically separated.
// Can return true only in tests, until separated engines are supported.
func (e *Engines) Separated() bool {
	return e.separated
}

// Close closes the underlying engine(s).
func (e *Engines) Close() {
	if !e.separated {
		e.Engine().Close()
		return
	}
	e.stateEngine.Close()
	e.logEngine.Close()
}

// SetMinVersion signals both engines about the current minimum version that
// they must maintain compatibility with.
func (e *Engines) SetMinVersion(cv clusterversion.ClusterVersion) error {
	if err := e.StateEngine().SetMinVersion(cv.Version); err != nil {
		return errors.Wrapf(err, "error writing version to engine %s", e.StateEngine())
	} else if !e.Separated() {
		return nil // there is only one engine
	} else if err := e.LogEngine().SetMinVersion(cv.Version); err != nil {
		return errors.Wrapf(err, "error writing version to engine %s", e.LogEngine())
	}
	return nil
}

// SetStoreID informs the engines of the store ID, once it is known. Used to
// show the store ID in logs and to initialize the shared object creator ID (if
// shared object storage is configured).
func (e *Engines) SetStoreID(ctx context.Context, id roachpb.StoreID) error {
	if err := e.StateEngine().SetStoreID(ctx, int32(id)); err != nil {
		return errors.Wrapf(err, "error setting store ID on %s", e.StateEngine())
	} else if !e.Separated() {
		return nil // there is only one engine
	} else if err := e.LogEngine().SetStoreID(ctx, int32(id)); err != nil {
		return errors.Wrapf(err, "error setting store ID on engine %s", e.LogEngine())
	}
	return nil
}

// NewWriteBatch creates a new write batch to storage. If engines are separated,
// it consists of two batches, one per engine.
// TODO(sep-raft-log): generalize this so that the LogEngine batch is lazy.
func (e *Engines) NewWriteBatch() WriteBatch {
	if e.Separated() {
		return WriteBatch{
			state:     e.StateEngine().NewWriteBatch(),
			raft:      e.LogEngine().NewWriteBatch(),
			separated: true,
		}
	}
	// With a single engine, create one batch, and reference it by both pointers.
	b := e.Engine().NewWriteBatch()
	if !spanset.EnableAssertions {
		return WriteBatch{state: b, raft: b}
	}
	// Additionally, if assertions are enabled, enclose the batch into the
	// corresponding per-engine wrappers. With EnableAssertions, State/LogEngine
	// are always wrapped, so we don't use conditional type assertions.
	type wrapper interface {
		WrapWriteBatch(wb storage.WriteBatch) storage.WriteBatch
	}
	return WriteBatch{
		state: e.StateEngine().(wrapper).WrapWriteBatch(b),
		raft:  e.LogEngine().(wrapper).WrapWriteBatch(b),
	}
}

// WriteBatch is a write batch to storage which is aware whether the log and
// state machine engines are separated.
type WriteBatch struct {
	state     storage.WriteBatch
	raft      storage.WriteBatch
	separated bool
}

// Writers returns the state machine and raft engine writer into this batch.
func (b *WriteBatch) Writers() (StateWO, RaftWO) {
	return b.state, b.raft
}

// CommitAndSync commits and syncs the batch to storage. When engines are
// separated, only the LogEngine batch is synced, and the StateEngine part is
// expected to be replayable from the LogEngine batch.
func (b *WriteBatch) CommitAndSync() error {
	if !b.separated {
		return b.state.Commit(true /* sync */)
	}
	if err := b.raft.Commit(true /* sync */); err != nil {
		return err
	}
	return b.state.Commit(false /* false */)
}

// Close closes the batch.
func (b *WriteBatch) Close() {
	b.state.Close()
	if b.separated {
		b.raft.Close()
	}
}

// validateIsStateEngineSpan asserts that the provided span only overlaps with
// keys in the State engine and returns an error if not.
// Note that we could receive the span with a nil startKey, which has a special
// meaning that the span represents: [endKey.Prev(), endKey).
func validateIsStateEngineSpan(span spanset.TrickySpan) error {
	// If the provided span overlaps with local store span, it cannot be a
	// StateEngine span because Store-local keys belong to the LogEngine.
	if spanset.Overlaps(roachpb.Span{
		Key:    keys.LocalStorePrefix,
		EndKey: keys.LocalStoreMax,
	}, span) {
		return errors.Errorf("overlaps with store local keys")
	}

	// If the provided span is completely outside the rangeID local spans for any
	// rangeID, then there is no overlap with any rangeID local keys.
	fullRangeIDLocalSpans := roachpb.Span{
		Key:    keys.LocalRangeIDPrefix.AsRawKey(),
		EndKey: keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd(),
	}
	if !spanset.Overlaps(fullRangeIDLocalSpans, span) {
		return nil
	}

	// At this point, we know that we overlap with fullRangeIDLocalSpans. If we
	// are not completely within fullRangeIDLocalSpans, return an error as we
	// make an assumption that spans should respect the local RangeID tree
	// structure, and that spans that partially overlaps with
	// fullRangeIDLocalSpans don't make logical sense.
	if !spanset.Contains(fullRangeIDLocalSpans, span) {
		return errors.Errorf("overlapping an unreplicated rangeID key")
	}

	// If the span in inside fullRangeIDLocalSpans, we expect that both start and
	// end keys should be in the same rangeID.
	rangeIDKey := span.Key
	if rangeIDKey == nil {
		rangeIDKey = span.EndKey
	}
	rangeID, err := keys.DecodeRangeIDPrefix(rangeIDKey)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			"could not decode range ID for span: %s", span)
	}

	// If the span is inside RangeIDLocalSpans but outside RangeIDUnreplicated,
	// it cannot overlap local raft keys.
	rangeIDPrefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	if !spanset.Overlaps(roachpb.Span{
		Key:    rangeIDPrefixBuf.UnreplicatedPrefix(),
		EndKey: rangeIDPrefixBuf.UnreplicatedPrefix().PrefixEnd(),
	}, span) {
		return nil
	}

	// RangeTombstoneKey and RaftReplicaIDKey belong to the StateEngine, and can
	// be accessed as point keys.
	if roachpb.Span(span).Equal(roachpb.Span{
		Key: rangeIDPrefixBuf.RangeTombstoneKey(),
	}) {
		return nil
	}

	if roachpb.Span(span).Equal(roachpb.Span{
		Key: rangeIDPrefixBuf.RaftReplicaIDKey(),
	}) {
		return nil
	}

	return errors.Errorf("overlapping an unreplicated rangeID span")
}

// validateIsRaftEngineSpan asserts that the provided span only overlaps with
// keys in the Raft engine and returns an error if not.
// Note that we could receive the span with a nil startKey, which has a special
// meaning that the span represents: [endKey.Prev(), endKey).
func validateIsRaftEngineSpan(span spanset.TrickySpan) error {
	// The LogEngine owns only Store-local and RangeID-local raft keys. A span
	// inside Store-local is correct. If it's only partially inside, an error is
	// returned below, as part of checking RangeID-local spans.
	if spanset.Contains(roachpb.Span{
		Key:    keys.LocalStorePrefix,
		EndKey: keys.LocalStoreMax,
	}, span) {
		return nil
	}

	// At this point, the remaining possible LogEngine keys are inside
	// LocalRangeID spans. If the span is not completely inside it, it must
	// overlap with some StateEngine keys.
	if !spanset.Contains(roachpb.Span{
		Key:    keys.LocalRangeIDPrefix.AsRawKey(),
		EndKey: keys.LocalRangeIDPrefix.AsRawKey().PrefixEnd(),
	}, span) {
		return errors.Errorf("overlaps with state engine keys")
	}

	// If the span in inside LocalRangeID, we assume that both start and
	// end keys should be in the same rangeID.
	rangeIDKey := span.Key
	if rangeIDKey == nil {
		rangeIDKey = span.EndKey
	}
	rangeID, err := keys.DecodeRangeIDPrefix(rangeIDKey)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			"could not decode range ID for span: %s", span)
	}
	rangeIDPrefixBuf := keys.MakeRangeIDPrefixBuf(rangeID)
	if !spanset.Contains(roachpb.Span{
		Key:    rangeIDPrefixBuf.UnreplicatedPrefix(),
		EndKey: rangeIDPrefixBuf.UnreplicatedPrefix().PrefixEnd(),
	}, span) {
		return errors.Errorf("overlaps with state engine keys")
	}
	if spanset.Overlaps(roachpb.Span{Key: rangeIDPrefixBuf.RangeTombstoneKey()}, span) {
		return errors.Errorf("overlaps with state engine keys")
	}
	if spanset.Overlaps(roachpb.Span{Key: rangeIDPrefixBuf.RaftReplicaIDKey()}, span) {
		return errors.Errorf("overlaps with state engine keys")
	}

	return nil
}
