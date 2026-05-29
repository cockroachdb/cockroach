// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// A range-shared engine is shared because of a shared manifest and shared
// objects/files for what the manifest references.
//
// Shared Manifest basic distributed lifecycle (failures possible at every
// point):
//
// written-to-basalt-path-for-local-store-rangeid-replicaid =>
// hardlinked-to-basalt-path-for-all-replicas => proposed-via-raft =>
// applied-to-current-pointer => current-pointer-application-durable.
//
// The first step is the responsibility of the RSEngine (delegated to the
// InnerRSEngine), and the rest are the responsibility of the higher layer
// (kvserver).
//
// The current-pointer is in the store-local engine.
//
// Manifest lifecycle from the perspective of a RSEngine:
//
// The RSEngine at the leaseholder sees the most action. But it doesn't know or
// care about leases. All it knows that the capability to install a new manifest
// has been externalized to a different entity. But care has to be taken to
// ensure that manifest updates are serialized, in that the next manifest being
// proposed is an immediate successor of a particular manifest, and the next
// manifest is only installed if the immediate successor relationship is
// maintained.
//
// This predecessor relationship is conveyed to the external layer in the
// parameter to ManifestChangeCommitter.InstallNewManifest, and is available to
// the external layer when it calls split or merge on a RSEngineSnapshot.
//
// Causes of manifest changes:
//
// Compactions: the range shared engine can have many concurrent compactions
// ongoing that don't conflict with each other, and an ongoing range flush.
//
// For a compaction, Pebble internally writes output files to a scratch
// directory with temporary file numbers. When the compaction completes, Pebble
// calls ManifestChangeCommitter.GetFileNums (via the FileNumAllocator callback)
// to allocate raft-coordinated file numbers, and remaps and hardlinks the
// output files to BasaltDir. Pebble then calls InstallNewManifest.
//
// Range flush or add-sstables: the files are written to BasaltScratchPathDir by
// the caller of RSEngine.{FlushSSTables,AddSSTables}. The flush/add-sstables
// calls pebble.DB.Ingest, which calls
// ManifestChangeCommitter.{GetFileNums,InstallNewManifest}.
//
// Split, Merge: kvserver drives these operations, which are delegated to
// Pebble, which creates the new manifest(s) and hardlinks files in the
// appropriate directory(s). Pebble calls GetFileNums during this process to
// allocate file numbers for the new manifest(s) and any renumbered files.
// Unlike compactions and flushes, Pebble does not call InstallNewManifest.
// Instead, kvserver coordinates the cross-replica installation itself
// (hardlinking to other replicas, proposing via Raft, updating the
// current-pointer).

// ManifestChangeCommitter is the interface provided to the range shared engine
// for manifest changes. The committer can return an error for various reasons,
// and the caller needs to handle errors gracefully by aborting the operation
// (compaction, range-flush, add-sstables etc.).
type ManifestChangeCommitter interface {
	// GetFileNums is used to fetch filenums for compactions, ingest, split,
	// merge. Pebble must make exactly one such call for each operation and use
	// the highest filenum(s) for the new manifest, to ensure the invariant that
	// all new files referenced by a manifest have numbers lower than it, and that
	// the history of manifest nums and their newly referenced files are
	// non-overlapping in terms of file numbers. We expect Pebble to make these
	// calls in the order in which Pebble expects the new manifests to be
	// installed. Safety does not rely on the ordering, since the
	// ManifestChangeCommitter ensures monotonically increasing manifest nums, but
	// we want to avoid wasted work.
	GetFileNums(count int) ([]DiskFileNum, error)
	// InstallNewManifest is called by Pebble after it has:
	// 1. Hardlinked new files to its own directory.
	// 2. Created the manifest in its own directory.
	//
	// The ManifestChangeCommitter:
	// 1. Hardlinks files to all OTHER replicas: thisStore/dir/name → otherStore/dir/name
	// 2. Raft-commits the manifest change
	// 3. Application of this change updates the current-pointer in the state machine
	//
	// The currentManifestNum is the manifest number being replaced, passed
	// explicitly by the RSEngine. The manifestInfo.Files contains the new files
	// (basenames) that need to be hardlinked to other replicas.
	//
	// Used for compactions, flush, add-sstable. Split/merge installation for
	// other replicas does not originate in Pebble, hence does not use
	// InstallNewManifest.
	//
	// ingestHandle is non-nil for flush commits and carries *FlushCommitInfo.
	InstallNewManifest(
		currentManifestNum DiskFileNum, manifestInfo ManifestInfo, ingestHandle interface{}) error
}

// FlushCommitInfo carries flush-specific metadata from kvserver through Pebble
// and back. It is a parameter to FlushSSTables, and Pebble threads it through
// as the ingestHandle parameter to InstallNewManifest, and kvserver's
// ManifestChangeCommitter uses it for safety validation and bookkeeping. The
// contents are opaque to Pebble.
type FlushCommitInfo struct {
	// ExpectedFlushGeneration is used to prevent flush commit when two flushes
	// have started concurrently (can happen due to lease transfers).
	ExpectedFlushGeneration roachpb.FlushGeneration
	// ActivateSpans are the key spans over which Writer.ClearRawRangeActivate is
	// written, activating dormant deletions from flush prepare. Currently, only
	// the user key span.
	//
	// TODO(sumeer): add Writer.ClearRawRangeActivate.
	ActivateSpans []roachpb.Span
	// FlushedApproxStoreLocalBytes is the ApproxStoreLocalBytes value captured at
	// flush prepare time. Subtracted from the range's ApproxStoreLocalBytes on
	// flush commit.
	FlushedApproxStoreLocalBytes int64
}

// FileNameAndNum is the name and number of a file. Currently, these
// are manifest files, sstables or blob files.
type FileNameAndNum struct {
	// Name is the basename of the file, without any directory components.
	Name string
	Num  DiskFileNum
}

// ManifestInfo contains information about a manifest.
type ManifestInfo struct {
	// Manifest is the manifest file info.
	Manifest FileNameAndNum
	// Files is the files referenced by the manifest. Depending on the context,
	// this may only be the delta since the previous manifest, or all the
	// referenced files.
	Files []FileNameAndNum
}

// RSEngine is the interface for a range-shared engine (rsEngineContainer is the
// real implementation).
//
// A RSEngine is opened once per Replica (when Basalt is configured) and closed
// when the Replica is destroyed. If a client has any method call that could
// race with Close, it must ensure it calls Ref before the Close, then calls the
// method, and then Unref, to ensure that Close waits for it. This behavior is
// unnecessary for RSEngineSnapshots, which Close inherently waits for.
// Additionally, PrepareExternalManifest and InstallPreparedManifest must
// precede Close since all three are considered lifecycle methods.
//
// Internally, it is a container that manages one
// active underlying innerRSEngine and tracks quiesced engines from past
// manifest changes. Each innerRSEngine wraps a pebble.DB.
//
// innerRSEngine Lifecycle:
//
// Multiple innerRSEngines can co-exist for the same range, since a new
// innerRSEngine *may* need to be opened when a new manifest is installed, while
// older innerRSEngines can be concurrently serving reads. All innerRSEngines
// must have returned from quiesce before opening a new innerRSEngine. Quiescing
// delegates to the underlying pebble.DB.
//
// A quiesced innerRSEngine will soon have Close called on it. Since there may
// be concurrent CockroachDB operations with RSEngineSnapshots using that
// innerRSEngine, it is the responsibility of the innerRSEngine to ensure that
// all RSEngineSnapshots are closed before calling pebble.DB.Close. DB.Close can
// further wait for operations it has started to complete.
//
// In the common case, we can transition from one manifest to another without
// quiescing and closing. This is attempted using the
// innerRSEngine.prepareExternalManifest and installPreparedManifest pair.
//
// Whether the RSEngine is quiescing innerRSEngines and opening ones or doing
// the fast path transition using a single innerRSEngine is hidden from the user
// of RSEngine which uniformly calls PrepareExternalManifest and
// InstallPreparedManifest.
//
// Since there can be quiesced pebble.DBs that have not completed Close, the
// current active DB cannot start cleaning up old unreferenced files. It is safe
// to do this when there are zero quiesced DBs. To illustrate: if there are
// quiesced DBs using manifest-nums 10 and 25, and the DB with manifest-num 10
// finishes Close, we cannot yet start cleaning up filenums <= 10, since they
// may be referenced by manifest-num 25. Since the active DB has limited
// history, we simply wait until there are zero quiesced DBs. There is a risk
// here that with very frequent manifest installs that we keep transitioning to
// new active DBs and there are always non-zero quiesced DBs. We accept this
// risk since most manifest transitions will be able to use
// innerRSEngine.{prepareExternalManifest,installPreparedManifest} and won't
// result in quiesced DBs.
type RSEngine interface {
	// CompactionToggle is called to enable or disable compactions. Compactions
	// are enabled only at the range leaseholder. The higher layer also disables
	// compactions during split and merge operations. This is best-effort, to
	// avoid wasted work and should not be relied on for safety.
	CompactionToggle(enable bool)
	// WaitForOngoingManifestChanges blocks until ongoing FlushSSTables,
	// AddSSTables, and compaction InstallNewManifest calls complete. Called after
	// CompactionToggle(false) to drain pending work before Split/Merge. This is
	// best-effort, to avoid wasted work and should not be relied on for safety.
	WaitForOngoingManifestChanges()
	// CurrentManifestNum returns the DiskFileNum of the current manifest.
	CurrentManifestNum() DiskFileNum
	// FlushSSTables accepts non-overlapping sstables that do not contain
	// multiple key-value pairs for the same userkey. The callee assigns a single
	// seqnum to each sstable. The scratchNames are basenames in the scratch
	// directory.
	FlushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error
	// AddSSTables accepts non-overlapping sstables that do not contain multiple
	// key-value pairs for the same userkey. This is needed as a fast-path for
	// index backfills to skip ingesting into the store-local engine. The callee
	// assigns a single seqnum to each sstable. The scratchNames are basenames in
	// the scratch directory.
	AddSSTables(scratchNames []string) error
	// Ref increments the container's external reference count. Close blocks
	// until all Ref/Unref pairs complete. Callers that hold a reference to
	// the RSEngine and want to prevent it from being closed (e.g. during
	// split/merge operations) should call Ref.
	Ref()
	// Unref decrements the container's external reference count. Must be
	// paired with a prior Ref call.
	Unref()
	// NewSnapshot creates a new RSEngineSnapshot at the current manifest of
	// the active engine.
	NewSnapshot() RSEngineSnapshot
	// PrepareExternalManifest prepares for a manifest transition to manifestNum.
	// The caller guarantees that manifestNum immediately succeeds
	// CurrentManifestNum and that there isn't an outstanding prepared manifest
	// that was not installed.
	PrepareExternalManifest(manifestNum DiskFileNum) error
	// InstallPreparedManifest installs a prepared manifest. After this call,
	// CurrentManifestNum() returns manifestNum.
	InstallPreparedManifest(manifestNum DiskFileNum)
	// Close closes the container and all underlying engines (active and
	// quiesced). Blocks until all container-level Ref/Unref pairs complete,
	// then quiesces and closes the active engine and waits for all quiesced
	// engine close goroutines to finish. It also waits for all the engines
	// RSEngineSnapshots to be closed.
	Close()
	// TestingInnerEngine returns the active underlying innerRSEngine. For
	// use in tests that need to inspect engine internals.
	TestingInnerEngine() innerRSEngine
}

// RSEngineSnapshot is pinned to a paricular manifest/version.
type RSEngineSnapshot interface {
	// ManifestInfo returns the complete information for the current manifest.
	// This is useful to the higher layer when adding a new replica, since the
	// returned information can be used to create hardlinks.
	ManifestInfo() ManifestInfo
	// ManifestNum returns the DiskFileNum of the manifest of the snapshot.
	ManifestNum() DiskFileNum
	// Clone creates a new RSEngineSnapshot that shares the same pinned manifest
	// state.
	Clone() RSEngineSnapshot
	// Split is used as part of splitting a range. It does the following.
	// - Creates the new LHS manifest in its own directory
	// - Hardlinks RHS files and manifest to rhsDir
	// It uses ManifestChangeCommitter.GetFileNums.
	//
	// Returns:
	// - lhsManifest: FileNameAndNum for the new LHS manifest
	// - rhs: ManifestInfo with manifest and files in rhsDir
	//
	// It is the callers responsibility to hardlink to other replicas and
	// to install these manifests.
	Split(ctx context.Context, splitKey roachpb.Key, rhsDir string) (
		lhsManifest FileNameAndNum, rhs ManifestInfo, err error)
	// Merge is used as part of merging two ranges. It is called on the LHS
	// RSEngineSnapshot. It does the following:
	// - Queries RHS snapshot for its directory and filenames
	// - Hardlinks RHS files into LHS directory with new renumbered names
	// - Creates the new merged manifest
	// It uses ManifestChangeCommitter.GetFileNums.
	//
	// Returns:
	// - merged: ManifestInfo with new manifest and files referenced by it that
	//   were not in the previous LHS manifest.
	//
	// It is the callers responsibility to hardlinks to other replicas and to
	// install the new manifest.
	Merge(ctx context.Context, rhs RSEngineSnapshot) (merged ManifestInfo, err error)
	// Close releases the snapshot.
	Close()
}

// RSEngineOptions holds options for creating a RSEngine.
type RSEngineOptions struct {
	ManifestChangeCommitter ManifestChangeCommitter
	BasaltFS                vfs.FS
	// BasaltDir is the directory containing the range-shared engine data files.
	BasaltDir string
	// BasaltScratchPathDir is a directory to use for scratch files.
	BasaltScratchPathDir string
	// LogCtx is the context used for Pebble's logger. It should carry
	// logging tags (e.g. node, store, range) from the caller.
	LogCtx context.Context
	// Stopper is used to run async close goroutines for quiesced engines.
	Stopper *stop.Stopper
	// TestingOpenRSEngineFunc allows tests to override the func to open an
	// innerRSEngine.
	TestingOpenRSEngineFunc OpenInnerRSEngineFunc
}

// InnerRSEngineOptions holds options for opening an innerRSEngine. Fields are
// package-private and mostly mirror those of RSEngineOptions.
type InnerRSEngineOptions struct {
	manifestChangeCommitter ManifestChangeCommitter
	basaltFS                vfs.FS
	basaltDir               string
	basaltScratchPathDir    string
	logCtx                  context.Context
}

// innerRSEngine is the interface for the underlying range-shared engine
// implementation. Methods are unexported because only rsEngineContainer (within
// this package) calls them. Most methods match those on RSEngine.
type innerRSEngine interface {
	compactionToggle(enable bool)
	// enableUnreferencedFileDeletion is called when the engine will never be
	// reopened pointing to an older manifest, and there are no older quiesced
	// engines.
	//
	// https://github.com/cockroachlabs/basalt/issues/289 should obviate the
	// need for durability detection, if we go with the WAG solution.
	enableUnreferencedFileDeletion()
	waitForOngoingManifestChanges()
	currentManifestNum() DiskFileNum
	flushSSTables(scratchNames []string, flushCommit *FlushCommitInfo) error
	addSSTables(scratchNames []string) error
	// ref increments the external reference count, which prevents closeInner from
	// completing. Must not be called if quiesce has been called.
	ref()
	// unref decrements the external reference count. Must be paired with a
	// prior ref call. closeInner blocks until all ref/unref pairs complete.
	unref()
	newSnapshot() RSEngineSnapshot
	// prepareExternalManifest reads a manifest file (previously hardlinked into
	// BasaltDir by the leaseholder) from disk, builds the in-memory state, and
	// stages it as a candidate. The caller guarantees that the manifestNum is the
	// one immediately succeeding currentManifestNum. Must not be called if
	// quiesce has been called.
	prepareExternalManifest(manifestNum DiskFileNum) error
	// installPreparedManifest promotes the prepared candidate version to current.
	// Must only be called after prepareExternalManifest returned with no error.
	// After this call, currentManifestNum() returns manifestNum. Must not be
	// called if quiesce has been called.
	installPreparedManifest(manifestNum DiskFileNum)
	// quiesce prevents new background work (flushes, compactions) from starting.
	// The DB remains open so that outstanding snapshots continue to work, and new
	// snapshots can be created. In-flight compactions finish naturally, but will
	// likely fail since the manifestNum of this DB has already been superceded.
	// quiesce does NOT wait for outstanding ref/unref pairs to drain. Callers
	// must still call closeInner() afterward to release resources.
	quiesce()
	// closeInner closes the engine, releasing all resources. This method blocks
	// until all outstanding RSEngineSnapshots are closed, and all ref/unref pairs
	// complete.
	closeInner()
}

// OpenInnerRSEngineFunc is a function type for opening an innerRSEngine. It
// allows injection of TestingRSEngine for testing.
type OpenInnerRSEngineFunc func(manifestNum DiskFileNum, opts InnerRSEngineOptions) (innerRSEngine, error)

// OpenInnerRSEngine is the default function for opening an innerRSEngine.
// Currently points to the testing implementation.
var OpenInnerRSEngine = OpenTestingRSEngine

// NoManifestNum indicates that no shared manifest exists yet for the
// range-shared engine. A fresh RSEngine opened with this value has no data.
// Pebble creates an internal bootstrap manifest (MANIFEST-000000 for
// range-shared LSMs) but CockroachDB does not track it. The first
// InstallPreparedManifest call replaces this with a real shared manifest.
//
// Code in kvserver can use this sentinel to skip range-shared engine work on
// split, merge, outgoing snapshot.
const NoManifestNum DiskFileNum = 0

// ErrInnerRSEngineClosed is returned when an operation is attempted on a closed
// innerRSEngine. Callers can use errors.Is to detect this condition and retry.
var ErrInnerRSEngineClosed = errors.New("engine is closed")

// TODO(sumeer): export Pebble's base.DiskFileNum type and use that instead of
// this.
type DiskFileNum uint64
