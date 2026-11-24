// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package restorerevlog

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"iter"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// RewriteKeyFn rewrites a key from the source keyspace to the target
// keyspace. It returns the rewritten key, a boolean indicating whether
// the key matched a rewrite rule, and any error. If the key doesn't
// match any rule, ok is false.
type RewriteKeyFn func(key []byte) (rewritten []byte, ok bool, err error)

// NewKeyRewriter is set by the backup package's init() to avoid a
// circular import. It creates a RewriteKeyFn from table/tenant rekeys.
//
// TODO(kev-cao): Remove this injection by extracting KeyRewriter and
// MakeKeyRewriterFromRekeys into a shared leaf package when we get
// around to breaking up pkg/backup.
var NewKeyRewriter func(
	codec keys.SQLCodec,
	tableRekeys []execinfrapb.TableRekey,
	tenantRekeys []execinfrapb.TenantRekey,
) (RewriteKeyFn, error)

const revlogLocalMergeProcessorName = "revlogLocalMerge"

// revlogLocalMergeProcessor runs one node's share of the revision log
// local merge. Each processor receives a set of tick manifests in its
// spec, reads the corresponding tick data from the revision log,
// performs a k-way merge across all events sorted by (user_key ASC,
// mvcc_ts ASC), deduplicates to keep only the latest revision per key
// with timestamp ≤ the restore AOST, rewrites key prefixes from
// source to target keyspace, and writes the result as MVCC-encoded
// SSTs to nodelocal storage. SST metadata and reservoir samples are
// sent back to the coordinator via BulkProcessorProgress messages.
type revlogLocalMergeProcessor struct {
	execinfra.ProcessorBase
	spec execinfrapb.RevlogLocalMergeSpec

	cancel   context.CancelFunc
	wg       ctxgroup.Group
	progCh   chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress
	mergeErr error
}

var (
	_ execinfra.Processor = &revlogLocalMergeProcessor{}
	_ execinfra.RowSource = &revlogLocalMergeProcessor{}
)

// NewRevlogLocalMergeProcessor constructs a revlogLocalMergeProcessor.
// It is registered with the DistSQL processor factory via an init()
// in pkg/backup.
func NewRevlogLocalMergeProcessor(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.RevlogLocalMergeSpec,
	post *execinfrapb.PostProcessSpec,
) (execinfra.Processor, error) {
	proc := &revlogLocalMergeProcessor{
		spec:   spec,
		progCh: make(chan execinfrapb.RemoteProducerMetadata_BulkProcessorProgress),
	}
	if err := proc.Init(
		ctx, proc, post, []*types.T{}, flowCtx, processorID,
		nil, /* memMonitor */
		execinfra.ProcStateOpts{
			InputsToDrain: nil,
			TrailingMetaCallback: func() []execinfrapb.ProducerMetadata {
				proc.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}
	return proc, nil
}

// Start implements the execinfra.RowSource interface.
func (p *revlogLocalMergeProcessor) Start(ctx context.Context) {
	ctx = p.StartInternal(ctx, revlogLocalMergeProcessorName)
	log.Dev.Infof(
		ctx,
		"revlog local merge processor started with %d ticks",
		len(p.spec.Ticks),
	)

	grpCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.wg = ctxgroup.WithContext(grpCtx)
	p.wg.GoCtx(func(ctx context.Context) error {
		defer close(p.progCh)
		p.mergeErr = p.runLocalMerge(ctx)
		return nil
	})
}

// Next implements the execinfra.RowSource interface.
func (p *revlogLocalMergeProcessor) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if p.State != execinfra.StateRunning {
		return nil, p.DrainHelper()
	}

	for prog := range p.progCh {
		pr := prog
		return nil, &execinfrapb.ProducerMetadata{BulkProcessorProgress: &pr}
	}

	p.MoveToDraining(p.mergeErr)
	return nil, p.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (p *revlogLocalMergeProcessor) ConsumerClosed() {
	p.close()
}

func (p *revlogLocalMergeProcessor) close() {
	if p.Closed {
		return
	}
	if p.cancel != nil {
		p.cancel()
	}
	_ = p.wg.Wait()
	p.InternalClose()
}

// runLocalMerge performs the core merge algorithm in a single
// streaming pass with no intermediate buffering:
//  1. Open the collection storage and create tick readers.
//  2. K-way merge events across all ticks via a min-heap.
//  3. Deduplicate: keep the latest timestamp ≤ AOST per key.
//  4. Rewrite key prefixes (source→target keyspace).
//  5. Write to output SSTs on nodelocal storage via
//     ExternalFileAllocator, splitting at the configured batch
//     size threshold.
//  6. Send SST metadata and reservoir samples back via progCh.
//
// Key rewriting preserves sort order because restore allocates new
// descriptor IDs in the same order as the originals (see allocateIDs
// in restore_planning.go), so no re-sort is needed.
func (p *revlogLocalMergeProcessor) runLocalMerge(ctx context.Context) error {
	if len(p.spec.Ticks) == 0 {
		return nil
	}

	user := p.spec.UserProto.Decode()
	store, err := p.FlowCtx.Cfg.ExternalStorageFromURI(
		ctx, p.spec.CollectionURI, user,
	)
	if err != nil {
		return errors.Wrap(err, "opening collection storage")
	}
	defer store.Close()

	rewriteKey, err := NewKeyRewriter(
		p.FlowCtx.Codec(),
		p.spec.TableRekeys,
		p.spec.TenantRekeys,
	)
	if err != nil {
		return errors.Wrap(err, "creating key rewriter")
	}

	// Write output SSTs to nodelocal://{instanceID}/job/{jobID}/map/,
	// matching the convention used by import and index backfill so
	// that BulkJobCleaner can sweep on job completion or failure.
	instanceID := p.FlowCtx.NodeID.SQLInstanceID()
	paths := bulkutil.NewDistMergePaths(jobspb.JobID(p.spec.JobID))
	outputURI := fmt.Sprintf(
		"nodelocal://%d/%s", instanceID, paths.MapPath(),
	)
	outputStore, err := p.FlowCtx.Cfg.ExternalStorageFromURI(
		ctx, outputURI, user,
	)
	if err != nil {
		return errors.Wrap(err, "opening nodelocal output storage")
	}
	defer outputStore.Close()

	alloc := bulksst.NewExternalFileAllocator(
		outputStore, outputURI, p.FlowCtx.Cfg.DB.KV().Clock(), instanceID,
	)

	maxSize := bulksst.BatchSize.Get(&p.FlowCtx.Cfg.Settings.SV)
	var w storage.SSTWriter
	var curURI string
	var firstKey, lastKey roachpb.Key
	var keyCount uint64

	openNewSST := func() error {
		writable, uri, openErr := alloc.AddFile(ctx)
		if openErr != nil {
			return errors.Wrap(openErr, "creating output SST file")
		}
		curURI = uri
		w = storage.MakeIngestionSSTWriter(
			ctx, p.FlowCtx.Cfg.Settings, writable,
		)
		firstKey = nil
		lastKey = nil
		keyCount = 0
		return nil
	}

	flushSST := func() error {
		if keyCount == 0 {
			w.Close()
			return nil
		}
		if err := w.Finish(); err != nil {
			w.Close()
			return errors.Wrap(err, "finishing output SST")
		}
		span := roachpb.Span{
			Key:    firstKey,
			EndKey: lastKey.Next(),
		}
		alloc.CommitFile(
			curURI, span, firstKey, uint64(w.DataSize), keyCount,
		)
		return nil
	}

	if err := openNewSST(); err != nil {
		return err
	}

	for entry, mergeErr := range MergeTickEvents(ctx, store, p.spec) {
		if mergeErr != nil {
			w.Close()
			return mergeErr
		}
		rewritten, ok, rewriteErr := rewriteKey(entry.Key.Key)
		if rewriteErr != nil {
			// The revlog contains events for all tables in the backup
			// scope, but we only have rekeys for tables being restored.
			// Skip keys for unknown tables.
			continue
		}
		if !ok {
			// Key doesn't match any rewrite rule — skip.
			continue
		}
		entry.Key.Key = rewritten

		// Rewriting the key invalidates the roachpb.Value checksum
		// (which is computed over the original key). Clear and
		// recompute it for the new key.
		if len(entry.Value) > 0 {
			val := roachpb.Value{RawBytes: entry.Value}
			val.ClearChecksum()
			val.InitChecksum(entry.Key.Key)
			entry.Value = val.RawBytes
		}

		// Split SSTs at the configured batch size threshold.
		if keyCount > 0 && w.DataSize >= maxSize {
			if err := flushSST(); err != nil {
				return err
			}
			if err := openNewSST(); err != nil {
				return err
			}
		}

		if firstKey == nil {
			firstKey = append(roachpb.Key(nil), entry.Key.Key...)
		}
		lastKey = append(lastKey[:0], entry.Key.Key...)
		keyCount++
		if err := w.PutRawMVCC(entry.Key, entry.Value); err != nil {
			w.Close()
			return errors.Wrapf(err, "writing key %s", entry.Key)
		}
	}

	if err := flushSST(); err != nil {
		return err
	}

	files := alloc.GetFileList()
	if len(files.SST) == 0 {
		log.Dev.Infof(ctx, "no events after merge, dedup, and rewrite")
		return nil
	}

	// Send SST metadata back to the coordinator as a BulkMapProgress
	// message, following the pattern used by import and index backfill.
	manifests := bulksst.SSTFilesToManifests(files, nil /* writeTS */)
	mapProgress := execinfrapb.BulkMapProgress{SSTManifests: manifests}
	any, marshalErr := gogotypes.MarshalAny(&mapProgress)
	if marshalErr != nil {
		return errors.Wrap(marshalErr, "marshaling SST manifests")
	}
	prog := execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{
		ProgressDetails: *any,
	}
	select {
	case p.progCh <- prog:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// MergeTickEvents reads all assigned ticks, k-way merges their events
// by (user_key ASC, mvcc_ts ASC), and deduplicates to keep the latest
// revision per key with timestamp ≤ restoreTS. Results are yielded in
// key-ascending order via the returned iterator.
func MergeTickEvents(
	ctx context.Context, store cloud.ExternalStorage, spec execinfrapb.RevlogLocalMergeSpec,
) iter.Seq2[MergedEntry, error] {
	return func(yield func(MergedEntry, error) bool) {
		lr := revlog.NewLogReader(store)

		// Build tick iterators and seed the heap.
		h := &eventHeap{}
		heap.Init(h)

		// Ensure all heap iterators are closed when we exit, whether
		// due to initialization failure, consumer stop, or normal
		// completion. Each tickIterator is backed by an iter.Pull2
		// goroutine that leaks if stop() is never called.
		defer func() {
			for h.Len() > 0 {
				heap.Pop(h).(*tickIterator).close()
			}
		}()

		for i := range spec.Ticks {
			tick := revlog.Tick{
				EndTime:  spec.Ticks[i].TickEnd,
				Manifest: spec.Ticks[i],
			}
			tr := lr.GetTickReader(ctx, tick, nil /* spans */)
			it := newTickIterator(ctx, tr)
			if it.err != nil {
				it.close()
				yield(MergedEntry{}, errors.Wrapf(
					it.err,
					"initializing tick iterator for tick ending %s",
					tick.EndTime,
				))
				return
			}
			if it.cur != nil {
				heap.Push(h, it)
			} else {
				it.close()
			}
		}

		restoreTS := spec.RestoreTimestamp
		var curKey roachpb.Key
		var latestKey storage.MVCCKey
		var latestVal []byte
		hasLatest := false

		emitLatest := func() bool {
			if !hasLatest {
				return true
			}
			hasLatest = false
			return yield(MergedEntry{
				Key: storage.MVCCKey{
					Key:       slices.Clone(latestKey.Key),
					Timestamp: latestKey.Timestamp,
				},
				Value: slices.Clone(latestVal),
			}, nil)
		}

		for h.Len() > 0 {
			it := (*h)[0]
			ev := it.cur

			it.advance()
			if it.err != nil {
				yield(MergedEntry{}, errors.Wrap(
					it.err, "reading tick events",
				))
				return
			}
			if it.cur != nil {
				heap.Fix(h, 0)
			} else {
				heap.Pop(h).(*tickIterator).close()
			}

			// Skip events past the AOST.
			if !restoreTS.IsEmpty() &&
				restoreTS.Less(ev.Timestamp) {
				continue
			}

			// Dedup: for each user key, keep the latest timestamp.
			// Events arrive in (key ASC, ts ASC) order, so we keep
			// overwriting the latest entry with newer timestamps.
			if !bytes.Equal(curKey, ev.Key) {
				if !emitLatest() {
					return
				}
				curKey = slices.Clone(ev.Key)
			}
			latestKey = storage.MVCCKey{
				Key:       ev.Key,
				Timestamp: ev.Timestamp,
			}
			latestVal = ev.Value.RawBytes
			hasLatest = true
		}
		emitLatest()
	}
}

// MergedEntry is one deduplicated key-value pair produced by the
// k-way merge of revision log tick events.
type MergedEntry struct {
	Key   storage.MVCCKey
	Value []byte // raw roachpb.Value bytes (empty = tombstone)
}

// tickIterator is a pull-based adapter around the push-based
// TickReader.Events() iterator. It uses iter.Pull2 to convert the
// push-based iter.Seq2 into a pull-based (next, stop) pair, allowing
// the k-way merge heap to peek at and advance individual tick
// iterators independently.
type tickIterator struct {
	pull func() (revlog.Event, error, bool)
	stop func()
	cur  *revlog.Event
	err  error
}

func newTickIterator(ctx context.Context, tr *revlog.TickReader) *tickIterator {
	pull, stop := iter.Pull2(tr.Events(ctx))
	it := &tickIterator{pull: pull, stop: stop}
	it.advance()
	return it
}

// advance moves to the next event, updating cur and err.
func (it *tickIterator) advance() {
	ev, err, ok := it.pull()
	if err != nil {
		it.cur = nil
		it.err = err
		return
	}
	if !ok {
		it.cur = nil
		return
	}
	it.cur = &ev
}

// close stops the underlying iterator.
func (it *tickIterator) close() {
	it.stop()
}

// eventHeap is a min-heap of tickIterators ordered by their next
// event's (Key ASC, Timestamp ASC).
type eventHeap []*tickIterator

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool {
	a, b := h[i].cur, h[j].cur
	if c := bytes.Compare(a.Key, b.Key); c != 0 {
		return c < 0
	}
	return a.Timestamp.Less(b.Timestamp)
}

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(*tickIterator))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
