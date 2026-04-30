// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/mvccencoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// BranchSender wraps an inner kv.Sender (typically a DistSender) for a branch
// tenant. It implements copy-on-write semantics at the request level:
//
//   - Writes pass through to the branch's keyspace.
//   - Reads (Get/Scan) first hit the branch's keyspace with IncludeTombstones
//     set. Any keys the branch has not written are then fetched from the
//     parent tenant at the branch's fork timestamp via a separate, non-
//     transactional batch and merged into the response.
//
// The fallback batch is non-transactional because the parent's keyspace is
// outside the branch txn's scope; refreshes/locks across tenants are not
// supported in this PoC. See .memory/WHITEBOARD.md.
type BranchSender struct {
	inner       kv.Sender
	fallbackDB  *kv.DB
	parentCodec keys.SQLCodec
	branchCodec keys.SQLCodec
	branchTS    hlc.Timestamp
}

var _ kv.Sender = (*BranchSender)(nil)

// Unwrap returns the inner sender. This lets callers that need direct access
// to the underlying DistSender (e.g. the rangefeed dbAdapter) bypass the
// CoW layer. Rangefeeds operate on raw KV state and have no notion of
// branching; the parent's data is reachable directly via its keyspace.
func (b *BranchSender) Unwrap() kv.Sender {
	return b.inner
}

// NewBranchSender constructs a BranchSender. The inner sender should be the
// raw DistSender (not a TxnCoordSender) so write batches pass through directly.
// fallbackDB is a kv.DB rooted on the same DistSender; its non-transactional
// sender is used to issue fallback reads against the parent (which may need
// to be auto-wrapped in a txn when they span multiple ranges).
func NewBranchSender(
	inner kv.Sender,
	fallbackDB *kv.DB,
	parentCodec, branchCodec keys.SQLCodec,
	branchTS hlc.Timestamp,
) *BranchSender {
	return &BranchSender{
		inner:       inner,
		fallbackDB:  fallbackDB,
		parentCodec: parentCodec,
		branchCodec: branchCodec,
		branchTS:    branchTS,
	}
}

// Send implements the kv.Sender interface.
func (b *BranchSender) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	// Two write-side fixups for CoW semantics, applied to every batch (read or
	// write) since either kind can carry an Increment or CPut on a key the
	// branch has never written.
	//
	// Increment: MVCCIncrement starts from 0 if the key is missing, so the
	// branch would allocate ID sequence values that collide with the parent's.
	// We seed the branch with the parent's current value before the Increment
	// is dispatched.
	//
	// CPut: a CPut whose expected bytes came from a parent-fallback read will
	// fail against the branch (which has nil for that key). Setting
	// AllowIfDoesNotExist=true makes the CPut succeed when the branch is empty
	// — i.e. the branch's first write of a key it previously only saw via the
	// parent.
	if err := b.seedIncrements(ctx, ba); err != nil {
		return nil, kvpb.NewError(err)
	}
	ba = rewriteCPutsForBranch(ba)
	if pErr := b.checkParentForCPuts(ctx, ba); pErr != nil {
		return nil, pErr
	}

	if !batchHasReads(ba) {
		log.Dev.Infof(ctx, "[BRANCH] passthrough write batch (%d reqs)", len(ba.Requests))
		return b.inner.Send(ctx, ba)
	}

	log.Dev.Infof(ctx, "[BRANCH] read batch (%d reqs) at branch_ts=%s", len(ba.Requests), b.branchTS)

	// Mark every Get/Scan in the branch read with IncludeTombstones so we can
	// distinguish "branch deleted this key" from "branch never wrote it". The
	// caller's original ScanFormat is recorded so we can re-encode the merged
	// result before returning (the merge step needs KEY_VALUES, but the caller
	// may have asked for BATCH_RESPONSE / COL_BATCH_RESPONSE).
	branchBa := ba.ShallowCopy()
	branchBa.Requests = make([]kvpb.RequestUnion, len(ba.Requests))
	origFormats := make([]kvpb.ScanFormat, len(ba.Requests))
	for i, ru := range ba.Requests {
		branchBa.Requests[i], origFormats[i] = withTombstones(ru)
	}

	br, pErr := b.inner.Send(ctx, branchBa)
	if pErr != nil {
		log.Dev.Infof(ctx, "[BRANCH] branch read errored: %s", pErr)
		return nil, pErr
	}

	// For every read that returned no value (and no tombstone), build a parallel
	// fallback request against the parent's keyspace at branch_ts. If nothing
	// needs fallback, we can return the branch's response as-is.
	fallbackIdx, fallbackBa := b.buildFallback(ba, br)
	if fallbackBa == nil {
		log.Dev.Infof(ctx, "[BRANCH] no fallback needed (%d reqs all satisfied by branch)", len(ba.Requests))
		stripTombstones(br)
		restoreScanFormat(br, origFormats)
		return br, nil
	}

	log.Dev.Infof(ctx, "[BRANCH] dispatching fallback to parent (%d of %d reqs)", len(fallbackIdx), len(ba.Requests))
	// Use the fallbackDB's non-transactional sender so multi-range Gets/Scans
	// get auto-wrapped in a txn. The CrossRangeTxnWrapperSender drops the
	// AOST timestamp when it auto-wraps, so the fallback observes the
	// parent's current state rather than branch_ts; the protected timestamp
	// merely guarantees the data is reachable. This is acceptable for the
	// PoC. See .memory/WHITEBOARD.md.
	fbResp, pErr := b.fallbackDB.NonTransactionalSender().Send(ctx, fallbackBa)
	if pErr != nil {
		log.Dev.Infof(ctx, "[BRANCH] fallback errored: %s", pErr)
		return nil, pErr
	}

	b.mergeFallback(br, fbResp, fallbackIdx)
	stripTombstones(br)
	restoreScanFormat(br, origFormats)
	log.Dev.Infof(ctx, "[BRANCH] merged response ready")
	return br, nil
}

// batchHasReads reports whether ba contains any read requests we need to apply
// CoW semantics to.
func batchHasReads(ba *kvpb.BatchRequest) bool {
	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *kvpb.GetRequest, *kvpb.ScanRequest:
			return true
		}
	}
	return false
}

// withTombstones returns a RequestUnion that, for read requests, has the
// IncludeTombstones flag set. Scans are also forced into KEY_VALUES format so
// that the BranchSender's row-level merge has access to the per-row keys
// (BATCH_RESPONSE and COL_BATCH_RESPONSE bury the keys inside opaque bytes,
// which we cannot rewrite without a table descriptor). The caller's original
// ScanFormat is returned so the response can be re-encoded after the merge.
func withTombstones(ru kvpb.RequestUnion) (kvpb.RequestUnion, kvpb.ScanFormat) {
	switch r := ru.GetInner().(type) {
	case *kvpb.GetRequest:
		clone := *r
		clone.IncludeTombstones = true
		var out kvpb.RequestUnion
		out.MustSetInner(&clone)
		return out, kvpb.KEY_VALUES
	case *kvpb.ScanRequest:
		clone := *r
		clone.IncludeTombstones = true
		orig := clone.ScanFormat
		clone.ScanFormat = kvpb.KEY_VALUES
		var out kvpb.RequestUnion
		out.MustSetInner(&clone)
		return out, orig
	default:
		return ru, kvpb.KEY_VALUES
	}
}

// buildFallback constructs a non-transactional BatchRequest against the
// parent's keyspace at branch_ts for the subset of reads in ba that returned
// no value and no tombstone in br. It returns the fallback batch and a
// parallel slice mapping fallback request index -> original request index.
// If no requests need a fallback, it returns (nil, nil).
func (b *BranchSender) buildFallback(
	ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) (origIdx []int, fallbackBa *kvpb.BatchRequest) {
	for i, ru := range ba.Requests {
		switch r := ru.GetInner().(type) {
		case *kvpb.GetRequest:
			resp := br.Responses[i].GetInner().(*kvpb.GetResponse)
			if resp.Value != nil {
				// Either a value or a tombstone (empty bytes with timestamp).
				// Either way the branch has a definitive answer for this key.
				continue
			}
			fbReq := &kvpb.GetRequest{
				RequestHeader: kvpb.RequestHeader{
					Key: rewriteKey(r.Key, b.branchCodec, b.parentCodec),
				},
			}
			if fallbackBa == nil {
				fallbackBa = newFallbackBatch(b.branchTS)
			}
			origIdx = append(origIdx, i)
			var out kvpb.RequestUnion
			out.MustSetInner(fbReq)
			fallbackBa.Requests = append(fallbackBa.Requests, out)

		case *kvpb.ScanRequest:
			// Scans always need a fallback: the branch may have only some of
			// the rows in the range, and the parent may have additional rows.
			// We let mergeFallback dedupe and respect tombstones on overlap.
			fbReq := &kvpb.ScanRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    rewriteKey(r.Key, b.branchCodec, b.parentCodec),
					EndKey: rewriteKey(r.EndKey, b.branchCodec, b.parentCodec),
				},
				ScanFormat: kvpb.KEY_VALUES,
			}
			if fallbackBa == nil {
				fallbackBa = newFallbackBatch(b.branchTS)
			}
			origIdx = append(origIdx, i)
			var out kvpb.RequestUnion
			out.MustSetInner(fbReq)
			fallbackBa.Requests = append(fallbackBa.Requests, out)
		}
	}
	return origIdx, fallbackBa
}

// newFallbackBatch returns a BatchRequest with no transaction, pinned to
// branch_ts, suitable as a non-transactional AOST read against the parent
// tenant's keyspace.
func newFallbackBatch(branchTS hlc.Timestamp) *kvpb.BatchRequest {
	return &kvpb.BatchRequest{
		Header: kvpb.Header{
			Timestamp:       branchTS,
			ReadConsistency: kvpb.CONSISTENT,
		},
	}
}

// mergeFallback merges the fallback responses (against the parent) back into
// the branch's response br. fallbackIdx[i] gives the original request index
// in br corresponding to fbResp.Responses[i].
//
// For Get: if the branch returned no value, plug in the parent's value
// (rewriting the key prefix back to the branch).
// For Scan: take the union of parent rows and branch rows, prefer the
// branch row on key collision, drop any parent row whose key has a branch
// tombstone.
func (b *BranchSender) mergeFallback(
	br *kvpb.BatchResponse, fbResp *kvpb.BatchResponse, fallbackIdx []int,
) {
	// fbResp may contain extra trailing responses (e.g. an EndTxn appended
	// when the CrossRangeTxnWrapperSender auto-wrapped the batch in a txn).
	// Only the first len(fallbackIdx) responses correspond to our requests.
	for j := range fallbackIdx {
		i := fallbackIdx[j]
		switch fb := fbResp.Responses[j].GetInner().(type) {
		case *kvpb.GetResponse:
			if fb.Value == nil {
				continue
			}
			// Rewrite parent's value into branch's response slot. The Get
			// response carries no key, so we just plug the value in.
			origGet := br.Responses[i].GetInner().(*kvpb.GetResponse)
			origGet.Value = fb.Value

		case *kvpb.ScanResponse:
			origScan := br.Responses[i].GetInner().(*kvpb.ScanResponse)
			merged := mergeScanRows(origScan.Rows, fb.Rows, b.branchCodec, b.parentCodec)
			origScan.Rows = merged
		}
	}
}

// mergeScanRows merges branch and parent rows into a single key-sorted slice.
// Branch rows whose value is nil are tombstones: the row is dropped from the
// output, but the key still suppresses any matching parent row. Parent row
// keys are rewritten from the parent prefix into the branch prefix.
func mergeScanRows(
	branchRows, parentRows []roachpb.KeyValue, branchCodec, parentCodec keys.SQLCodec,
) []roachpb.KeyValue {
	// Build the set of branch keys (including tombstoned ones) so we know
	// which parent rows to suppress.
	branchKeys := make(map[string]struct{}, len(branchRows))
	for _, r := range branchRows {
		branchKeys[string(r.Key)] = struct{}{}
	}

	out := make([]roachpb.KeyValue, 0, len(branchRows)+len(parentRows))
	for _, r := range branchRows {
		if r.Value.IsPresent() && len(r.Value.RawBytes) > 0 {
			out = append(out, r)
		}
		// else: tombstone. Drop from output, but the key remains in
		// branchKeys to suppress parent.
	}
	for _, r := range parentRows {
		bk := rewriteKey(r.Key, parentCodec, branchCodec)
		if _, ok := branchKeys[string(bk)]; ok {
			continue
		}
		out = append(out, roachpb.KeyValue{Key: bk, Value: r.Value})
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].Key, out[j].Key) < 0 })
	return out
}

// stripTombstones removes tombstone entries from Get and Scan responses so
// the caller (above the branchSender) sees the standard "missing key" /
// "row absent" semantics.
func stripTombstones(br *kvpb.BatchResponse) {
	for _, ru := range br.Responses {
		switch r := ru.GetInner().(type) {
		case *kvpb.GetResponse:
			if r.Value != nil && len(r.Value.RawBytes) == 0 {
				r.Value = nil
			}
		case *kvpb.ScanResponse:
			out := r.Rows[:0]
			for _, row := range r.Rows {
				if row.Value.IsPresent() && len(row.Value.RawBytes) > 0 {
					out = append(out, row)
				}
			}
			r.Rows = out
		}
	}
}

// restoreScanFormat re-encodes Scan responses whose original ScanFormat was
// not KEY_VALUES back into the format the caller asked for. The branch and
// fallback paths are forced to KEY_VALUES so the merge can dedupe by key;
// without this step the SQL pod's KV fetcher would error with "unexpectedly
// got a ScanResponse using KEY_VALUES response format".
//
// COL_BATCH_RESPONSE is also collapsed into BATCH_RESPONSE: the columnar
// format requires an IndexFetchSpec-driven encoder, which would entail
// rewriting the spec's tenant prefix and re-running the cfetcher in this
// package. The KV fetcher accepts BatchResponses regardless of which format
// was originally requested (popBatch in pkg/sql/row/kv_batch_fetcher.go
// prefers the BatchResponses path), so this is sufficient for the PoC.
func restoreScanFormat(br *kvpb.BatchResponse, origFormats []kvpb.ScanFormat) {
	for i, ru := range br.Responses {
		scan, ok := ru.GetInner().(*kvpb.ScanResponse)
		if !ok {
			continue
		}
		if origFormats[i] == kvpb.KEY_VALUES {
			continue
		}
		scan.BatchResponses = encodeBatchResponse(scan.Rows)
		scan.Rows = nil
	}
}

// encodeBatchResponse encodes a slice of KeyValues into the BATCH_RESPONSE
// wire format expected by MVCCScanDecodeKeyValues: a single []byte buffer of
// repeated <valueLen:u32 LE><keyLen:u32 LE><MVCCKey><Value>. Returns nil for
// an empty input so an empty Scan stays empty (BatchResponses=nil rather
// than [[]byte{}], which the fetcher treats the same).
func encodeBatchResponse(rows []roachpb.KeyValue) [][]byte {
	if len(rows) == 0 {
		return nil
	}
	// Pre-size the output buffer.
	totalSize := 0
	keyLens := make([]int, len(rows))
	for i, kv := range rows {
		keyLens[i] = mvccencoding.EncodedMVCCKeyLength(kv.Key, kv.Value.Timestamp)
		totalSize += 8 + keyLens[i] + len(kv.Value.RawBytes)
	}
	buf := make([]byte, totalSize)
	pos := 0
	for i, kv := range rows {
		valLen := len(kv.Value.RawBytes)
		keyLen := keyLens[i]
		binary.LittleEndian.PutUint32(buf[pos:], uint32(valLen))
		binary.LittleEndian.PutUint32(buf[pos+4:], uint32(keyLen))
		pos += 8
		mvccencoding.EncodeMVCCKeyToBufSized(buf[pos:pos+keyLen], kv.Key, kv.Value.Timestamp, keyLen)
		pos += keyLen
		copy(buf[pos:], kv.Value.RawBytes)
		pos += valLen
	}
	return [][]byte{buf}
}

// seedIncrements ensures every IncrementRequest in ba targets a key that
// already has a value in the branch's keyspace. For each Increment whose key
// is unwritten on the branch, it copies the parent's current value at
// branch_ts into the branch. The Increment then proceeds normally and
// produces the same value the parent would have, plus the requested delta.
//
// This is the CoW fix for sequence keys: descriptor IDs, role IDs, etc. live
// at fixed sequence keys in the tenant keyspace, are mutated via Increment,
// and need to start from the parent's current value rather than 0 on a fresh
// branch.
//
// The seeding Get/Put are non-transactional: they're idempotent (repeated
// seeding writes the same value) and must be visible to all subsequent
// readers, not just the calling txn. A racing seeder writes the same value,
// so concurrent CREATE TABLE statements on a branch converge.
func (b *BranchSender) seedIncrements(ctx context.Context, ba *kvpb.BatchRequest) error {
	for _, ru := range ba.Requests {
		inc, ok := ru.GetInner().(*kvpb.IncrementRequest)
		if !ok {
			continue
		}
		if err := b.maybeSeedIncrement(ctx, inc.Key); err != nil {
			return errors.Wrapf(err, "seeding increment key %s", inc.Key)
		}
	}
	return nil
}

// maybeSeedIncrement seeds branchKey with the parent's current value at
// branch_ts if the branch has no value there yet. A no-op if the branch
// already has a value, or if the parent has none either (in which case
// MVCCIncrement starting from zero is the correct behavior).
func (b *BranchSender) maybeSeedIncrement(ctx context.Context, branchKey roachpb.Key) error {
	branchKV, err := b.fallbackDB.Get(ctx, branchKey)
	if err != nil {
		return errors.Wrap(err, "reading branch key")
	}
	if branchKV.Value != nil && branchKV.Value.IsPresent() {
		return nil
	}

	parentBa := newFallbackBatch(b.branchTS)
	parentKey := rewriteKey(branchKey, b.branchCodec, b.parentCodec)
	var u kvpb.RequestUnion
	u.MustSetInner(&kvpb.GetRequest{RequestHeader: kvpb.RequestHeader{Key: parentKey}})
	parentBa.Requests = append(parentBa.Requests, u)
	parentBr, pErr := b.fallbackDB.NonTransactionalSender().Send(ctx, parentBa)
	if pErr != nil {
		return errors.Wrap(pErr.GoError(), "reading parent key")
	}
	parentResp := parentBr.Responses[0].GetInner().(*kvpb.GetResponse)
	if parentResp.Value == nil || !parentResp.Value.IsPresent() {
		return nil
	}
	parentInt, err := parentResp.Value.GetInt()
	if err != nil {
		return errors.Wrap(err, "decoding parent value as int")
	}
	log.Dev.Infof(ctx, "[BRANCH] seeding increment key %s with parent value %d", branchKey, parentInt)
	if err := b.fallbackDB.Put(ctx, branchKey, parentInt); err != nil {
		return errors.Wrap(err, "writing seed value")
	}
	return nil
}

// rewriteCPutsForBranch returns a batch in which every ConditionalPutRequest
// with non-nil ExpBytes has AllowIfDoesNotExist set to true. On a branch the
// expected bytes of any CPut came from a read that fell through to the
// parent, so the branch keyspace is necessarily empty for that key on the
// first write — exactly the case AllowIfDoesNotExist was designed for.
//
// Returns ba unchanged if there are no CPuts to rewrite.
func rewriteCPutsForBranch(ba *kvpb.BatchRequest) *kvpb.BatchRequest {
	var modified *kvpb.BatchRequest
	for i, ru := range ba.Requests {
		cput, ok := ru.GetInner().(*kvpb.ConditionalPutRequest)
		if !ok || len(cput.ExpBytes) == 0 || cput.AllowIfDoesNotExist {
			continue
		}
		if modified == nil {
			modified = ba.ShallowCopy()
			modified.Requests = append([]kvpb.RequestUnion(nil), ba.Requests...)
		}
		clone := *cput
		clone.AllowIfDoesNotExist = true
		var out kvpb.RequestUnion
		out.MustSetInner(&clone)
		modified.Requests[i] = out
	}
	if modified == nil {
		return ba
	}
	return modified
}

// checkParentForCPuts enforces CPut(nil-expected) semantics — "the key must
// not have a value" — across both the branch's and the parent's keyspaces.
//
// CPut(nil-expected) is the storage-level mechanism behind primary key and
// unique-secondary-index uniqueness. Without intervention a CPut on a key the
// branch has never written succeeds against the empty branch keyspace even
// when the parent has a value at the same key, silently allowing a duplicate
// row that violates the constraint.
//
// For each candidate CPut we first look the key up on the branch with
// IncludeTombstones. If the branch has any state (live value or tombstone)
// the storage CPut already produces the right answer — fail on a live value,
// succeed on a tombstone (the tombstone records an explicit branch-side
// DELETE, so re-inserting at that key is legitimate). Otherwise we look the
// key up on the parent at branch_ts; a value there means the CPut would
// violate uniqueness, and we synthesize the ConditionFailedError the storage
// layer would have produced if the value lived on the branch so the SQL
// layer's ConvertBatchError can surface it as a duplicate-key error.
//
// CPut(non-nil-expected) is the "update from old to new" shape and is
// handled separately by rewriteCPutsForBranch; it does not need a parent
// check because the expected bytes already came from a parent-fallback read.
func (b *BranchSender) checkParentForCPuts(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	type cputPos struct {
		origIdx int
		key     roachpb.Key
	}
	var positions []cputPos
	for i, ru := range ba.Requests {
		cput, ok := ru.GetInner().(*kvpb.ConditionalPutRequest)
		if !ok || len(cput.ExpBytes) > 0 {
			continue
		}
		positions = append(positions, cputPos{origIdx: i, key: cput.Key})
	}
	if len(positions) == 0 {
		return nil
	}

	branchBa := &kvpb.BatchRequest{Header: kvpb.Header{ReadConsistency: kvpb.CONSISTENT}}
	for _, p := range positions {
		var u kvpb.RequestUnion
		u.MustSetInner(&kvpb.GetRequest{
			RequestHeader:     kvpb.RequestHeader{Key: p.key},
			IncludeTombstones: true,
		})
		branchBa.Requests = append(branchBa.Requests, u)
	}
	branchBr, pErr := b.fallbackDB.NonTransactionalSender().Send(ctx, branchBa)
	if pErr != nil {
		return pErr
	}

	var parentBa *kvpb.BatchRequest
	var parentToPos []int // index in parentBa.Requests -> index in positions
	for j, p := range positions {
		gr := branchBr.Responses[j].GetInner().(*kvpb.GetResponse)
		if gr.Value != nil {
			// Branch has a live value or a tombstone — the storage CPut
			// will give the correct answer without involving the parent.
			continue
		}
		if parentBa == nil {
			parentBa = newFallbackBatch(b.branchTS)
		}
		var u kvpb.RequestUnion
		u.MustSetInner(&kvpb.GetRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: rewriteKey(p.key, b.branchCodec, b.parentCodec),
			},
		})
		parentBa.Requests = append(parentBa.Requests, u)
		parentToPos = append(parentToPos, j)
	}
	if parentBa == nil {
		return nil
	}
	parentBr, pErr := b.fallbackDB.NonTransactionalSender().Send(ctx, parentBa)
	if pErr != nil {
		return pErr
	}
	for k, j := range parentToPos {
		gr := parentBr.Responses[k].GetInner().(*kvpb.GetResponse)
		if gr.Value == nil || !gr.Value.IsPresent() {
			continue
		}
		log.Dev.Infof(ctx,
			"[BRANCH] CPut(nil-expected) on inherited key %s blocked by parent value",
			positions[j].key)
		pe := kvpb.NewErrorWithTxn(
			&kvpb.ConditionFailedError{ActualValue: gr.Value}, ba.Txn,
		)
		pe.SetErrorIndex(int32(positions[j].origIdx))
		return pe
	}
	return nil
}

// rewriteKey strips fromCodec's tenant prefix from key and prepends
// toCodec's. Returns nil if key is nil.
func rewriteKey(key roachpb.Key, fromCodec, toCodec keys.SQLCodec) roachpb.Key {
	if len(key) == 0 {
		return key
	}
	fromPrefix := fromCodec.TenantPrefix()
	if !bytes.HasPrefix(key, fromPrefix) {
		// Defensive: if the key isn't in fromCodec's tenant, return as-is
		// rather than panic. Non-tenant-prefixed keys (e.g. range-local)
		// land here.
		return key
	}
	suffix := key[len(fromPrefix):]
	out := make(roachpb.Key, 0, len(toCodec.TenantPrefix())+len(suffix))
	out = append(out, toCodec.TenantPrefix()...)
	out = append(out, suffix...)
	return out
}
