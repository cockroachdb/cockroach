// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore/vecstorepb"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// Searcher is a helper struct that makes it easy for the execution engine to
// search a C-SPANN index. It wraps creation of a C-SPANN transaction, setup of
// search context and parameters, and iteration over search results.
//
// NOTE: Searcher is intended to be embedded within an execution engine
// processor object.
type Searcher struct {
	idx       *cspann.Index
	txn       vecstore.Txn
	idxCtx    cspann.Context
	options   cspann.SearchOptions
	searchSet cspann.SearchSet
	results   cspann.SearchResults
	resultIdx int
	evalCtx   *eval.Context
}

// Init wraps the given KV transaction in a C-SPANN transaction and initializes
// the search context.
//
// NOTE: The index is expected to come from a call to Manager.Get, and therefore
// using a vecstore.Store instance.
//
// tableDesc is expected to be leased such that its lifetime is at least as long
// as txn.
func (s *Searcher) Init(
	evalCtx *eval.Context,
	idx *cspann.Index,
	txn *kv.Txn,
	fullVecFetchSpec *vecstorepb.GetFullVectorsFetchSpec,
	baseBeamSize, maxResults int,
) {
	s.idx = idx
	s.txn.Init(evalCtx, idx.Store().(*vecstore.Store), txn, fullVecFetchSpec)
	s.idxCtx.Init(&s.txn)
	s.evalCtx = evalCtx

	// An index-join + top-k operation will handle the re-ranking, so we skip
	// doing it here.
	s.options = cspann.SearchOptions{
		BaseBeamSize: baseBeamSize,
		SkipRerank:   true,
	}
	s.searchSet.MaxResults, s.searchSet.MaxExtraResults = cspann.IncreaseRerankResults(maxResults)

	// If the index is deterministic, then synchronously run the background worker
	// to process any pending fixups.
	if idx.Options().IsDeterministic {
		s.idx.ProcessFixups()
	}
}

// Search triggers a search over the index for the given vector, within the
// scope of the given prefix. "maxResults" specifies the maximum number of
// results that will be returned.
//
// NOTE: The caller is assumed to own the memory for all parameters and can
// reuse the memory after the call returns.
func (s *Searcher) Search(ctx context.Context, prefix roachpb.Key, vec vector.T) error {
	err := s.idx.Search(ctx, &s.idxCtx, cspann.TreeKey(prefix), vec, &s.searchSet, s.options)
	if err != nil {
		return err
	}
	s.results = s.searchSet.PopResults()
	s.resultIdx = 0
	return nil
}

// NextResult iterates over search results. It returns nil when there are no
// more results.
func (s *Searcher) NextResult() *cspann.SearchResult {
	if s.resultIdx >= len(s.results) {
		return nil
	}
	res := &s.results[s.resultIdx]
	s.resultIdx++
	return res
}
