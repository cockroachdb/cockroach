// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
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
	searchSet cspann.SearchSet
	results   cspann.SearchResults
	resultIdx int
}

// Init wraps the given KV transaction in a C-SPANN transaction and initializes
// the search context.
//
// NOTE: The index is expected to come from a call to Manager.Get, and therefore
// using a vecstore.Store instance.
func (s *Searcher) Init(idx *cspann.Index, txn *kv.Txn) {
	s.idx = idx
	s.txn.Init(idx.Store().(*vecstore.Store), txn)
	s.idxCtx.Init(&s.txn)
}

// Search triggers a search over the index for the given vector, within the
// scope of the given prefix. "maxResults" specifies the maximum number of
// results that will be returned.
func (s *Searcher) Search(
	ctx context.Context, prefix roachpb.Key, vec vector.T, maxResults int,
) error {
	// An index-join + top-k operation will handle the re-ranking, so we skip
	// doing it here.
	options := cspann.SearchOptions{SkipRerank: true}
	s.searchSet.MaxResults = maxResults
	s.searchSet.MaxExtraResults = maxResults * cspann.RerankMultiplier
	err := s.idx.Search(ctx, &s.idxCtx, cspann.TreeKey(prefix), vec, &s.searchSet, options)
	if err != nil {
		return err
	}
	s.results = s.searchSet.PopResults()
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
