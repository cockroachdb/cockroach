// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import "github.com/cockroachdb/cockroach/pkg/base"

type VecIndexTestingKnobs struct {
	DuringVecIndexPull func()
	BeforeVecIndexWait func()

	// PanicDuringSearch, if set, is invoked at the top of Searcher.Search.
	// Used by tests to verify that panics originating in pkg/sql/vecindex on
	// the read executor path are caught by the colexecerror allow-list and
	// surface as SQL errors instead of crashing the node.
	PanicDuringSearch func()

	// PanicDuringMutationSearch, if set, is invoked at the top of
	// MutationSearcher.SearchForInsert and SearchForDelete. Used by tests to
	// verify that panics originating in pkg/sql/vecindex on the mutation
	// executor path are caught by the colexecerror allow-list.
	PanicDuringMutationSearch func()
}

var _ base.ModuleTestingKnobs = (*VecIndexTestingKnobs)(nil)

func (VecIndexTestingKnobs) ModuleTestingKnobs() {}
