// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import "github.com/cockroachdb/cockroach/pkg/base"

type VecIndexTestingKnobs struct {
	DuringVecIndexPull func()
	BeforeVecIndexWait func()

	// PanicDuringSearch, if set, is invoked at the top of every call to
	// Searcher.Search. Gate the closure on a counter to fire on a specific
	// call only.
	PanicDuringSearch func()

	// PanicDuringMutationSearch, if set, is invoked at the top of every call
	// to MutationSearcher.SearchForInsert and SearchForDelete.
	PanicDuringMutationSearch func()

	// PanicDuringCspannSearch, if set, is invoked at the top of
	// cspann.Index.Search via IndexOptions.PanicDuringCspannSearch.
	PanicDuringCspannSearch func()
}

var _ base.ModuleTestingKnobs = (*VecIndexTestingKnobs)(nil)

func (VecIndexTestingKnobs) ModuleTestingKnobs() {}
