// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.

package storagebase

// BatchEvalTestingKnobs contains testing helpers that are used during batch evaluation.
type BatchEvalTestingKnobs struct {
	// TestingEvalFilter is called before evaluating each command. The
	// number of times this callback is run depends on the propEvalKV
	// setting, and it is therefore deprecated in favor of either
	// TestingProposalFilter (which runs only on the lease holder) or
	// TestingApplyFilter (which runs on each replica). If your filter is
	// not idempotent, consider wrapping it in a
	// ReplayProtectionFilterWrapper.
	// TODO(bdarnell,tschottdorf): Migrate existing tests which use this
	// to one of the other filters. See #10493
	// TODO(andrei): Provide guidance on what to use instead for trapping reads.
	TestingEvalFilter ReplicaCommandFilter
	// NumKeysEvaluatedForRangeIntentResolution is set by the stores to the
	// number of keys evaluated for range intent resolution.
	NumKeysEvaluatedForRangeIntentResolution *int64
	// DisallowUnsequencedTransactionalWrites enables an assertion that all
	// transactional writes include Request-scoped sequence numbers. This
	// assertion is not safe to enable by default, because it would trigger
	// in mixed-version clusters. However, it is useful in testing to ensure
	// that tests properly assign these sequence numbers.
	//
	// TODO(nvanbenschoten): Remove this testing knob in 2.2. The corresponding
	// assertion can be performed unconditionally.
	DisallowUnsequencedTransactionalWrites bool
}
