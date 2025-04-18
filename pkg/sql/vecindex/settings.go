// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecpb"
)

// DeterministicFixupsSetting, if true, makes all background index operations
// deterministic by:
//  1. Using a fixed pseudo-random seed for random operations.
//  2. Using a single background worker that processes fixups.
//  3. Synchronously running the worker only at prescribed times, e.g. before
//     running SearchForInsert or SearchForDelete.
var DeterministicFixupsSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.vecindex.deterministic_fixups.enabled",
	"set to true to make all background index operations deterministic, for testing",
	false,
	settings.WithVisibility(settings.Reserved),
)

// StalledOpTimeoutSetting specifies how long a split/merge operation can remain
// in its current state before another fixup worker may attempt to assist. If
// this is set too high, then a fixup can get stuck for too long. If it is set
// too low, then multiple workers can assist at the same time, resulting in
// duplicate work.
// TODO(andyk): Consider making this more dynamic, e.g. with
// livenesspb.NodeVitalityInterface.
var StalledOpTimeoutSetting = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.vecindex.stalled_op.timeout",
	"amount of time before other vector index workers will assist with a stalled background fixup",
	cspann.DefaultStalledOpTimeout,
	settings.WithPublic,
)

// SearchBeamSizeSetting controls the number of candidates examined during
// vector index searches. It represents the number of vector partitions that are
// are considered at each level of the search tree. Higher values increase
// search accuracy but require more processing resources.
var SearchBeamSizeSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.vecindex.search_beam_size",
	"number of vector partitions searched at each level of the search tree "+
		"(higher values increase accuracy but require more processing)",
	32,
	settings.IntInRange(1, 512),
)

// VectorIndexEnabled is used to enable and disable vector indexes.
var VectorIndexEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"feature.vector_index.enabled",
	"set to true to enable vector indexes, false to disable; default is false",
	false,
	settings.WithPublic)

// CheckEnabled returns an error if the feature.vector_index.enabled cluster
// setting is false.
func CheckEnabled(sv *settings.Values) error {
	if !VectorIndexEnabled.Get(sv) {
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"vector indexes are not enabled; enable with the feature.vector_index.enabled cluster setting")
	}
	return nil
}

// MakeVecConfig constructs a new VecConfig with Dims and Seed set.
func MakeVecConfig(evalCtx *eval.Context, typ *types.T) vecpb.Config {
	if DeterministicFixupsSetting.Get(&evalCtx.Settings.SV) {
		// Create index with well-known seed and deterministic fixups.
		return vecpb.Config{Dims: typ.Width(), Seed: 42, IsDeterministic: true}
	}
	return vecpb.Config{Dims: typ.Width(), Seed: evalCtx.GetRNG().Int63()}
}
