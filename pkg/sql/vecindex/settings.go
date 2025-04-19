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
	return vecpb.Config{Dims: typ.Width(), Seed: evalCtx.GetRNG().Int63()}
}
