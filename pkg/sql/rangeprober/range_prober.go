// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangeprober

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvprober"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// RangeProber implements eval.RangeProber.
type RangeProber struct {
	db  *kv.DB
	ops kvprober.ProberOps
}

// NewRangeProber returns a new instance of a RangeProber.
func NewRangeProber(db *kv.DB) *RangeProber {
	return &RangeProber{
		db: db,
	}
}

// RunProbe implements the eval.RangeProber interface.
func (r *RangeProber) RunProbe(
	ctx context.Context, desc *roachpb.RangeDescriptor, isWrite bool,
) error {
	key := kvprober.ProbeKeyForRange(desc)
	op := r.ops.Read
	if isWrite {
		op = r.ops.Write
	}
	// NB: intentionally using a separate txn per probe to avoid undesirable cross-probe effects.
	return r.db.Txn(ctx, op(key))
}
