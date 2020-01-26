// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// TestParseDatumStringAs tests that datums are roundtrippable between
// printing with FmtExport and ParseDatumStringAs, but with random datums.
// This test lives in sqlbase to avoid dependency cycles when trying to move
// RandDatumWithNullChance into tree.
func TestRandParseDatumStringAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tests := []*types.T{
		types.Bool,
		types.Bytes,
		types.Date,
		types.Decimal,
		types.Float,
		types.INet,
		types.Int,
		types.Jsonb,
		types.String,
		types.Uuid,
		types.Interval,
		types.Timestamp,
		types.MakeTimestamp(0),
		types.MakeTimestamp(3),
		types.MakeTimestamp(6),
		types.TimestampTZ,
		types.MakeTimestampTZ(0),
		types.MakeTimestampTZ(3),
		types.MakeTimestampTZ(6),
		types.Time,
		types.MakeTime(0),
		types.MakeTime(3),
		types.MakeTime(6),
		types.TimeTZ,
		types.MakeTimeTZ(0),
		types.MakeTimeTZ(3),
		types.MakeTimeTZ(6),
		// TODO (rohany): extend this test with array types.
	}
	evalCtx := tree.NewTestingEvalContext(nil)
	rng, _ := randutil.NewPseudoRand()
	for _, typ := range tests {
		const testsForTyp = 100
		t.Run(typ.String(), func(t *testing.T) {
			for i := 0; i < testsForTyp; i++ {
				datum := RandDatumWithNullChance(rng, typ, 0)

				// Because of how RandDatumWithNullChanceWorks, we might
				// get an interesting datum for a time related type that
				// doesn't have the precision that we requested. In these
				// cases, manually correct the type ourselves.
				prec := tree.TimeFamilyPrecisionToRoundDuration(typ.Precision())
				switch d := datum.(type) {
				case *tree.DTimestampTZ:
					datum = d.Round(prec)
				case *tree.DTimestamp:
					datum = d.Round(prec)
				case *tree.DTime:
					datum = d.Round(prec)
				case *tree.DTimeTZ:
					datum = d.Round(prec)
				}

				ds := tree.AsStringWithFlags(datum, tree.FmtExport)
				parsed, err := tree.ParseDatumStringAs(typ, ds, evalCtx)
				if err != nil {
					t.Fatal(err)
				}
				if parsed.Compare(evalCtx, datum) != 0 {
					t.Fatal("expected", datum, "found", parsed)
				}
			}
		})
	}
}
