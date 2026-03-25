// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"bytes"
	"cmp"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// PickResult is the outcome of comparing two datums for LWW
// tie-breaking.
type PickResult int

const (
	// Equal means the two datums are indistinguishable for
	// tie-breaking purposes.
	Equal PickResult = iota
	// ChooseA means datum A should be chosen as the winner.
	ChooseA
	// ChooseB means datum B should be chosen as the winner.
	ChooseB
)

func pickFromCmp(c int) PickResult {
	switch {
	case c < 0:
		return ChooseA
	case c > 0:
		return ChooseB
	default:
		return Equal
	}
}

// Pick compares two non-NULL datums of the same type for LWW
// tie-breaking. It uses an explicit type switch to compare values
// directly on their in-memory representation, so the result is
// stable across CockroachDB versions. The ordering does not need to
// match SQL ordering — it only needs to be deterministic and
// antisymmetric.
//
// For types where logical equality (SQL =) can hide internal
// differences (e.g. DDecimal 1.0 vs 1.00, DFloat -0.0 vs 0.0),
// Pick applies type-specific tie-breakers after finding logical
// equality.
//
// For composite types (arrays, tuples, vectors), Pick recurses into
// elements so that internal differences in nested values are caught.
func Pick(a, b tree.Datum) (PickResult, error) {
	// Unwrap OID wrappers so the type switch sees the underlying type.
	ua, ub := tree.UnwrapDOidWrapper(a), tree.UnwrapDOidWrapper(b)

	switch at := ua.(type) {
	case *tree.DBool:
		bt := ub.(*tree.DBool)
		if *at == *bt {
			return Equal, nil
		}
		// false < true
		if !*at {
			return ChooseA, nil
		}
		return ChooseB, nil

	case *tree.DInt:
		return pickFromCmp(cmp.Compare(int64(*at), int64(*ub.(*tree.DInt)))), nil

	case *tree.DFloat:
		return pickFloat(float64(*at), float64(*ub.(*tree.DFloat))), nil

	case *tree.DDecimal:
		bt := ub.(*tree.DDecimal)
		// CmpTotal is IEEE 754's totalOrder: it distinguishes
		// values that Cmp considers equal (e.g. 1.0 vs 1.00) by
		// comparing exponents, and orders -0 before +0.
		return pickFromCmp(at.Decimal.CmpTotal(&bt.Decimal)), nil

	case *tree.DString:
		return pickFromCmp(cmp.Compare(
			string(*at), string(*ub.(*tree.DString)),
		)), nil

	case *tree.DCollatedString:
		bt := ub.(*tree.DCollatedString)
		c := bytes.Compare(at.Key, bt.Key)
		if c != 0 {
			return pickFromCmp(c), nil
		}
		// Same collation key but possibly different contents.
		return pickFromCmp(cmp.Compare(at.Contents, bt.Contents)), nil

	case *tree.DBytes:
		return pickFromCmp(bytes.Compare(
			[]byte(*at), []byte(*ub.(*tree.DBytes)),
		)), nil

	case *tree.DEncodedKey:
		return pickFromCmp(bytes.Compare(
			[]byte(*at), []byte(*ub.(*tree.DEncodedKey)),
		)), nil

	case *tree.DDate:
		return pickFromCmp(cmp.Compare(
			at.UnixEpochDaysWithOrig(),
			ub.(*tree.DDate).UnixEpochDaysWithOrig(),
		)), nil

	case *tree.DTime:
		return pickFromCmp(cmp.Compare(
			int64(*at), int64(*ub.(*tree.DTime)),
		)), nil

	case *tree.DTimeTZ:
		bt := ub.(*tree.DTimeTZ)
		c := cmp.Compare(at.TimeOfDay, bt.TimeOfDay)
		if c != 0 {
			return pickFromCmp(c), nil
		}
		return pickFromCmp(cmp.Compare(at.OffsetSecs, bt.OffsetSecs)), nil

	case *tree.DTimestamp:
		bt := ub.(*tree.DTimestamp)
		if at.Time.Before(bt.Time) {
			return ChooseA, nil
		}
		if at.Time.After(bt.Time) {
			return ChooseB, nil
		}
		return Equal, nil

	case *tree.DTimestampTZ:
		bt := ub.(*tree.DTimestampTZ)
		if at.Time.Before(bt.Time) {
			return ChooseA, nil
		}
		if at.Time.After(bt.Time) {
			return ChooseB, nil
		}
		return Equal, nil

	case *tree.DInterval:
		bt := ub.(*tree.DInterval)
		c := cmp.Compare(at.Months, bt.Months)
		if c != 0 {
			return pickFromCmp(c), nil
		}
		c = cmp.Compare(at.Days, bt.Days)
		if c != 0 {
			return pickFromCmp(c), nil
		}
		return pickFromCmp(cmp.Compare(at.Nanos(), bt.Nanos())), nil

	case *tree.DUuid:
		bt := ub.(*tree.DUuid)
		return pickFromCmp(bytes.Compare(at.UUID.GetBytes(), bt.UUID.GetBytes())), nil

	case *tree.DIPAddr:
		bt := ub.(*tree.DIPAddr)
		return pickFromCmp(at.IPAddr.Compare(&bt.IPAddr)), nil

	case *tree.DOid:
		return pickFromCmp(cmp.Compare(at.Oid, ub.(*tree.DOid).Oid)), nil

	case *tree.DJSON:
		bt := ub.(*tree.DJSON)
		c, err := at.JSON.Compare(bt.JSON)
		if err != nil {
			return Equal, err
		}
		if c != 0 {
			return pickFromCmp(c), nil
		}
		// JSON values that compare equal but could differ internally
		// (e.g. object key order). Compare their string forms.
		return pickFromCmp(cmp.Compare(
			at.JSON.String(), bt.JSON.String(),
		)), nil

	case *tree.DArray:
		bt := ub.(*tree.DArray)
		return pickArray(at.Array, bt.Array)

	case *tree.DTuple:
		bt := ub.(*tree.DTuple)
		return pickDatums(at.D, bt.D)

	case *tree.DEnum:
		bt := ub.(*tree.DEnum)
		return pickFromCmp(bytes.Compare(at.PhysicalRep, bt.PhysicalRep)), nil

	case *tree.DBox2D:
		bt := ub.(*tree.DBox2D)
		if c := pickFloat(at.LoX, bt.LoX); c != Equal {
			return c, nil
		}
		if c := pickFloat(at.HiX, bt.HiX); c != Equal {
			return c, nil
		}
		if c := pickFloat(at.LoY, bt.LoY); c != Equal {
			return c, nil
		}
		return pickFloat(at.HiY, bt.HiY), nil

	case *tree.DGeography:
		bt := ub.(*tree.DGeography)
		return pickFromCmp(bytes.Compare(
			at.SpatialObjectRef().EWKB, bt.SpatialObjectRef().EWKB,
		)), nil

	case *tree.DGeometry:
		bt := ub.(*tree.DGeometry)
		return pickFromCmp(bytes.Compare(
			at.SpatialObjectRef().EWKB, bt.SpatialObjectRef().EWKB,
		)), nil

	case *tree.DTSVector:
		bt := ub.(*tree.DTSVector)
		return pickFromCmp(cmp.Compare(
			at.TSVector.String(), bt.TSVector.String(),
		)), nil

	case *tree.DTSQuery:
		bt := ub.(*tree.DTSQuery)
		return pickFromCmp(cmp.Compare(
			at.TSQuery.String(), bt.TSQuery.String(),
		)), nil

	case *tree.DPGLSN:
		return pickFromCmp(cmp.Compare(
			uint64(at.LSN), uint64(ub.(*tree.DPGLSN).LSN),
		)), nil

	case *tree.DPGVector:
		bt := ub.(*tree.DPGVector)
		return pickVector(at.T, bt.T)

	case *tree.DLTree:
		bt := ub.(*tree.DLTree)
		return pickFromCmp(at.LTree.Compare(bt.LTree)), nil

	case *tree.DVoid:
		return Equal, nil

	case *tree.DBitArray:
		bt := ub.(*tree.DBitArray)
		return pickFromCmp(bitarray.Compare(at.BitArray, bt.BitArray)), nil

	default:
		return Equal, errors.AssertionFailedf(
			"lww tie-breaking: unhandled datum type %T", a,
		)
	}
}

// pickFloat compares two float64 values using their raw IEEE 754 bit
// patterns. This distinguishes -0.0 from 0.0 and provides a
// deterministic ordering for NaN values with different payloads.
func pickFloat(a, b float64) PickResult {
	return pickFromCmp(cmp.Compare(math.Float64bits(a), math.Float64bits(b)))
}

// pickArray compares two datum arrays element-wise using Pick,
// handling NULLs.
func pickArray(a, b tree.Datums) (PickResult, error) {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		result, err := pickNullable(a[i], b[i])
		if err != nil {
			return Equal, err
		}
		if result != Equal {
			return result, nil
		}
	}
	return pickFromCmp(cmp.Compare(len(a), len(b))), nil
}

// pickDatums compares two datum slices element-wise using Pick.
func pickDatums(a, b tree.Datums) (PickResult, error) {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		result, err := pickNullable(a[i], b[i])
		if err != nil {
			return Equal, err
		}
		if result != Equal {
			return result, nil
		}
	}
	return pickFromCmp(cmp.Compare(len(a), len(b))), nil
}

// pickNullable wraps Pick with NULL handling: NULL < non-NULL.
func pickNullable(a, b tree.Datum) (PickResult, error) {
	if a == tree.DNull && b == tree.DNull {
		return Equal, nil
	}
	if a == tree.DNull {
		return ChooseA, nil
	}
	if b == tree.DNull {
		return ChooseB, nil
	}
	return Pick(a, b)
}

// pickVector compares two float32 vectors element-wise using raw bit
// patterns, then by length.
func pickVector(a, b []float32) (PickResult, error) {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		c := cmp.Compare(
			math.Float32bits(a[i]), math.Float32bits(b[i]),
		)
		if c != 0 {
			return pickFromCmp(c), nil
		}
	}
	return pickFromCmp(cmp.Compare(len(a), len(b))), nil
}

// IsLwwWinner determines whether an incoming replicated write should
// win over an existing local row using Last-Write-Wins semantics.
//
// It first compares timestamps: if the incoming timestamp is strictly
// newer, the incoming write wins. If strictly older, it loses.
//
// When timestamps are equal, the tie is broken deterministically by
// comparing column values element-wise using Pick. NULLs are ordered
// before non-NULLs.
//
// Returns false when all values are identical to avoid duplicating
// KVs on replayed writes.
func IsLwwWinner(
	incomingTS, existingTS hlc.Timestamp, incoming, existing tree.Datums,
) (bool, error) {
	if existingTS.Less(incomingTS) {
		return true, nil
	}
	if incomingTS.Less(existingTS) {
		return false, nil
	}
	// Timestamps are equal — break the tie by comparing column values.
	for i := range incoming {
		result, err := pickNullable(incoming[i], existing[i])
		if err != nil {
			return false, err
		}
		switch result {
		case ChooseA:
			return true, nil
		case ChooseB:
			return false, nil
		}
	}
	return false, nil // all columns equal → no winner
}
