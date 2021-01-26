// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stats

import (
	"context"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// JSONStatistic is a struct used for JSON marshaling and unmarshaling statistics.
//
// See TableStatistic for a description of the fields.
type JSONStatistic struct {
	Name          string   `json:"name,omitempty"`
	CreatedAt     string   `json:"created_at"`
	Columns       []string `json:"columns"`
	RowCount      uint64   `json:"row_count"`
	DistinctCount uint64   `json:"distinct_count"`
	NullCount     uint64   `json:"null_count"`
	// HistogramColumnType is the string representation of the column type for the
	// histogram (or unset if there is no histogram). Parsable with
	// tree.GetTypeFromValidSQLSyntax.
	HistogramColumnType string            `json:"histo_col_type"`
	HistogramBuckets    []JSONHistoBucket `json:"histo_buckets,omitempty"`
}

// JSONHistoBucket is a struct used for JSON marshaling and unmarshaling of
// histogram data.
//
// See HistogramData for a description of the fields.
type JSONHistoBucket struct {
	NumEq         int64   `json:"num_eq"`
	NumRange      int64   `json:"num_range"`
	DistinctRange float64 `json:"distinct_range"`
	// UpperBound is the string representation of a datum; parsable with
	// sqlbase.ParseDatumStringAs.
	UpperBound string `json:"upper_bound"`
}

// SetHistogram fills in the HistogramColumnType and HistogramBuckets fields.
func (js *JSONStatistic) SetHistogram(h *HistogramData) error {
	typ := h.ColumnType
	if typ == nil {
		return fmt.Errorf("histogram type is unset")
	}
	js.HistogramColumnType = typ.SQLString()
	js.HistogramBuckets = make([]JSONHistoBucket, len(h.Buckets))
	var a rowenc.DatumAlloc
	for i := range h.Buckets {
		b := &h.Buckets[i]
		js.HistogramBuckets[i].NumEq = b.NumEq
		js.HistogramBuckets[i].NumRange = b.NumRange
		js.HistogramBuckets[i].DistinctRange = b.DistinctRange

		if b.UpperBound == nil {
			return fmt.Errorf("histogram bucket upper bound is unset")
		}
		datum, _, err := rowenc.DecodeTableKey(&a, typ, b.UpperBound, encoding.Ascending)
		if err != nil {
			return err
		}

		js.HistogramBuckets[i] = JSONHistoBucket{
			NumEq:         b.NumEq,
			NumRange:      b.NumRange,
			DistinctRange: b.DistinctRange,
			UpperBound:    tree.AsStringWithFlags(datum, tree.FmtExport),
		}
	}
	return nil
}

// DecodeAndSetHistogram decodes a histogram marshaled as a Bytes datum and
// fills in the HistogramColumnType and HistogramBuckets fields.
func (js *JSONStatistic) DecodeAndSetHistogram(
	ctx context.Context, semaCtx *tree.SemaContext, datum tree.Datum,
) error {
	if datum == tree.DNull {
		return nil
	}
	if datum.ResolvedType().Family() != types.BytesFamily {
		return fmt.Errorf("histogram datum type should be Bytes")
	}
	if len(*datum.(*tree.DBytes)) == 0 {
		// This can happen if every value in the column is null.
		return nil
	}
	h := &HistogramData{}
	if err := protoutil.Unmarshal([]byte(*datum.(*tree.DBytes)), h); err != nil {
		return err
	}
	// If the serialized column type is user defined, then it needs to be
	// hydrated before use.
	if h.ColumnType.UserDefined() {
		resolver := semaCtx.GetTypeResolver()
		if resolver == nil {
			return errors.AssertionFailedf("attempt to resolve user defined type with nil TypeResolver")
		}
		typ, err := resolver.ResolveTypeByOID(ctx, h.ColumnType.Oid())
		if err != nil {
			return err
		}
		h.ColumnType = typ
	}
	return js.SetHistogram(h)
}

// GetHistogram converts the json histogram into HistogramData.
func (js *JSONStatistic) GetHistogram(
	semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) (*HistogramData, error) {
	if len(js.HistogramBuckets) == 0 {
		return nil, nil
	}
	h := &HistogramData{}
	colTypeRef, err := parser.GetTypeFromValidSQLSyntax(js.HistogramColumnType)
	if err != nil {
		return nil, err
	}
	colType, err := tree.ResolveType(evalCtx.Context, colTypeRef, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, err
	}
	h.ColumnType = colType
	h.Buckets = make([]HistogramData_Bucket, len(js.HistogramBuckets))
	for i := range h.Buckets {
		hb := &js.HistogramBuckets[i]
		upperVal, err := rowenc.ParseDatumStringAs(colType, hb.UpperBound, evalCtx)
		if err != nil {
			return nil, err
		}
		h.Buckets[i].NumEq = hb.NumEq
		h.Buckets[i].NumRange = hb.NumRange
		h.Buckets[i].DistinctRange = hb.DistinctRange
		h.Buckets[i].UpperBound, err = rowenc.EncodeTableKey(nil, upperVal, encoding.Ascending)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}
