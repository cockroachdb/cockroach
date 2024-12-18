// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// JSONStatistic is a struct used for JSON marshaling and unmarshaling statistics.
//
// See TableStatistic for a description of the fields.
type JSONStatistic struct {
	ID            uint64   `json:"id,omitempty"`
	Name          string   `json:"name,omitempty"`
	CreatedAt     string   `json:"created_at"`
	Columns       []string `json:"columns"`
	RowCount      uint64   `json:"row_count"`
	DistinctCount uint64   `json:"distinct_count"`
	NullCount     uint64   `json:"null_count"`
	AvgSize       uint64   `json:"avg_size"`
	// HistogramColumnType is the string representation of the column type for the
	// histogram (or unset if there is no histogram). Parsable with
	// tree.GetTypeFromValidSQLSyntax.
	HistogramColumnType string            `json:"histo_col_type"`
	HistogramBuckets    []JSONHistoBucket `json:"histo_buckets,omitempty"`
	HistogramVersion    HistogramVersion  `json:"histo_version,omitempty"`
	PartialPredicate    string            `json:"partial_predicate,omitempty"`
	FullStatisticID     uint64            `json:"full_statistic_id,omitempty"`
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
	// Use the fully qualified type name in case this is part of injected stats
	// done across databases. If it is a user-defined type, we need the type name
	// resolution to be for the correct database.
	js.HistogramColumnType = typ.SQLStringFullyQualified()
	js.HistogramBuckets = make([]JSONHistoBucket, 0, len(h.Buckets))
	js.HistogramVersion = h.Version
	var a tree.DatumAlloc
	for i := range h.Buckets {
		b := &h.Buckets[i]
		if b.UpperBound == nil {
			return fmt.Errorf("histogram bucket upper bound is unset")
		}
		datum, err := DecodeUpperBound(h.Version, typ, &a, b.UpperBound)
		if err != nil {
			if h.ColumnType.Family() == types.EnumFamily && errors.Is(err, types.EnumValueNotFound) {
				// Skip over buckets for enum values that were dropped.
				continue
			}
			return err
		}

		js.HistogramBuckets = append(js.HistogramBuckets, JSONHistoBucket{
			NumEq:         b.NumEq,
			NumRange:      b.NumRange,
			DistinctRange: b.DistinctRange,
			UpperBound:    tree.AsStringWithFlags(datum, tree.FmtExport|tree.FmtAlwaysQualifyUserDefinedTypeNames),
		})
	}
	return nil
}

// DecodeAndSetHistogram decodes a histogram marshaled as a Bytes datum and
// fills in the JSONStatistic histogram fields.
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
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context,
) (*HistogramData, error) {
	if js.HistogramColumnType == "" {
		return nil, nil
	}
	h := &HistogramData{}
	colTypeRef, err := parser.GetTypeFromValidSQLSyntax(js.HistogramColumnType)
	if err != nil {
		return nil, err
	}
	colType, err := tree.ResolveType(ctx, colTypeRef, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, err
	}
	h.ColumnType = colType
	h.Version = js.HistogramVersion
	h.Buckets = make([]HistogramData_Bucket, len(js.HistogramBuckets))
	for i := range h.Buckets {
		hb := &js.HistogramBuckets[i]
		upperVal, err := rowenc.ParseDatumStringAs(ctx, colType, hb.UpperBound, evalCtx, semaCtx)
		if err != nil {
			return nil, err
		}
		h.Buckets[i].NumEq = hb.NumEq
		h.Buckets[i].NumRange = hb.NumRange
		h.Buckets[i].DistinctRange = hb.DistinctRange
		h.Buckets[i].UpperBound, err = EncodeUpperBound(h.Version, upperVal)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

// IsPartial returns true if this statistic was collected with a where clause.
func (js *JSONStatistic) IsPartial() bool {
	return js.PartialPredicate != ""
}

// IsMerged returns true if this statistic was created by merging a partial and
// a full statistic.
func (js *JSONStatistic) IsMerged() bool {
	return js.Name == jobspb.MergedStatsName
}

// IsForecast returns true if this statistic was created by forecasting.
func (js *JSONStatistic) IsForecast() bool {
	return js.Name == jobspb.ForecastStatsName
}

// IsAuto returns true if this statistic was collected automatically.
func (js *JSONStatistic) IsAuto() bool {
	return js.Name == jobspb.AutoStatsName
}
