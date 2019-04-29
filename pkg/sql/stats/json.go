// Copyright 2017 The Cockroach Authors.
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

package stats

import (
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
	// tree.ParseType.
	HistogramColumnType string            `json:"histo_col_type"`
	HistogramBuckets    []JSONHistoBucket `json:"histo_buckets,omitempty"`
}

// JSONHistoBucket is a struct used for JSON marshaling and unmarshaling of
// histogram data.
//
// See HistogramData for a description of the fields.
type JSONHistoBucket struct {
	NumEq    int64 `json:"num_eq"`
	NumRange int64 `json:"num_range"`
	// UpperBound is the string representation of a datum; parsable with
	// tree.ParseStringAs.
	UpperBound string `json:"upper_bound"`
}

// SetHistogram fills in the HistogramColumnType and HistogramBuckets fields.
func (js *JSONStatistic) SetHistogram(h *HistogramData) error {
	typ := &h.ColumnType
	js.HistogramColumnType = typ.String()
	js.HistogramBuckets = make([]JSONHistoBucket, len(h.Buckets))
	var a sqlbase.DatumAlloc
	for i := range h.Buckets {
		b := &h.Buckets[i]
		js.HistogramBuckets[i].NumEq = b.NumEq
		js.HistogramBuckets[i].NumRange = b.NumRange

		datum, _, err := sqlbase.DecodeTableKey(&a, typ, b.UpperBound, encoding.Ascending)
		if err != nil {
			return err
		}

		js.HistogramBuckets[i] = JSONHistoBucket{
			NumEq:      b.NumEq,
			NumRange:   b.NumRange,
			UpperBound: datum.String(),
		}
	}
	return nil
}

// DecodeAndSetHistogram decodes a histogram marshaled as a Bytes datum and
// fills in the HistogramColumnType and HistogramBuckets fields.
func (js *JSONStatistic) DecodeAndSetHistogram(datum tree.Datum) error {
	if datum == tree.DNull {
		return nil
	}
	if datum.ResolvedType().Family() != types.BytesFamily {
		return fmt.Errorf("histogram datum type should be Bytes")
	}
	h := &HistogramData{}
	if err := protoutil.Unmarshal([]byte(*datum.(*tree.DBytes)), h); err != nil {
		return err
	}
	return js.SetHistogram(h)
}

// GetHistogram converts the json histogram into HistogramData.
func (js *JSONStatistic) GetHistogram(evalCtx *tree.EvalContext) (*HistogramData, error) {
	if len(js.HistogramBuckets) == 0 {
		return nil, nil
	}
	h := &HistogramData{}
	colType, err := parser.ParseType(js.HistogramColumnType)
	if err != nil {
		return nil, err
	}
	h.ColumnType = *colType
	if err != nil {
		return nil, err
	}
	h.Buckets = make([]HistogramData_Bucket, len(js.HistogramBuckets))
	for i := range h.Buckets {
		hb := &js.HistogramBuckets[i]
		upperVal, err := tree.ParseStringAs(colType, hb.UpperBound, evalCtx)
		if err != nil {
			return nil, err
		}
		h.Buckets[i].NumEq = hb.NumEq
		h.Buckets[i].NumRange = hb.NumRange
		h.Buckets[i].UpperBound, err = sqlbase.EncodeTableKey(nil, upperVal, encoding.Ascending)
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}
