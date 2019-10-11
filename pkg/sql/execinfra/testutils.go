// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// StaticNodeID is the default Node ID to be used in tests.
const StaticNodeID = roachpb.NodeID(3)

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       sqlbase.EncDatumRows
	// Schema of rows.
	types []types.T
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(types []types.T, rows sqlbase.EncDatumRows) *RepeatableRowSource {
	if types == nil {
		panic("types required")
	}
	return &RepeatableRowSource{rows: rows, types: types}
}

// OutputTypes is part of the RowSource interface.
func (r *RepeatableRowSource) OutputTypes() []types.T {
	return r.types
}

// Start is part of the RowSource interface.
func (r *RepeatableRowSource) Start(ctx context.Context) context.Context { return ctx }

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	// If we've emitted all rows, signal that we have reached the end.
	if r.nextRowIdx >= len(r.rows) {
		return nil, nil
	}
	nextRow := r.rows[r.nextRowIdx]
	r.nextRowIdx++
	return nextRow, nil
}

// Reset resets the RepeatableRowSource such that a subsequent call to Next()
// returns the first row.
func (r *RepeatableRowSource) Reset() {
	r.nextRowIdx = 0
}

// ConsumerDone is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerDone() {}

// ConsumerClosed is part of the RowSource interface.
func (r *RepeatableRowSource) ConsumerClosed() {}

// MakeTestDiskMonitor creates a new disk monitor to be used in tests.
func MakeTestDiskMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	diskMonitor := mon.MakeMonitor(
		"test-disk",
		mon.DiskResource,
		nil, /* curCount */
		nil, /* maxHist */
		-1,  /* increment: use default block size */
		math.MaxInt64,
		st,
	)
	diskMonitor.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	return &diskMonitor
}

// GenerateValuesSpec generates a ValuesCoreSpec that encodes the given rows.
// We pass the types as well because zero rows are allowed.
func GenerateValuesSpec(
	colTypes []types.T, rows sqlbase.EncDatumRows, rowsPerChunk int,
) (execinfrapb.ValuesCoreSpec, error) {
	var spec execinfrapb.ValuesCoreSpec
	spec.Columns = make([]execinfrapb.DatumInfo, len(colTypes))
	for i := range spec.Columns {
		spec.Columns[i].Type = colTypes[i]
		spec.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
	}

	var a sqlbase.DatumAlloc
	for i := 0; i < len(rows); {
		var buf []byte
		for end := i + rowsPerChunk; i < len(rows) && i < end; i++ {
			for j, info := range spec.Columns {
				var err error
				buf, err = rows[i][j].Encode(&colTypes[j], &a, info.Encoding, buf)
				if err != nil {
					return execinfrapb.ValuesCoreSpec{}, err
				}
			}
		}
		spec.RawBytes = append(spec.RawBytes, buf)
	}
	return spec, nil
}

// RowDisposer is a RowReceiver that discards any rows Push()ed.
type RowDisposer struct{}

var _ RowReceiver = &RowDisposer{}

// Push is part of the distsql.RowReceiver interface.
func (r *RowDisposer) Push(
	row sqlbase.EncDatumRow, meta *execinfrapb.ProducerMetadata,
) ConsumerStatus {
	return NeedMoreRows
}

// ProducerDone is part of the RowReceiver interface.
func (r *RowDisposer) ProducerDone() {}

// Types is part of the RowReceiver interface.
func (r *RowDisposer) Types() []types.T {
	return nil
}

// GenerateEqualityColumns produces a random permutation of nEqCols random
// columns on a table with nCols columns, so nEqCols must be not greater than
// nCols.
func GenerateEqualityColumns(rng *rand.Rand, nCols int, nEqCols int) []uint32 {
	if nEqCols > nCols {
		panic("nEqCols > nCols in GenerateEqualityColumns")
	}
	eqCols := make([]uint32, 0, nEqCols)
	for _, eqCol := range rng.Perm(nCols)[:nEqCols] {
		eqCols = append(eqCols, uint32(eqCol))
	}
	return eqCols
}

// GenerateOnExpr populates an execinfrapb.Expression that contains a single
// comparison which can be either comparing a column from the left against a
// column from the right or comparing a column from either side against a
// constant.
// If forceConstComparison is true, then the comparison against the constant
// will be used.
func GenerateOnExpr(
	rng *rand.Rand, nCols int, nEqCols int, colTypes []types.T, forceConstComparison bool,
) execinfrapb.Expression {
	var comparison string
	r := rng.Float64()
	if r < 0.25 {
		comparison = "<"
	} else if r < 0.5 {
		comparison = ">"
	} else if r < 0.75 {
		comparison = "="
	} else {
		comparison = "<>"
	}
	// When all columns are used in equality comparison between inputs, there is
	// only one interesting case when a column from either side is compared
	// against a constant. The second conditional is us choosing to compare
	// against a constant.
	if nCols == nEqCols || rng.Float64() < 0.33 || forceConstComparison {
		colIdx := rng.Intn(nCols)
		if rng.Float64() >= 0.5 {
			// Use right side.
			colIdx += nCols
		}
		constDatum := sqlbase.RandDatum(rng, &colTypes[colIdx], true /* nullOk */)
		constDatumString := constDatum.String()
		if strings.Contains(constDatumString, "NaN") || strings.Contains(constDatumString, "Inf") {
			// We need to surround special values with quotes.
			constDatumString = fmt.Sprintf("'%s'", constDatumString)
		}
		return execinfrapb.Expression{Expr: fmt.Sprintf("@%d %s %s", colIdx+1, comparison, constDatumString)}
	}
	// We will compare a column from the left against a column from the right.
	leftColIdx := rng.Intn(nCols) + 1
	rightColIdx := rng.Intn(nCols) + nCols + 1
	return execinfrapb.Expression{Expr: fmt.Sprintf("@%d %s @%d", leftColIdx, comparison, rightColIdx)}
}
