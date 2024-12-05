// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// StaticSQLInstanceID is the default Node ID to be used in tests.
const StaticSQLInstanceID = base.SQLInstanceID(3)

// RepeatableRowSource is a RowSource used in benchmarks to avoid having to
// reinitialize a new RowSource every time during multiple passes of the input.
// It is intended to be initialized with all rows.
type RepeatableRowSource struct {
	// The index of the next row to emit.
	nextRowIdx int
	rows       rowenc.EncDatumRows
	// Schema of rows.
	types []*types.T
}

var _ RowSource = &RepeatableRowSource{}

// NewRepeatableRowSource creates a RepeatableRowSource with the given schema
// and rows. types is optional if at least one row is provided.
func NewRepeatableRowSource(types []*types.T, rows rowenc.EncDatumRows) *RepeatableRowSource {
	if types == nil {
		panic("types required")
	}
	return &RepeatableRowSource{rows: rows, types: types}
}

// OutputTypes is part of the RowSource interface.
func (r *RepeatableRowSource) OutputTypes() []*types.T {
	return r.types
}

// Start is part of the RowSource interface.
func (r *RepeatableRowSource) Start(ctx context.Context) {}

// Next is part of the RowSource interface.
func (r *RepeatableRowSource) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
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

// NewTestMemMonitor creates and starts a new memory monitor to be used in
// tests.
// TODO(yuzefovich): consider reusing this in tree.MakeTestingEvalContext
// (currently it would create an import cycle, so this code will need to be
// moved).
func NewTestMemMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	memMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-mem"),
		Settings: st,
	})
	memMonitor.Start(ctx, nil, mon.NewStandaloneBudget(math.MaxInt64))
	return memMonitor
}

// NewTestDiskMonitor creates and starts a new disk monitor to be used in
// tests.
func NewTestDiskMonitor(ctx context.Context, st *cluster.Settings) *mon.BytesMonitor {
	diskMonitor := mon.NewMonitor(mon.Options{
		Name:     mon.MakeMonitorName("test-disk"),
		Res:      mon.DiskResource,
		Settings: st,
	})
	diskMonitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
	return diskMonitor
}

// GenerateValuesSpec generates a ValuesCoreSpec that encodes the given rows.
// We pass the types as well because zero rows are allowed.
func GenerateValuesSpec(
	colTypes []*types.T, rows rowenc.EncDatumRows,
) (execinfrapb.ValuesCoreSpec, error) {
	var spec execinfrapb.ValuesCoreSpec
	spec.Columns = make([]execinfrapb.DatumInfo, len(colTypes))
	for i := range spec.Columns {
		spec.Columns[i].Type = colTypes[i]
		spec.Columns[i].Encoding = catenumpb.DatumEncoding_VALUE
	}

	spec.NumRows = uint64(len(rows))
	if len(colTypes) != 0 {
		var a tree.DatumAlloc
		for i := 0; i < len(rows); i++ {
			var buf []byte
			for j, info := range spec.Columns {
				var err error
				buf, err = rows[i][j].Encode(colTypes[j], &a, info.Encoding, buf)
				if err != nil {
					return execinfrapb.ValuesCoreSpec{}, err
				}
			}
			spec.RawBytes = append(spec.RawBytes, buf)
		}
	}
	return spec, nil
}
