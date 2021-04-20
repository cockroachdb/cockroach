// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/distsqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestOrderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [6]rowenc.EncDatum{}
	for i := range v {
		v[i] = rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
	}

	asc := encoding.Ascending
	desc := encoding.Descending

	testCases := []struct {
		sources  []rowenc.EncDatumRows
		ordering colinfo.ColumnOrdering
		expected rowenc.EncDatumRows
	}{
		{
			sources: []rowenc.EncDatumRows{
				{
					{v[0], v[1], v[4]},
					{v[0], v[1], v[2]},
					{v[0], v[2], v[3]},
					{v[1], v[1], v[3]},
				},
				{
					{v[1], v[0], v[4]},
				},
				{
					{v[0], v[0], v[0]},
					{v[4], v[4], v[4]},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 0, Direction: asc},
				{ColIdx: 1, Direction: asc},
			},
			expected: rowenc.EncDatumRows{
				{v[0], v[0], v[0]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[2]},
				{v[0], v[2], v[3]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[3]},
				{v[4], v[4], v[4]},
			},
		},
		{
			sources: []rowenc.EncDatumRows{
				{},
				{
					{v[1], v[0], v[4]},
				},
				{
					{v[3], v[4], v[1]},
					{v[4], v[4], v[4]},
					{v[3], v[2], v[0]},
				},
				{
					{v[4], v[4], v[5]},
					{v[3], v[3], v[0]},
					{v[0], v[0], v[0]},
				},
			},
			ordering: colinfo.ColumnOrdering{
				{ColIdx: 1, Direction: desc},
				{ColIdx: 0, Direction: asc},
				{ColIdx: 2, Direction: asc},
			},
			expected: rowenc.EncDatumRows{
				{v[3], v[4], v[1]},
				{v[4], v[4], v[4]},
				{v[4], v[4], v[5]},
				{v[3], v[3], v[0]},
				{v[3], v[2], v[0]},
				{v[0], v[0], v[0]},
				{v[1], v[0], v[4]},
			},
		},
	}
	for testIdx, c := range testCases {
		var sources []execinfra.RowSource
		for _, srcRows := range c.sources {
			rowBuf := distsqlutils.NewRowBuffer(types.ThreeIntCols, srcRows, distsqlutils.RowBufferArgs{})
			sources = append(sources, rowBuf)
		}
		evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
		defer evalCtx.Stop(context.Background())
		src, err := makeSerialSync(c.ordering, evalCtx, sources)
		if err != nil {
			t.Fatal(err)
		}
		src.Start(context.Background())
		var retRows rowenc.EncDatumRows
		for {
			row, meta := src.Next()
			if meta != nil {
				t.Fatalf("unexpected metadata: %v", meta)
			}
			if row == nil {
				break
			}
			retRows = append(retRows, row)
		}
		expStr := c.expected.String(types.ThreeIntCols)
		retStr := retRows.String(types.ThreeIntCols)
		if expStr != retStr {
			t.Errorf("invalid results for case %d; expected:\n   %s\ngot:\n   %s",
				testIdx, expStr, retStr)
		}
	}
}

func TestOrderedSyncDrainBeforeNext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	expectedMeta := &execinfrapb.ProducerMetadata{Err: errors.New("expected metadata")}

	var sources []execinfra.RowSource
	for i := 0; i < 4; i++ {
		rowBuf := distsqlutils.NewRowBuffer(types.OneIntCol, nil /* rows */, distsqlutils.RowBufferArgs{})
		sources = append(sources, rowBuf)
		rowBuf.Push(nil, expectedMeta)
	}

	ctx := context.Background()
	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(ctx)
	o, err := makeSerialSync(colinfo.ColumnOrdering{}, evalCtx, sources)
	if err != nil {
		t.Fatal(err)
	}
	o.Start(ctx)

	// Call ConsumerDone before Next has been called.
	o.ConsumerDone()

	metasFound := 0
	for {
		_, meta := o.Next()
		if meta == nil {
			break
		}

		if meta != expectedMeta {
			t.Fatalf("unexpected meta %v, expected %v", meta, expectedMeta)
		}

		metasFound++
	}
	if metasFound != len(sources) {
		t.Fatalf("unexpected number of metadata items %d, expected %d", metasFound, len(sources))
	}
}

func TestUnorderedSync(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mrc := &execinfra.RowChannel{}
	mrc.InitWithNumSenders([]*types.T{types.Int}, 5)
	producerErr := make(chan error, 100)
	for i := 1; i <= 5; i++ {
		go func(i int) {
			for j := 1; j <= 100; j++ {
				a := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
				b := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(j)))
				row := rowenc.EncDatumRow{a, b}
				if status := mrc.Push(row, nil /* meta */); status != execinfra.NeedMoreRows {
					producerErr <- errors.Errorf("producer error: unexpected response: %d", status)
				}
			}
			mrc.ProducerDone()
		}(i)
	}
	var retRows rowenc.EncDatumRows
	for {
		row, meta := mrc.Next()
		if meta != nil {
			t.Fatalf("unexpected metadata: %v", meta)
		}
		if row == nil {
			break
		}
		retRows = append(retRows, row)
	}
	// Verify all elements.
	for i := 1; i <= 5; i++ {
		j := 1
		for _, row := range retRows {
			if int(tree.MustBeDInt(row[0].Datum)) == i {
				if int(tree.MustBeDInt(row[1].Datum)) != j {
					t.Errorf("Expected [%d %d], got %s", i, j, row.String(types.TwoIntCols))
				}
				j++
			}
		}
		if j != 101 {
			t.Errorf("Missing [%d %d]", i, j)
		}
	}
	select {
	case err := <-producerErr:
		t.Fatal(err)
	default:
	}

	// Test case when one source closes with an error.
	mrc = &execinfra.RowChannel{}
	mrc.InitWithNumSenders([]*types.T{types.Int}, 5)
	for i := 1; i <= 5; i++ {
		go func(i int) {
			for j := 1; j <= 100; j++ {
				a := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(i)))
				b := rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(j)))
				row := rowenc.EncDatumRow{a, b}
				if status := mrc.Push(row, nil /* meta */); status != execinfra.NeedMoreRows {
					producerErr <- errors.Errorf("producer error: unexpected response: %d", status)
				}
			}
			if i == 3 {
				err := fmt.Errorf("Test error")
				mrc.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: err})
			}
			mrc.ProducerDone()
		}(i)
	}
	foundErr := false
	for {
		row, meta := mrc.Next()
		if meta != nil && meta.Err != nil {
			if meta.Err.Error() != "Test error" {
				t.Error(meta.Err)
			} else {
				foundErr = true
			}
		}
		if row == nil && meta == nil {
			break
		}
	}
	select {
	case err := <-producerErr:
		t.Fatal(err)
	default:
	}
	if !foundErr {
		t.Error("Did not receive expected error")
	}
}
