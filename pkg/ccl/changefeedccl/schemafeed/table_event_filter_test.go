// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed/schematestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestTableEventIsRegionalByRowChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	var (
		mkTableDesc    = schematestutils.MakeTableDesc
		addColBackfill = schematestutils.AddNewColumnBackfillMutation
		setRBR         = schematestutils.SetLocalityRegionalByRow
	)
	for _, c := range []struct {
		name string
		e    TableEvent
		exp  bool
	}{
		{
			name: "regional by row change",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  setRBR(mkTableDesc(42, 2, ts(3), 2, 1)),
			},
			exp: true,
		},
		{
			name: "add non-NULL column",
			e: TableEvent{
				Before: addColBackfill(mkTableDesc(42, 3, ts(2), 1, 1)),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "unknown table event",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  mkTableDesc(42, 1, ts(2), 2, 1),
			},
			exp: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equalf(t, c.exp, IsRegionalByRowChange(c.e), "event %v", c.e)
		})
	}
}

func TestTableEventIsPrimaryIndexChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	var (
		mkTableDesc     = schematestutils.MakeTableDesc
		addColBackfill  = schematestutils.AddNewColumnBackfillMutation
		dropColBackfill = schematestutils.AddColumnDropBackfillMutation
		addIdx          = schematestutils.AddNewIndexMutation
		pkSwap          = schematestutils.AddPrimaryKeySwapMutation
		dropIdx         = schematestutils.AddDropIndexMutation
	)
	for _, c := range []struct {
		name string
		e    TableEvent
		exp  bool
	}{
		{
			name: "primary index change",
			e: TableEvent{
				Before: pkSwap(addIdx(mkTableDesc(42, 1, ts(2), 2, 1))),
				After:  dropIdx(mkTableDesc(42, 2, ts(3), 2, 2)),
			},
			exp: true,
		},
		{
			name: "primary index change with column addition",
			e: TableEvent{
				Before: pkSwap(addIdx(addColBackfill(
					mkTableDesc(42, 1, ts(2), 1, 1),
				))),
				After: dropIdx(mkTableDesc(42, 2, ts(3), 2, 2)),
			},
			exp: true,
		},
		{
			name: "drop column",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  dropColBackfill(mkTableDesc(42, 2, ts(3), 1, 1)),
			},
			exp: false,
		},
		{
			name: "add non-NULL column",
			e: TableEvent{
				Before: addColBackfill(mkTableDesc(42, 3, ts(2), 1, 1)),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "add NULL-able computed column",
			e: TableEvent{
				Before: func() catalog.TableDescriptor {
					td := addColBackfill(mkTableDesc(42, 4, ts(4), 1, 1))
					col := td.TableDesc().Mutations[0].GetColumn()
					col.Nullable = true
					col.ComputeExpr = proto.String("1")
					return tabledesc.NewBuilder(td.TableDesc()).BuildImmutableTable()
				}(),
				After: mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "unknown table event",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  mkTableDesc(42, 1, ts(2), 2, 1),
			},
			exp: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, _ := IsPrimaryIndexChange(c.e, changefeedbase.Targets{})
			require.Equalf(t, c.exp, got, "event %v", c.e)
		})
	}
}

func TestTableEventIsOnlyPrimaryIndexChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	var (
		mkTableDesc     = schematestutils.MakeTableDesc
		addColBackfill  = schematestutils.AddNewColumnBackfillMutation
		dropColBackfill = schematestutils.AddColumnDropBackfillMutation
		addIdx          = schematestutils.AddNewIndexMutation
		pkSwap          = schematestutils.AddPrimaryKeySwapMutation
		dropIdx         = schematestutils.AddDropIndexMutation
	)
	for _, c := range []struct {
		name string
		e    TableEvent
		exp  bool
	}{
		{
			name: "primary index change",
			e: TableEvent{
				Before: pkSwap(addIdx(mkTableDesc(42, 1, ts(2), 2, 1))),
				After:  dropIdx(mkTableDesc(42, 2, ts(3), 2, 2)),
			},
			exp: true,
		},
		{
			name: "primary index change with column addition",
			e: TableEvent{
				Before: pkSwap(addIdx(addColBackfill(
					mkTableDesc(42, 1, ts(2), 1, 1),
				))),
				After: dropIdx(mkTableDesc(42, 2, ts(3), 2, 2)),
			},
			exp: false,
		},
		{
			name: "drop column",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  dropColBackfill(mkTableDesc(42, 2, ts(3), 1, 1)),
			},
			exp: false,
		},
		{
			name: "add non-NULL column",
			e: TableEvent{
				Before: addColBackfill(mkTableDesc(42, 3, ts(2), 1, 1)),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "add NULL-able computed column",
			e: TableEvent{
				Before: func() catalog.TableDescriptor {
					td := addColBackfill(mkTableDesc(42, 4, ts(4), 1, 1))
					col := td.TableDesc().Mutations[0].GetColumn()
					col.Nullable = true
					col.ComputeExpr = proto.String("1")
					return tabledesc.NewBuilder(td.TableDesc()).BuildImmutableTable()
				}(),
				After: mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "unknown table event",
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  mkTableDesc(42, 1, ts(2), 2, 1),
			},
			exp: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equalf(t, c.exp, IsOnlyPrimaryIndexChange(c.e), "event %v", c.e)
		})
	}
}

func TestTableEventFilterErrorsWithIncompletePolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	mkTableDesc := schematestutils.MakeTableDesc
	dropColBackfill := schematestutils.AddColumnDropBackfillMutation

	incompleteFilter := tableEventFilter{
		// tableEventDropColumn:            false,
		tableEventAddColumnWithBackfill: false,
		tableEventAddColumnNoBackfill:   true,
		// tableEventUnknown:               true,
		tableEventPrimaryKeyChange: false,
	}
	dropColEvent := TableEvent{
		Before: mkTableDesc(42, 1, ts(2), 2, 1),
		After:  dropColBackfill(mkTableDesc(42, 2, ts(3), 1, 1)),
	}
	changefeedTargets := CreateChangefeedTargets(42)

	_, err := incompleteFilter.shouldFilter(context.Background(), dropColEvent, changefeedTargets)
	require.Error(t, err)

	unknownEvent := TableEvent{
		Before: mkTableDesc(42, 1, ts(2), 2, 1),
		After:  mkTableDesc(42, 1, ts(2), 2, 1),
	}
	_, err = incompleteFilter.shouldFilter(context.Background(), unknownEvent, changefeedTargets)
	require.Error(t, err)

}

func TestTableEventFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	mkTableDesc := schematestutils.MakeTableDesc
	addColBackfill := schematestutils.AddNewColumnBackfillMutation
	dropColBackfill := schematestutils.AddColumnDropBackfillMutation
	setRBR := schematestutils.SetLocalityRegionalByRow
	for _, c := range []struct {
		name string
		p    tableEventFilter
		e    TableEvent
		exp  bool
	}{
		{
			name: "don't filter drop column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  dropColBackfill(mkTableDesc(42, 2, ts(3), 1, 1)),
			},
			exp: false,
		},
		{
			name: "filter first step of add non-NULL column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 1, 1),
				After:  addColBackfill(mkTableDesc(42, 2, ts(4), 1, 1)),
			},
			exp: true,
		},
		{
			name: "filter rollback of add column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: addColBackfill(mkTableDesc(42, 3, ts(2), 1, 1)),
				After:  mkTableDesc(42, 4, ts(4), 1, 1),
			},
			exp: true,
		},
		{
			name: "don't filter end of add non-NULL column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: addColBackfill(mkTableDesc(42, 3, ts(2), 1, 1)),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "don't filter end of add NULL-able visible computed column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: func() catalog.TableDescriptor {
					td := addColBackfill(mkTableDesc(42, 4, ts(4), 1, 1))
					col := td.TableDesc().Mutations[0].GetColumn()
					col.Nullable = true
					col.ComputeExpr = proto.String("1")
					return tabledesc.NewBuilder(td.TableDesc()).BuildImmutableTable()
				}(),
				After: mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "filter end of add NULL column",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 3, ts(2), 1, 1),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: true,
		},
		{
			name: "filter unknown table event",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  mkTableDesc(42, 1, ts(2), 2, 1),
			},
			exp: true,
		},
		{
			name: "don't filter regional by row change",
			p:    defaultTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  setRBR(mkTableDesc(42, 2, ts(3), 2, 1)),
			},
			exp: false,
		},
		{
			name: "columnChange - don't filter end of add NULL column",
			p:    columnChangeTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 3, ts(2), 1, 1),
				After:  mkTableDesc(42, 4, ts(4), 2, 1),
			},
			exp: false,
		},
		{
			name: "columnChange - don't filter regional by row change",
			p:    columnChangeTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  setRBR(mkTableDesc(42, 2, ts(3), 2, 1)),
			},
			exp: false,
		},
		{
			name: "columnChange - filter unknown table event",
			p:    columnChangeTableEventFilter,
			e: TableEvent{
				Before: mkTableDesc(42, 1, ts(2), 2, 1),
				After:  mkTableDesc(42, 1, ts(2), 2, 1),
			},
			exp: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			shouldFilter, err := c.p.shouldFilter(context.Background(), c.e, CreateChangefeedTargets(42))
			require.NoError(t, err)
			require.Equalf(t, c.exp, shouldFilter, "event %v", c.e)
		})
	}
}
