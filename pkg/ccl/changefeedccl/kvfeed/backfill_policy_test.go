// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestBackfillPolicy(t *testing.T) {
	ts := func(seconds int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: (time.Duration(seconds) * time.Second).Nanoseconds()}
	}
	for _, c := range []struct {
		name string
		p    backfillPolicy
		e    schemafeed.TableEvent
		exp  bool
	}{
		{
			name: "don't filter drop column",
			p:    defaultBackfillPolicy,
			e: schemafeed.TableEvent{
				Before: makeTableDesc(42, 1, ts(2), 2),
				After:  addColumnDropBackfillMutation(makeTableDesc(42, 2, ts(3), 1)),
			},
			exp: false,
		},
		{
			name: "filter first step of add column",
			p:    defaultBackfillPolicy,
			e: schemafeed.TableEvent{
				Before: makeTableDesc(42, 1, ts(2), 1),
				After:  addNewColumnBackfillMutation(makeTableDesc(42, 2, ts(4), 1)),
			},
			exp: true,
		},
		{
			name: "filter rollback of add column",
			p:    defaultBackfillPolicy,
			e: schemafeed.TableEvent{
				Before: addNewColumnBackfillMutation(makeTableDesc(42, 3, ts(2), 1)),
				After:  makeTableDesc(42, 4, ts(4), 1),
			},
			exp: true,
		},
		{
			name: "don't filter end of add column",
			p:    defaultBackfillPolicy,
			e: schemafeed.TableEvent{
				Before: addNewColumnBackfillMutation(makeTableDesc(42, 3, ts(2), 1)),
				After:  makeTableDesc(42, 4, ts(4), 2),
			},
			exp: false,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			shouldFilter, err := c.p.ShouldFilter(context.Background(), c.e)
			require.NoError(t, err)
			require.Equalf(t, c.exp, shouldFilter, "event %v", c.e)
		})

	}
}

func mkColumnDesc(id sqlbase.ColumnID) *sqlbase.ColumnDescriptor {
	return &sqlbase.ColumnDescriptor{
		Name:        "c" + strconv.Itoa(int(id)),
		ID:          id,
		Type:        *types.Bool,
		DefaultExpr: proto.String("true"),
	}
}
func makeTableDesc(
	tableID sqlbase.ID, version sqlbase.DescriptorVersion, modTime hlc.Timestamp, cols int,
) *sqlbase.TableDescriptor {
	td := &sqlbase.TableDescriptor{
		Name:             "foo",
		ID:               tableID,
		Version:          version,
		ModificationTime: modTime,
		NextColumnID:     1,
	}
	for i := 0; i < cols; i++ {
		td.Columns = append(td.Columns, *mkColumnDesc(td.NextColumnID))
		td.NextColumnID++
	}
	return td
}
func addColumnDropBackfillMutation(desc *sqlbase.TableDescriptor) *sqlbase.TableDescriptor {
	desc.Mutations = append(desc.Mutations, sqlbase.DescriptorMutation{
		State:     sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction: sqlbase.DescriptorMutation_DROP,
	})
	return desc
}
func addNewColumnBackfillMutation(desc *sqlbase.TableDescriptor) *sqlbase.TableDescriptor {
	desc.Mutations = append(desc.Mutations, sqlbase.DescriptorMutation{
		Descriptor_: &sqlbase.DescriptorMutation_Column{Column: mkColumnDesc(desc.NextColumnID)},
		State:       sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		Direction:   sqlbase.DescriptorMutation_ADD,
		MutationID:  0,
		Rollback:    false,
	})
	return desc
}
