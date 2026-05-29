// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlog_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/revlog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSchemaDescRoundTrip writes a descriptor and a tombstone at
// distinct (HLC, desc-id) pairs and reads each back.
func TestSchemaDescRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newCoverageTestStorage(t)

	desc := &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{ID: 1}}}
	require.NoError(t, revlog.WriteSchemaDesc(ctx, es, tsAt(100), 7, desc))
	require.NoError(t, revlog.WriteSchemaDesc(ctx, es, tsAt(200), 7, nil /* tombstone */))

	got, err := revlog.ReadSchemaDesc(ctx, es, revlog.SchemaDescPath(tsAt(100), 7))
	require.NoError(t, err)
	require.NotNil(t, got)

	tomb, err := revlog.ReadSchemaDesc(ctx, es, revlog.SchemaDescPath(tsAt(200), 7))
	require.NoError(t, err)
	require.Nil(t, tomb, "tombstone should decode as nil descriptor")
}

// TestIterSchemaChangesWindow drops three descriptor changes
// across distinct HLCs and confirms IterSchemaChanges respects
// the (start, end] window and yields in (HLC, descID) order.
func TestIterSchemaChangesWindow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	es := newCoverageTestStorage(t)

	// Three changes: at HLC 100/desc 5, 100/desc 8, 200/desc 5.
	require.NoError(t, revlog.WriteSchemaDesc(ctx, es, tsAt(100), 5, &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{ID: 1}}}))
	require.NoError(t, revlog.WriteSchemaDesc(ctx, es, tsAt(100), 8, &descpb.Descriptor{Union: &descpb.Descriptor_Table{Table: &descpb.TableDescriptor{ID: 1}}}))
	require.NoError(t, revlog.WriteSchemaDesc(ctx, es, tsAt(200), 5, nil /* tombstone */))

	type entry struct {
		ChangedAt hlc.Timestamp
		DescID    descpb.ID
		IsTomb    bool
	}
	collect := func(start, end hlc.Timestamp) []entry {
		t.Helper()
		var got []entry
		for c, err := range revlog.IterSchemaChanges(ctx, es, start, end) {
			require.NoError(t, err)
			got = append(got, entry{
				ChangedAt: c.ChangedAt,
				DescID:    c.DescID,
				IsTomb:    c.Descriptor == nil,
			})
		}
		return got
	}

	require.Equal(t, []entry{
		{ChangedAt: tsAt(100), DescID: 5},
		{ChangedAt: tsAt(100), DescID: 8},
		{ChangedAt: tsAt(200), DescID: 5, IsTomb: true},
	}, collect(tsAt(50), tsAt(200)))

	require.Equal(t, []entry{
		{ChangedAt: tsAt(200), DescID: 5, IsTomb: true},
	}, collect(tsAt(100), tsAt(200)), "(100, 200] excludes the HLC-100 entries")

	require.Empty(t, collect(tsAt(200), tsAt(300)),
		"(200, 300] excludes the HLC-200 entry")
}
