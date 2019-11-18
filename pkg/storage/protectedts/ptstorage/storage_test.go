// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Start small: create a record, get it etc

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	p := ptstorage.New(s.ClusterSettings(),
		s.InternalExecutor().(*sql.InternalExecutor))
	ts := s.Clock().Now()

	meta, err := p.GetMetadata(ctx, nil /* txn */)
	require.NoError(t, err)
	var emptyMeta ptpb.Metadata
	require.Equal(t, emptyMeta, meta)
	state, err := p.GetState(ctx, nil /* txn */)
	require.NoError(t, err)
	require.EqualValues(t, ptpb.State{}, state)

	// Protect a span.
	r := ptpb.NewRecord(ts, ptpb.PROTECT_AT, "", nil, roachpb.Span{
		Key:    keys.MakeTablePrefix(42),
		EndKey: roachpb.Key(keys.MakeTablePrefix(42)).PrefixEnd(),
	})
	require.NoError(t, p.Protect(ctx, nil /* txn */, &r))
	// Creating the record again should fail.
	require.Equal(t, protectedts.ErrExists, p.Protect(ctx, nil /* txn */, &r))

	// Make sure we read it.
	read, err := p.GetRecord(ctx, nil /* txn */, r.ID)
	require.NoError(t, err)
	require.EqualValues(t, r, *read)

	// Make sure that the metadata has been updated to reflect the state.
	state, err = p.GetState(ctx, nil /* txn */)
	require.NoError(t, err)
	require.Equal(t, ptpb.Metadata{Version: 1, NumRecords: 1, NumSpans: 1, TotalBytes: 8}, state.Metadata)
	require.EqualValues(t, []ptpb.Record{r}, state.Records)

	// Release the span.
	require.NoError(t, p.Release(ctx, nil /* txn */, r.ID))
	_, err = p.GetRecord(ctx, nil /* txn */, r.ID)
	require.EqualError(t, err, protectedts.ErrNotFound.Error())
	state, err = p.GetState(ctx, nil /* txn */)
	require.NoError(t, err)
	require.Equal(t, ptpb.Metadata{Version: 2, NumRecords: 0, NumSpans: 0}, state.Metadata)
	require.EqualValues(t, []ptpb.Record(nil), state.Records)

	// Protecting it should succeed now (though this is weird because we'd
	// generally not reuse an ID).
	require.NoError(t, p.Protect(ctx, nil /* txn */, &r))
	require.NoError(t, p.MarkVerified(ctx, nil, r.ID))
	r.Verified = true
	state, err = p.GetState(ctx, nil /* txn */)
	require.NoError(t, err)
	require.Equal(t, ptpb.Metadata{Version: 3, NumRecords: 1, NumSpans: 1, TotalBytes: 8}, state.Metadata)
	require.EqualValues(t, []ptpb.Record{r}, state.Records)
}

// TODO(ajwerner): more testing, especially around addressing and limits.
