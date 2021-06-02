// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type recordResolvedWriter struct {
	resolved []jobspb.ResolvedSpan
}

func (r *recordResolvedWriter) AddKV(
	ctx context.Context, kv roachpb.KeyValue, prevVal roachpb.Value, backfillTimestamp hlc.Timestamp,
) error {
	return nil
}

func (r *recordResolvedWriter) AddResolved(
	ctx context.Context,
	span roachpb.Span,
	ts hlc.Timestamp,
	boundaryType jobspb.ResolvedSpan_BoundaryType,
) error {
	r.resolved = append(r.resolved, jobspb.ResolvedSpan{Span: span, Timestamp: ts})
	return nil
}

func (r *recordResolvedWriter) Close(ctx context.Context) {
}

var _ EventBufferWriter = (*recordResolvedWriter)(nil)

func TestEmitsResolvedDuringScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `
CREATE TABLE t (a INT PRIMARY KEY);
INSERT INTO t VALUES (1), (2), (3);
`)

	descr := catalogkv.TestingGetTableDescriptor(kvdb, keys.SystemSQLCodec, "defaultdb", "t")
	span := tableSpan(uint32(descr.GetID()))

	exportTime := kvdb.Clock().Now()
	cfg := physicalConfig{
		Spans:     []roachpb.Span{span},
		Timestamp: exportTime,
		knobs: testingKnobs{
			beforeScanRequest: func(b *kv.Batch) {
				b.Header.MaxSpanRequestKeys = 1
			},
		},
	}

	scanner := &scanRequestScanner{
		settings: s.ClusterSettings(),
		gossip:   gossip.MakeOptionalGossip(s.GossipI().(*gossip.Gossip)),
		db:       kvdb,
	}

	sink := &recordResolvedWriter{}
	require.NoError(t, scanner.Scan(ctx, sink, cfg))

	startKey := span.Key
	require.Equal(t, 3, len(sink.resolved))
	for i := 0; i < 2; i++ {
		require.Equal(t, startKey, sink.resolved[i].Span.Key)
		startKey = sink.resolved[i].Span.EndKey
	}
	// The last resolved span is the entire span we exported.
	require.Equal(t, span, sink.resolved[2].Span)
	require.Equal(t, exportTime, sink.resolved[2].Timestamp)
}
