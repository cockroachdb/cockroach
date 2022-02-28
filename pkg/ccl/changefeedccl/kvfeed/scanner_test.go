// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvfeed

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

type recordResolvedWriter struct {
	resolved []jobspb.ResolvedSpan
}

func (r *recordResolvedWriter) Add(ctx context.Context, e kvevent.Event) error {
	if e.Type() == kvevent.TypeResolved {
		r.resolved = append(r.resolved, *e.Resolved())
	}
	return nil
}

func (r *recordResolvedWriter) Drain(ctx context.Context) error {
	return nil
}

func (r *recordResolvedWriter) CloseWithReason(ctx context.Context, reason error) error {
	return nil
}

var _ kvevent.Writer = (*recordResolvedWriter)(nil)

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

	descr := desctestutils.TestingGetPublicTableDescriptor(kvdb, keys.SystemSQLCodec, "defaultdb", "t")
	span := tableSpan(uint32(descr.GetID()))

	exportTime := kvdb.Clock().Now()
	cfg := physicalConfig{
		Spans:     []roachpb.Span{span},
		Timestamp: exportTime,
		Knobs: TestingKnobs{
			BeforeScanRequest: func(b *kv.Batch) error {
				b.Header.MaxSpanRequestKeys = 1
				return nil
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
