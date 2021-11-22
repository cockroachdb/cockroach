// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svpreemptor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svacquirer"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svpreemptor"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPreemptor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "SET CLUSTER SETTING kv.rangefeed.enabled = true")

	_, tableID := svtestutils.SetUpTestingTable(t, tdb)
	codec := keys.SystemSQLCodec
	var instance svtestutils.TestInstance
	exp := kvDB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	session := svtestutils.MakeTestSession("foo", exp)
	instance.SetSession(session)

	slr := testReader{}
	slr.Store(session.ID(), nil)
	rf := s.RangeFeedFactory().(*rangefeed.Factory)
	svStorage := svstorage.NewStorage(codec, tableID)
	p := svpreemptor.NewPreemptor(s.Stopper(), kvDB, rf, svStorage, &slr)
	a := svacquirer.NewAcquirer(log.AmbientContext{}, s.Stopper(), kvDB, rf, svStorage, &instance)
	require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return p.EnsureNoSingleVersionLeases(ctx, txn, catalog.MakeDescriptorIDSet(1, 2, 3))
	}))

	mkL := func(id descpb.ID) singleversion.Lease {
		l, err := a.Acquire(ctx, id)
		require.NoError(t, err)
		return l
	}
	preempt := func(ids ...descpb.ID) <-chan error {
		errCh := make(chan error, 1)
		go func() {
			idSet := catalog.MakeDescriptorIDSet(ids...)
			var commitTS func() hlc.Timestamp
			errCh <- kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				defer func() {
					log.Infof(ctx, "interally done preempting %v", idSet)
				}()
				commitTS = txn.CommitTimestamp
				return p.EnsureNoSingleVersionLeases(ctx, txn, idSet)
			})
			log.Infof(ctx, "done preempting %v %v", idSet, commitTS())
		}()
		return errCh
	}
	blocked := func(chans ...<-chan error) {
		t.Helper()
		const aShortWhile = 10 * time.Millisecond
		time.Sleep(aShortWhile)
		for i, ch := range chans {
			select {
			case err := <-ch:
				t.Fatalf("expected no error for %d: %v", i, err)
			default:
			}
		}
	}
	l1, l2a, l2b, l2c, l3 := mkL(1), mkL(2), mkL(2), mkL(2), mkL(3)
	p12 := preempt(1, 2)
	tdb.CheckQueryResultsRetry(t,
		"SELECT count(*) FROM crdb_internal.active_range_feeds WHERE tags LIKE '%singleversion%'",
		[][]string{{"1"}},
	)
	log.Infof(ctx, "here after rangefeeds")
	p1 := preempt(1)
	p3 := preempt(3)
	blocked(p12, p1, p3)
	l3.Release(ctx, kvDB.Clock().Now().Add(time.Millisecond.Nanoseconds(), 0))
	require.NoError(t, <-p3)
	p23 := preempt(2, 3)
	blocked(p12, p1, p23)
	l2a.Release(ctx, hlc.Timestamp{})
	l2b.Release(ctx, hlc.Timestamp{})
	blocked(p12, p1, p23)
	l2c.Release(ctx, hlc.Timestamp{})
	blocked(p12, p1, p23) // p23 is blocked on p12
	l1.Release(ctx, hlc.Timestamp{})
	require.NoError(t, <-p12)
	require.NoError(t, <-p1)
	require.NoError(t, <-p23)

}

type testReader struct {
	sync.Map
}

func (t testReader) IsAlive(ctx context.Context, id sqlliveness.SessionID) (alive bool, err error) {
	_, alive = t.Load(id)
	return alive, nil
}

var _ sqlliveness.Reader = (*testReader)(nil)
