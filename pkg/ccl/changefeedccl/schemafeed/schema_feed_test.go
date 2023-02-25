// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package schemafeed

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTableHistoryIngestionTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	ts := func(wt int64) hlc.Timestamp { return hlc.Timestamp{WallTime: wt} }
	validateFn := func(_ context.Context, _ hlc.Timestamp, desc catalog.Descriptor) error {
		if desc.GetName() != `` {
			return errors.Newf("descriptor: %s", desc.GetName())
		}
		return nil
	}
	requireChannelEmpty := func(t *testing.T, ch chan error) {
		t.Helper()
		select {
		case err := <-ch:
			t.Fatalf(`expected empty channel got %v`, err)
		default:
		}
	}

	m := schemaFeed{}
	m.mu.highWater = ts(0)

	require.Equal(t, ts(0), m.highWater())

	// advance
	require.NoError(t, m.ingestDescriptors(ctx, ts(0), ts(1), nil, validateFn))
	require.Equal(t, ts(1), m.highWater())
	require.NoError(t, m.ingestDescriptors(ctx, ts(1), ts(2), nil, validateFn))
	require.Equal(t, ts(2), m.highWater())

	// no-ops
	require.NoError(t, m.ingestDescriptors(ctx, ts(0), ts(1), nil, validateFn))
	require.Equal(t, ts(2), m.highWater())
	require.NoError(t, m.ingestDescriptors(ctx, ts(1), ts(2), nil, validateFn))
	require.Equal(t, ts(2), m.highWater())

	// overlap
	require.NoError(t, m.ingestDescriptors(ctx, ts(1), ts(3), nil, validateFn))
	require.Equal(t, ts(3), m.highWater())

	// gap
	require.EqualError(t, m.ingestDescriptors(ctx, ts(4), ts(5), nil, validateFn),
		`gap between 0.000000003,0 and 0.000000004,0`)
	require.Equal(t, ts(3), m.highWater())

	// validates
	require.NoError(t, m.ingestDescriptors(ctx, ts(3), ts(4), []catalog.Descriptor{
		tabledesc.NewBuilder(&descpb.TableDescriptor{ID: 0}).BuildImmutable(),
	}, validateFn))
	require.Equal(t, ts(4), m.highWater())

	// high-water already high enough. fast-path
	require.NoError(t, m.waitForTS(ctx, ts(3)))
	require.NoError(t, m.waitForTS(ctx, ts(4)))

	// high-water not there yet. blocks
	errCh6 := make(chan error, 1)
	errCh7 := make(chan error, 1)
	go func() { errCh7 <- m.waitForTS(ctx, ts(7)) }()
	go func() { errCh6 <- m.waitForTS(ctx, ts(6)) }()
	requireChannelEmpty(t, errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances, but not enough
	require.NoError(t, m.ingestDescriptors(ctx, ts(4), ts(5), nil, validateFn))
	requireChannelEmpty(t, errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances, unblocks only errCh6
	require.NoError(t, m.ingestDescriptors(ctx, ts(5), ts(6), nil, validateFn))
	require.NoError(t, <-errCh6)
	requireChannelEmpty(t, errCh7)

	// high-water advances again, unblocks errCh7
	require.NoError(t, m.ingestDescriptors(ctx, ts(6), ts(7), nil, validateFn))
	require.NoError(t, <-errCh7)

	// validate ctx cancellation
	errCh8 := make(chan error, 1)
	ctxTS8, cancelTS8 := context.WithCancel(ctx)
	go func() { errCh8 <- m.waitForTS(ctxTS8, ts(8)) }()
	requireChannelEmpty(t, errCh8)
	cancelTS8()
	require.EqualError(t, <-errCh8, `context canceled`)

	// does not validate, high-water does not change
	require.EqualError(t, m.ingestDescriptors(ctx, ts(7), ts(10), []catalog.Descriptor{
		tabledesc.NewBuilder(&descpb.TableDescriptor{ID: 0, Name: `whoops!`}).BuildImmutable(),
	}, validateFn), `descriptor: whoops!`)
	require.Equal(t, ts(7), m.highWater())

	// ts 10 has errored, so validate can return its error without blocking
	require.EqualError(t, m.waitForTS(ctx, ts(10)), `descriptor: whoops!`)

	// ts 8 and 9 are still unknown
	errCh8 = make(chan error, 1)
	errCh9 := make(chan error, 1)
	go func() { errCh8 <- m.waitForTS(ctx, ts(8)) }()
	go func() { errCh9 <- m.waitForTS(ctx, ts(9)) }()
	requireChannelEmpty(t, errCh8)
	requireChannelEmpty(t, errCh9)

	// turns out ts 10 is not a tight bound. ts 9 also has an error
	require.EqualError(t, m.ingestDescriptors(ctx, ts(7), ts(9), []catalog.Descriptor{
		tabledesc.NewBuilder(&descpb.TableDescriptor{ID: 0, Name: `oh no!`}).BuildImmutable(),
	}, validateFn), `descriptor: oh no!`)
	require.Equal(t, ts(7), m.highWater())
	require.EqualError(t, <-errCh9, `descriptor: oh no!`)

	// ts 8 is still unknown
	requireChannelEmpty(t, errCh8)

	// always return the earlist error seen (so waiting for ts 10 immediately
	// returns the 9 error now, it returned the ts 10 error above)
	require.EqualError(t, m.waitForTS(ctx, ts(9)), `descriptor: oh no!`)

	// something earlier than ts 10 can still be okay
	require.NoError(t, m.ingestDescriptors(ctx, ts(7), ts(8), nil, validateFn))
	require.Equal(t, ts(8), m.highWater())
	require.NoError(t, <-errCh8)
}

func TestIssuesHighPriorityReadsIfBlocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Lay down an intent on system.descriptors table.
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `BEGIN; CREATE TABLE defaultdb.foo (a INT);`)

	// Attempt to fetch descriptors; it should succeed withing reasonable time.
	priorityAfter := 500 * time.Millisecond
	highPriorityAfter.Override(ctx, &s.ClusterSettings().SV, priorityAfter)
	var responseFiles []kvpb.ExportResponse_File
	testutils.SucceedsWithin(t, func() error {
		span := roachpb.Span{Key: keys.SystemSQLCodec.TablePrefix(keys.DescriptorTableID)}
		span.EndKey = span.Key.PrefixEnd()
		resp, err := sendExportRequestWithPriorityOverride(ctx, s.ClusterSettings(),
			kvDB.NonTransactionalSender(), span, hlc.Timestamp{}, s.Clock().Now())
		if err != nil {
			return err
		}
		responseFiles = resp.(*kvpb.ExportResponse).Files
		return nil
	}, 10*priorityAfter)
	require.Less(t, 0, len(responseFiles))
}

func TestFetchDescriptorVersionsCPULimiterPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var numRequests int
	first := true
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
				for _, ru := range request.Requests {
					if _, ok := ru.GetInner().(*kvpb.ExportRequest); ok {
						numRequests++
						h := admission.ElasticCPUWorkHandleFromContext(ctx)
						if h == nil {
							t.Fatalf("expected context to have CPU work handle")
						}
						h.TestingOverrideOverLimit(func() (bool, time.Duration) {
							if first {
								first = false
								return true, 0
							}
							return false, 0
						})
					}
				}
				return nil
			},
		}},
	})
	defer s.Stopper().Stop(ctx)
	sqlServer := s.SQLServer().(*sql.Server)
	if len(s.TestTenants()) != 0 {
		sqlServer = s.TestTenants()[0].PGServer().(*pgwire.Server).SQLServer
	}

	sqlDB := sqlutils.MakeSQLRunner(db)
	beforeCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE baz (a INT PRIMARY KEY)`)
	afterCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	var targets changefeedbase.Targets
	var tableID descpb.ID
	var statementTimeName changefeedbase.StatementTimeName
	sqlDB.QueryRow(t, "SELECT $1::regclass::int, $1::regclass::string", "foo").Scan(
		&tableID, &statementTimeName)
	targets.Add(changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableID,
		FamilyName:        "primary",
		StatementTimeName: statementTimeName,
	})
	sqlDB.QueryRow(t, "SELECT $1::regclass::int, $1::regclass::string", "bar").Scan(
		&tableID, &statementTimeName)
	targets.Add(changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableID,
		FamilyName:        "primary",
		StatementTimeName: statementTimeName,
	})
	now := s.Clock().Now()
	sf := New(ctx, &sqlServer.GetExecutorConfig().DistSQLSrv.ServerConfig,
		TestingAllEventFilter, targets, now, nil, changefeedbase.CanHandle{
			MultipleColumnFamilies: true,
			VirtualColumns:         true,
		})
	scf := sf.(*schemaFeed)
	desc, err := scf.fetchDescriptorVersions(ctx, beforeCreate, afterCreate)
	require.NoError(t, err)
	require.Len(t, desc, 2)
	require.Equal(t, 2, numRequests)
}
