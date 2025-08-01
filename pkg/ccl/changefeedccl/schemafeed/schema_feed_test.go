// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemafeed

import (
	"context"
	"slices"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
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
	frontier := func() hlc.Timestamp {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.mu.ts.frontier
	}

	require.Equal(t, ts(0), frontier())

	// advance
	require.NoError(t, m.ingestDescriptors(ctx, ts(0), ts(1), nil, validateFn))
	require.Equal(t, ts(1), frontier())
	require.NoError(t, m.ingestDescriptors(ctx, ts(1), ts(2), nil, validateFn))
	require.Equal(t, ts(2), frontier())

	// overlap
	require.NoError(t, m.ingestDescriptors(ctx, ts(1), ts(3), nil, validateFn))
	require.Equal(t, ts(3), frontier())

	// gap
	require.EqualError(t, m.ingestDescriptors(ctx, ts(4), ts(5), nil, validateFn),
		`gap between 0.000000003,0 and 0.000000004,0`)
	require.Equal(t, ts(3), frontier())

	// validates
	require.NoError(t, m.ingestDescriptors(ctx, ts(3), ts(4), []catalog.Descriptor{
		tabledesc.NewBuilder(&descpb.TableDescriptor{ID: 0}).BuildImmutable(),
	}, validateFn))
	require.Equal(t, ts(4), frontier())

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
	require.Equal(t, ts(7), frontier())

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
	require.Equal(t, ts(7), frontier())
	require.EqualError(t, <-errCh9, `descriptor: oh no!`)

	// ts 8 is still unknown
	requireChannelEmpty(t, errCh8)

	// always return the earliest error seen (so waiting for ts 10 immediately
	// returns the 9 error now, it returned the ts 10 error above)
	require.EqualError(t, m.waitForTS(ctx, ts(9)), `descriptor: oh no!`)

	// something earlier than ts 10 can still be okay
	require.NoError(t, m.ingestDescriptors(ctx, ts(7), ts(8), nil, validateFn))
	require.Equal(t, ts(8), frontier())
	require.NoError(t, <-errCh8)
}

func TestIssuesHighPriorityReadsIfBlocked(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Lay down an intent on system.descriptors table.
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `BEGIN; CREATE TABLE defaultdb.foo (a INT);`)

	// Attempt to fetch descriptors; it should succeed withing reasonable time.
	priorityAfter := 500 * time.Millisecond
	highPriorityAfter.Override(ctx, &s.ClusterSettings().SV, priorityAfter)
	var responseFiles []kvpb.ExportResponse_File
	testutils.SucceedsWithin(t, func() error {
		span := roachpb.Span{Key: s.Codec().TablePrefix(keys.DescriptorTableID)}
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
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
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
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	sqlServer := s.SQLServer().(*sql.Server)

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

func TestSchemaFeedHandlesCascadeDatabaseDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	sqlServer := s.SQLServer().(*sql.Server)

	sqlDB := sqlutils.MakeSQLRunner(db)

	beforeCreate := s.Clock().Now()

	// Create a database, containing user defined type along with table using that type.
	sqlDB.ExecMultiple(t,
		`CREATE DATABASE test`,
		`USE test`,
		`CREATE TYPE status AS ENUM ('open', 'closed', 'inactive')`,
		`CREATE TABLE foo(a INT, t status DEFAULT 'open')`,
		`USE defaultdb`,
	)

	var targets changefeedbase.Targets
	var tableID descpb.ID
	sqlDB.QueryRow(t, "SELECT 'test.foo'::regclass::int").Scan(&tableID)
	targets.Add(changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableID,
		FamilyName:        "primary",
		StatementTimeName: "foo",
	})
	sf := New(ctx, &sqlServer.GetExecutorConfig().DistSQLSrv.ServerConfig,
		TestingAllEventFilter, targets, s.Clock().Now(), nil, changefeedbase.CanHandle{
			MultipleColumnFamilies: true,
			VirtualColumns:         true,
		}).(*schemaFeed)

	// initialize type dependencies in schema feed.
	require.NoError(t, sf.primeInitialTableDescs(ctx))

	// DROP database with cascade to cause the type along with the table to be dropped.
	// Dropped tables are marked as being dropped (i.e. there is an MVCC version of the
	// descriptor that has a state indicating that the table is being dropped).
	// However, dependent UDTs are simply deleted so, there is an MVCC tombstone for that type.
	sqlDB.Exec(t, `DROP DATABASE test CASCADE;`)

	// Fetching descriptor versions from before the initial create statement
	// up until the current time should result in a catalog.ErrDescriptorDropped error.
	_, err := sf.fetchDescriptorVersions(ctx, beforeCreate, s.Clock().Now())
	require.True(t, errors.Is(err, catalog.ErrDescriptorDropped),
		"expected dropped descriptor error, found: %v", err)
}

// testLeaseAcquirer is a test implementation of leaseAcquirer.
// It contains an ordered time map of descriptors for a single ID.
// TODO(yang): Extend this for multiple IDs.
type testLeaseAcquirer struct {
	id    descpb.ID
	descs []*testLeasedDescriptor
}

func (t *testLeaseAcquirer) Acquire(
	ctx context.Context, timestamp hlc.Timestamp, id descpb.ID,
) (lease.LeasedDescriptor, error) {
	if id != t.id {
		return nil, errors.Newf("unknown id: %d", id)
	}

	i, ok := slices.BinarySearchFunc(t.descs, timestamp, func(desc *testLeasedDescriptor, timestamp hlc.Timestamp) int {
		return desc.timestamp.Compare(timestamp)
	})
	if ok {
		return t.descs[i], nil
	}
	if i == 0 {
		return nil, errors.Newf("no descriptors for id: %d", id)
	}
	return t.descs[i-1], nil
}

func (t *testLeaseAcquirer) AcquireFreshestFromStore(ctx context.Context, id descpb.ID) error {
	if id != t.id {
		return errors.Newf("unknown id: %d", id)
	}
	return nil
}

func (t *testLeaseAcquirer) Codec() keys.SQLCodec {
	panic("should not be called")
}

type testLeasedDescriptor struct {
	lease.LeasedDescriptor
	timestamp hlc.Timestamp
	desc      catalog.Descriptor
}

func newTestLeasedDescriptor(
	id descpb.ID, version descpb.DescriptorVersion, schemaLocked bool, timestamp hlc.Timestamp,
) *testLeasedDescriptor {
	return &testLeasedDescriptor{
		timestamp: timestamp,
		desc: &testTableDescriptor{
			id:           id,
			version:      version,
			schemaLocked: schemaLocked,
		},
	}
}

func (ld *testLeasedDescriptor) Release(ctx context.Context) {
}

func (ld *testLeasedDescriptor) Underlying() catalog.Descriptor {
	return ld.desc
}

type testTableDescriptor struct {
	catalog.TableDescriptor
	id           descpb.ID
	version      descpb.DescriptorVersion
	schemaLocked bool
}

func (td *testTableDescriptor) GetID() descpb.ID {
	return td.id
}

func (td *testTableDescriptor) GetVersion() descpb.DescriptorVersion {
	return td.version
}

func (td *testTableDescriptor) IsSchemaLocked() bool {
	return td.schemaLocked
}

func TestPauseOrResumePolling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const tableID = 123
	const (
		v1 = 1
		v2 = 2
		v3 = 3
	)
	const (
		schemaLocked    = true
		notSchemaLocked = false
	)
	tableDescs := []*testLeasedDescriptor{
		newTestLeasedDescriptor(tableID, v1, notSchemaLocked, hlc.Timestamp{WallTime: 20}),
		newTestLeasedDescriptor(tableID, v2, schemaLocked, hlc.Timestamp{WallTime: 40}),
		newTestLeasedDescriptor(tableID, v3, notSchemaLocked, hlc.Timestamp{WallTime: 60}),
	}

	sf := schemaFeed{
		leaseMgr: &testLeaseAcquirer{
			id:    tableID,
			descs: tableDescs,
		},
		targets: CreateChangefeedTargets(tableID),
	}

	getFrontier := func() hlc.Timestamp {
		sf.mu.Lock()
		defer sf.mu.Unlock()
		return sf.mu.ts.frontier
	}
	setFrontier := func(ts hlc.Timestamp) error {
		sf.mu.Lock()
		defer sf.mu.Unlock()
		return sf.mu.ts.advanceFrontier(ts)
	}

	// Set the initial frontier to 10.
	require.NoError(t, setFrontier(hlc.Timestamp{WallTime: 10}))

	// Initially, polling should not be paused.
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 10}, getFrontier())

	// We expect a non-terminal error to be swallowed for time 10.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 10}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 10}, getFrontier())

	// We bump the highwater up to reflect a descriptor being read at time 20.
	require.NoError(t, setFrontier(hlc.Timestamp{WallTime: 20}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 20}, getFrontier())

	// We expect polling not to be paused for time 30.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 30}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 20}, getFrontier())

	// We expect polling not to be paused for time 40 since the highwater
	// has not caught up to the schema-locked version.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 40}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 20}, getFrontier())

	// We bump the highwater up to reflect a descriptor being read at time 40.
	require.NoError(t, setFrontier(hlc.Timestamp{WallTime: 40}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 40}, getFrontier())

	// We expect polling to be paused for time 40 now that the highwater has
	// caught up to the schema-locked version.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 40}))
	require.True(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 40}, getFrontier())

	// We expect polling continue to be paused for time 50 and to see the
	// highwater bumped up.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 50}))
	require.True(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 50}, getFrontier())

	// We expect polling continue to be paused for time 20 and to see the
	// highwater remain unchanged (fast path).
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 20}))
	require.True(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 50}, getFrontier())

	// We expect polling to be resumed for time 60 and to not see the highwater
	// bumped up.
	require.NoError(t, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 60}))
	require.False(t, sf.pollingPaused())
	require.Equal(t, hlc.Timestamp{WallTime: 50}, getFrontier())
}

// BenchmarkPauseOrResumePolling benchmarks pauseOrResumePolling in cases where
// there is a non-terminal error early, polling should be paused, and polling
// should not be paused.
func BenchmarkPauseOrResumePolling(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()

	const tableID = 123
	sf := schemaFeed{
		leaseMgr: &testLeaseAcquirer{
			id: tableID,
			descs: []*testLeasedDescriptor{
				newTestLeasedDescriptor(tableID, 1, false, hlc.Timestamp{WallTime: 30}),
				newTestLeasedDescriptor(tableID, 2, true, hlc.Timestamp{WallTime: 40}),
			},
		},
		targets: CreateChangefeedTargets(tableID),
	}
	setFrontier := func(ts hlc.Timestamp) error {
		sf.mu.Lock()
		defer sf.mu.Unlock()
		return sf.mu.ts.advanceFrontier(ts)
	}

	// Set the initial frontier to 10.
	require.NoError(b, setFrontier(hlc.Timestamp{WallTime: 10}))
	// Initially, polling should not be paused.
	require.False(b, sf.pollingPaused())

	b.Run("non-terminal error", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// We expect a non-terminal error to be swallowed for time 10 since a
			// valid descriptor does not exist for time 10.
			require.NoError(b, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 10}))
		}
		require.False(b, sf.pollingPaused())
	})
	b.Run("not schema locked", func(b *testing.B) {
		// We bump the highwater up to reflect a descriptor being read at time 30.
		require.NoError(b, setFrontier(hlc.Timestamp{WallTime: 30}))
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// We do not expect polling to be paused for time 30 since the descriptor
			// at time 30 is not schema locked.
			require.NoError(b, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 30}))
		}
		require.False(b, sf.pollingPaused())
	})
	b.Run("schema locked", func(b *testing.B) {
		// We bump the highwater up to reflect a descriptor being read at time 50.
		require.NoError(b, setFrontier(hlc.Timestamp{WallTime: 50}))
		for i := 0; i < b.N; i++ {
			// We expect polling to be paused for time 50 now that the highwater on a
			// schema-locked version.
			require.NoError(b, sf.pauseOrResumePolling(ctx, hlc.Timestamp{WallTime: 50}))
		}
		require.True(b, sf.pollingPaused())
	})
}
