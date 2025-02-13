// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import (
	"context"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	kvstorage "github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// MoveTablePrimaryIndexID used to move the primary index of the created
// lease table from 1 to some target. It is injected from the lease_test package so that
// it can use sql primitives.
var MoveTablePrimaryIndexIDtoTarget func(
	context.Context, *testing.T, serverutils.ApplicationLayerInterface, descpb.ID, descpb.IndexID,
)

// TestKVWriterMatchesIEWriter is a rather involved test to exercise the
// kvWriter and ieWriter and confirm that they write exactly the same thing
// to the underlying key-value store. It does this by teeing operations to
// both under different table prefixes and then fetching the histories of
// those tables, removing the prefix and exact timestamps, and ensuring
// they are the same. This test will run against both the old and new table
// formats (with expiration or session ID).
func TestKVWriterMatchesIEWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serverArgs := base.TestServerArgs{}
	serverArgs.Settings = cluster.MakeClusterSettings()
	srv, sqlDB, kvDB := serverutils.StartServer(t, serverArgs)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// Otherwise, we wouldn't get complete SSTs in our export under stress.
	sqlutils.MakeSQLRunner(srv.SystemLayer().SQLConn(t)).Exec(
		t, "SET CLUSTER SETTING admission.elastic_cpu.enabled = false",
	)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	schema := strings.Replace(systemschema.LeaseTableSchema,
		"exclude_data_from_backup = true",
		"exclude_data_from_backup = false",
		1)

	makeTable := func(name string) (id descpb.ID) {
		// Rewrite the schema and drop the exclude_data_from_backup from flag,
		// since this will prevent export from working later on in the test.
		tdb.Exec(t, strings.Replace(schema, "system.lease", name, 1))
		tdb.QueryRow(t, "SELECT id FROM system.namespace WHERE name = $1", name).Scan(&id)
		// Modifies the primary index IDs to line up with the session based
		// or multi-region expiry based formats of the table.
		MoveTablePrimaryIndexIDtoTarget(ctx, t, s, id, 3)
		return id
	}
	lease1ID := makeTable("lease1")
	lease2ID := makeTable("lease2")
	ie := s.InternalDB().(isql.DB).Executor()
	codec := s.Codec()
	settingsWatcher := s.SettingsWatcher().(*settingswatcher.SettingsWatcher)
	w := teeWriter{
		a: newInternalExecutorWriter(ie, "defaultdb.public.lease1"),
		b: newKVWriter(codec, kvDB, lease2ID, settingsWatcher),
	}
	start := kvDB.Clock().Now()
	groups := generateWriteOps(2<<10, 1<<10)
	for {
		ops, ok := groups()
		if !ok {
			break
		}
		do := func(i int, txn *kv.Txn) error {
			return ops[i].f(w, ctx, txn, ops[i].leaseFields)
		}
		if len(ops) == 1 {
			require.NoError(t, do(0, nil /* txn */))
		} else {
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				for i := range ops {
					if err := do(i, txn); err != nil {
						return err
					}
				}
				return nil
			}))
		}
	}
	require.Equal(
		t,
		getRawHistoryKVs(ctx, t, kvDB, lease1ID, start, codec),
		getRawHistoryKVs(ctx, t, kvDB, lease2ID, start, codec),
	)
}

// getRawHistoryKVs will pull the complete revision history of the table since
// start, but strip the keys of the table prefix and fully remove the
// timestamps (though it will preserve timestamp ordering). This will allow us
// to ensure that the same KVs get written to both tables in the same order.
func getRawHistoryKVs(
	ctx context.Context,
	t *testing.T,
	kvDB *kv.DB,
	tabID descpb.ID,
	start hlc.Timestamp,
	codec keys.SQLCodec,
) []roachpb.KeyValue {
	var b kvpb.BatchRequest
	b.Header.Timestamp = kvDB.Clock().Now()
	b.Add(&kvpb.ExportRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    codec.TablePrefix(uint32(tabID)),
			EndKey: codec.TablePrefix(uint32(tabID)).PrefixEnd(),
		},
		MVCCFilter: kvpb.MVCCFilter_All,
		StartTime:  start,
	})
	br, err := kvDB.NonTransactionalSender().Send(ctx, &b)
	require.NoError(t, err.GoError())
	resp := br.Responses[0].GetExport()
	var rows []roachpb.KeyValue
	for _, f := range resp.Files {
		require.NoError(t, func() error {
			it, err := kvstorage.NewMemSSTIterator(f.SST, false /* verify */, kvstorage.IterOptions{
				// NB: We assume there will be no MVCC range tombstones here.
				KeyTypes:   kvstorage.IterKeyTypePointsOnly,
				LowerBound: keys.MinKey,
				UpperBound: keys.MaxKey,
			})
			if err != nil {
				return err
			}
			defer it.Close()
			for it.SeekGE(kvstorage.NilKey); ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					return err
				} else if !ok {
					return nil
				}
				k := it.UnsafeKey().Clone()
				suffix, _, err := codec.DecodeTablePrefix(k.Key)
				require.NoError(t, err)
				v, err := it.Value()
				require.NoError(t, err)
				row := roachpb.KeyValue{
					Key: suffix,
					Value: roachpb.Value{
						RawBytes: v,
					},
				}
				row.Value.ClearChecksum()
				rows = append(rows, row)
			}
		}())
	}
	return rows
}

type teeWriter struct {
	a, b writer
}

type writeOp struct {
	f func(writer, context.Context, *kv.Txn, leaseFields) error
	leaseFields
}

// We want to create groups of inserts and deletes. We want to make sure
// a row is only deleted if it was inserted. We also want to make sure a
// row is not deleted in the same transaction which deletes it. We achieve
// this by generating a mix of inserts and deletes, using new rows for
// inserts and previous rows for deletes. We then add the new writes to
// the history in a deferred function.
//
// The returned function is a generator which will return numGroups times
// before returning false. The total number of ops in all the groups will
// be n.
func generateWriteOps(n, numGroups int) func() (_ []writeOp, wantMore bool) {
	randLeaseFields := func() leaseFields {
		const vals = 10
		ts, err := tree.MakeDTimestamp(timeutil.Unix(
			0, rand.Int63()), time.Microsecond,
		)
		if err != nil {
			panic(err)
		}
		lf := leaseFields{
			descID:       descpb.ID(rand.Intn(vals)),
			version:      descpb.DescriptorVersion(rand.Intn(vals)),
			instanceID:   base.SQLInstanceID(rand.Intn(vals)),
			expiration:   *ts,
			sessionID:    []byte(ts.String() + "_session"),
			regionPrefix: enum.One,
		}
		return lf
	}
	var existing []leaseFields
	return func() ([]writeOp, bool) {
		if numGroups == 0 {
			return nil, false
		}
		var l int
		defer func() { n -= l; numGroups-- }()
		if numGroups == 1 {
			l = n
		} else if avg := (n - 1) / numGroups; avg > 0 {
			l = rand.Intn(avg*2) + 1
		} else {
			l = 1
		}
		numDeletes := rand.Intn(l)
		if numDeletes > len(existing) {
			numDeletes = len(existing)
		}
		ops := make([]writeOp, 0, l)
		numInserts := l - numDeletes
		toAddToHistory := make([]leaseFields, 0, numInserts)
		for i := 0; i < numInserts; i++ {
			lf := randLeaseFields()
			toAddToHistory = append(toAddToHistory, lf)
			ops = append(ops, writeOp{
				f:           writer.insertLease,
				leaseFields: lf,
			})
		}
		defer func() { existing = append(existing, toAddToHistory...) }()
		for i := 0; i < numDeletes; i++ {
			k := len(existing)
			r := rand.Intn(k)
			existing[r], existing[k-1] = existing[k-1], existing[r]
			ops = append(ops, writeOp{
				f:           writer.deleteLease,
				leaseFields: existing[k-1],
			})
			existing = existing[:k-1]
		}
		rand.Shuffle(len(ops), func(i, j int) {
			ops[i], ops[j] = ops[j], ops[i]
		})
		return ops, true
	}
}

func (t teeWriter) deleteLease(ctx context.Context, txn *kv.Txn, fields leaseFields) error {
	return errors.CombineErrors(
		t.a.deleteLease(ctx, txn, fields),
		t.b.deleteLease(ctx, txn, fields),
	)
}

func (t teeWriter) insertLease(ctx context.Context, txn *kv.Txn, fields leaseFields) error {
	return errors.CombineErrors(
		t.a.insertLease(ctx, txn, fields),
		t.b.insertLease(ctx, txn, fields),
	)
}

var _ writer = (*teeWriter)(nil)
