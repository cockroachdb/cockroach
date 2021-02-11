// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl" // Ensure changefeed init hooks run.
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl" // Ensure we can start tenant.
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/jackc/pgx"
	"github.com/stretchr/testify/require"
)

// replicationMessage represents the data returned by replication stream.
type replicationMessage struct {
	kv       roachpb.KeyValue
	resolved hlc.Timestamp
}

// replicationFeed yields replicationMessages from the replication stream.
type replicationFeed struct {
	t      *testing.T
	conn   *pgx.Conn
	rows   *pgx.Rows
	msg    replicationMessage
	cancel func()
}

// Close closes underlying sql connection.
func (f *replicationFeed) Close() {
	f.cancel()
	f.rows.Close()
	require.NoError(f.t, f.conn.Close())
}

// Next sets replicationMessage and returns true if there are more rows available.
// Returns false otherwise.
func (f *replicationFeed) Next() (m replicationMessage, haveMoreRows bool) {
	haveMoreRows = f.rows.Next()
	if !haveMoreRows {
		return
	}

	var ignoreTopic gosql.NullString
	var k, v []byte
	require.NoError(f.t, f.rows.Scan(&ignoreTopic, &k, &v))

	if len(k) == 0 {
		require.NoError(f.t, protoutil.Unmarshal(v, &m.resolved))
	} else {
		m.kv.Key = k
		require.NoError(f.t, protoutil.Unmarshal(v, &m.kv.Value))
	}
	return
}

func nativeToDatum(t *testing.T, native interface{}) tree.Datum {
	t.Helper()
	switch v := native.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v))
	case int:
		return tree.NewDInt(tree.DInt(v))
	case string:
		return tree.NewDString(v)
	case nil:
		return tree.DNull
	case tree.Datum:
		return v
	default:
		t.Fatalf("unexpected value type %T", v)
		return nil
	}
}

// encodeKV encodes primary key with the specified "values".  Values must be
// specified in the same order as the columns in the primary family.
func encodeKV(
	t *testing.T, codec keys.SQLCodec, descr catalog.TableDescriptor, pkeyVals ...interface{},
) roachpb.KeyValue {
	require.Equal(t, 1, descr.NumFamilies(), "there can be only one")
	primary := descr.GetPrimaryIndex().IndexDesc()
	require.LessOrEqual(t, len(primary.ColumnIDs), len(pkeyVals))

	var datums tree.Datums
	var colMap catalog.TableColMap
	for i, val := range pkeyVals {
		datums = append(datums, nativeToDatum(t, val))
		col, err := descr.FindColumnWithID(descpb.ColumnID(i + 1))
		require.NoError(t, err)
		colMap.Set(col.GetID(), col.Ordinal())
	}

	const includeEmpty = true
	indexEntries, err := rowenc.EncodePrimaryIndex(codec, descr, primary,
		colMap, datums, includeEmpty)
	require.Equal(t, 1, len(indexEntries))
	require.NoError(t, err)
	indexEntries[0].Value.InitChecksum(indexEntries[0].Key)
	return roachpb.KeyValue{Key: indexEntries[0].Key, Value: indexEntries[0].Value}
}

type feedPredicate func(message replicationMessage) bool

func (f *replicationFeed) consumeUntil(pred feedPredicate) error {
	const maxWait = 10 * time.Second
	doneCh := make(chan struct{})
	defer close(doneCh)
	go func() {
		select {
		case <-time.After(maxWait):
			f.cancel()
		case <-doneCh:
		}
	}()

	for {
		msg, haveMoreRows := f.Next()
		require.True(f.t, haveMoreRows, f.rows.Err()) // Our replication stream never ends.
		if pred(msg) {
			f.msg = msg
			return nil
		}
	}
}

func keyMatches(key roachpb.Key) feedPredicate {
	return func(msg replicationMessage) bool {
		return bytes.Equal(key, msg.kv.Key)
	}
}

func resolvedAtLeast(lo hlc.Timestamp) feedPredicate {
	return func(msg replicationMessage) bool {
		return lo.LessEq(msg.resolved)
	}
}

// ObserveKey consumes the feed until requested key has been seen (or deadline expired).
// Note: we don't do any buffering here.  Therefore, it is required that the key
// we want to observe will arrive at some point in the future.
func (f *replicationFeed) ObserveKey(key roachpb.Key) replicationMessage {
	require.NoError(f.t, f.consumeUntil(keyMatches(key)))
	return f.msg
}

// ObserveResolved consumes the feed until we received resolved timestamp that's at least
// as high as the specified low watermark.  Returns observed resolved timestamp.
func (f *replicationFeed) ObserveResolved(lo hlc.Timestamp) hlc.Timestamp {
	require.NoError(f.t, f.consumeUntil(resolvedAtLeast(lo)))
	return f.msg.resolved
}

// tenantState maintains test state related to tenant.
type tenantState struct {
	id    roachpb.TenantID
	codec keys.SQLCodec
	sql   *sqlutils.SQLRunner
}

// replicationHelper accommodates setup and execution of replications stream.
type replicationHelper struct {
	sysServer serverutils.TestServerInterface
	sysDB     *sqlutils.SQLRunner
	tenant    tenantState
	sink      url.URL
}

// StartReplication starts replication stream, specified as query and its args.
func (r *replicationHelper) StartReplication(
	t *testing.T, create string, args ...interface{},
) *replicationFeed {
	sink := r.sink
	sink.RawQuery = r.sink.Query().Encode()

	// Use pgx directly instead of database/sql so we can close the conn
	// (instead of returning it to the pool).
	pgxConfig, err := pgx.ParseConnectionString(sink.String())
	require.NoError(t, err)

	conn, err := pgx.Connect(pgxConfig)
	require.NoError(t, err)

	queryCtx, cancel := context.WithCancel(context.Background())
	rows, err := conn.QueryEx(queryCtx, create, nil, args...)
	require.NoError(t, err)
	return &replicationFeed{
		t:      t,
		conn:   conn,
		rows:   rows,
		cancel: cancel,
	}
}

// newReplicationHelper starts test server and configures it to have
// active tenant.
func newReplicationHelper(t *testing.T) (*replicationHelper, func()) {
	ctx := context.Background()

	// Start server
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})

	// Set required cluster settings.
	_, err := db.Exec(`
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'
`)
	require.NoError(t, err)

	// Make changefeeds run faster.
	resetFreq := changefeedbase.TestingSetDefaultFlushFrequency(50 * time.Microsecond)

	// Start tenant server
	tenantID := roachpb.MakeTenantID(10)
	_, tenantConn := serverutils.StartTenant(t, s, base.TestTenantArgs{TenantID: tenantID})

	// Sink to read data from.
	sink, cleanupSink := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))

	h := &replicationHelper{
		sysServer: s,
		sysDB:     sqlutils.MakeSQLRunner(db),
		sink:      sink,
		tenant: tenantState{
			id:    tenantID,
			codec: keys.MakeSQLCodec(tenantID),
			sql:   sqlutils.MakeSQLRunner(tenantConn),
		},
	}

	return h, func() {
		resetFreq()
		cleanupSink()
		require.NoError(t, tenantConn.Close())
		s.Stopper().Stop(ctx)
	}
}

func TestReplicationStreamTenant(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	h, cleanup := newReplicationHelper(t)
	defer cleanup()

	h.tenant.sql.Exec(t, `
CREATE DATABASE d;
CREATE TABLE d.t1(i int primary key, a string, b string);
CREATE TABLE d.t2(i int primary key);
INSERT INTO d.t1 (i) VALUES (42);
INSERT INTO d.t2 VALUES (2);
`)

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d`, h.tenant.id.ToUint64())

	t.Run("cannot-stream-tenant-from-tenant", func(t *testing.T) {
		// Cannot replicate stream from inside the tenant
		_, err := h.tenant.sql.DB.ExecContext(context.Background(), streamTenantQuery)
		require.True(t, testutils.IsError(err, "only the system tenant can backup other tenants"), err)
	})

	descr := catalogkv.TestingGetTableDescriptor(h.sysServer.DB(), h.tenant.codec, "d", "t1")

	t.Run("stream-tenant", func(t *testing.T) {
		feed := h.StartReplication(t, streamTenantQuery)
		defer feed.Close()

		expected := encodeKV(t, h.tenant.codec, descr, 42)
		firstObserved := feed.ObserveKey(expected.Key)

		require.Equal(t, expected.Value.RawBytes, firstObserved.kv.Value.RawBytes)

		// Periodically, resolved timestamps should be published.
		// Observe resolved timestamp that's higher than the previous value timestamp.
		feed.ObserveResolved(firstObserved.kv.Value.Timestamp)

		// Update our row.
		h.tenant.sql.Exec(t, `UPDATE d.t1 SET b = 'world' WHERE i = 42`)
		expected = encodeKV(t, h.tenant.codec, descr, 42, nil, "world")

		// Observe its changes.
		secondObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.kv.Value.RawBytes)
		require.True(t, firstObserved.kv.Value.Timestamp.Less(secondObserved.kv.Value.Timestamp))
	})

	t.Run("stream-tenant-with-cursor", func(t *testing.T) {
		beforeUpdateTS := h.sysServer.Clock().Now()
		h.tenant.sql.Exec(t, `UPDATE d.t1 SET a = 'привет' WHERE i = 42`)
		h.tenant.sql.Exec(t, `UPDATE d.t1 SET b = 'мир' WHERE i = 42`)

		feed := h.StartReplication(t, fmt.Sprintf(
			"%s WITH cursor='%s'", streamTenantQuery, beforeUpdateTS.AsOfSystemTime()))
		defer feed.Close()

		// We should observe 2 versions of this key: one with ("привет", "world"), and a later
		// version ("привет", "мир")
		expected := encodeKV(t, h.tenant.codec, descr, 42, "привет", "world")
		firstObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, firstObserved.kv.Value.RawBytes)

		expected = encodeKV(t, h.tenant.codec, descr, 42, "привет", "мир")
		secondObserved := feed.ObserveKey(expected.Key)
		require.Equal(t, expected.Value.RawBytes, secondObserved.kv.Value.RawBytes)
	})
}
