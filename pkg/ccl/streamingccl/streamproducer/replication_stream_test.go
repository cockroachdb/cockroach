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
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
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
	t    *testing.T
	conn *pgx.Conn
	rows *pgx.Rows
}

// Close closes underlying sql connection.
func (f *replicationFeed) Close() {
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

type feedMatcher struct {
	t     *testing.T
	descr catalog.TableDescriptor
	codec keys.SQLCodec
}

// ObserveKey consumes the feed until requested primary key has been seen.
// Primary key is specified as pkeyVals -- interface values representing every column
// in the primary key index.
func (m feedMatcher) ObserveKey(f *replicationFeed, pkeyVals ...interface{}) replicationMessage {
	pk, err := rowenc.TestingMakePrimaryIndexKeyForTenant(m.descr, m.codec, pkeyVals...)
	require.NoError(f.t, err)
	key := keys.MakeFamilyKey(pk, uint32(m.descr.TableDesc().Families[0].ID))
	for {
		m, haveMoreRows := f.Next()
		if bytes.Equal(m.kv.Key, key) {
			return m
		}
		require.True(f.t, haveMoreRows)
	}
}

// tenantState maintains test state related to tenat.
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

	rows, err := conn.Query(create, args...)
	require.NoError(t, err)
	return &replicationFeed{
		t:    t,
		conn: conn,
		rows: rows,
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
CREATE DATABASE foo;
CREATE TABLE foo.bar(i int primary key, a string, b string);
INSERT INTO foo.bar (i) VALUES (42), (43);
INSERT INTO foo.bar (i, a, b) VALUES (1, 'hello', 'world'), (2, 'bye', 'covid');
`)

	streamTenantQuery := fmt.Sprintf(
		`CREATE REPLICATION STREAM FOR TENANT %d`, h.tenant.id.ToUint64())

	// Cannot replicate stream from inside the tenant
	_, err := h.tenant.sql.DB.ExecContext(context.Background(), streamTenantQuery)
	require.Error(t, err)

	feed := h.StartReplication(t, streamTenantQuery)
	defer feed.Close()

	fm := feedMatcher{
		t:     t,
		descr: catalogkv.TestingGetTableDescriptor(h.sysServer.DB(), h.tenant.codec, "foo", "bar"),
		codec: h.tenant.codec,
	}

	orig := fm.ObserveKey(feed, 42)

	// Update.
	h.tenant.sql.Exec(t, `UPDATE foo.bar SET a = 'blah' WHERE i = 42`)
	updated := fm.ObserveKey(feed, 42)

	require.True(t, orig.kv.Value.Timestamp.Less(updated.kv.Value.Timestamp))
}
