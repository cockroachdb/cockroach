// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package copy

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyFromNullInfNaN(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT NULL,
			f FLOAT NULL,
			s STRING NULL,
			b BYTES NULL,
			d DATE NULL,
			t TIME NULL,
			ttz TIME NULL,
			ts TIMESTAMP NULL,
			n INTERVAL NULL,
			o BOOL NULL,
			e DECIMAL NULL,
			u UUID NULL,
			ip INET NULL,
			tz TIMESTAMPTZ NULL,
			geography GEOGRAPHY NULL,
			geometry GEOMETRY NULL,
			box2d BOX2D NULL
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(
		"t", "i", "f", "s", "b", "d", "t", "ttz",
		"ts", "n", "o", "e", "u", "ip", "tz", "geography", "geometry", "box2d"))
	if err != nil {
		t.Fatal(err)
	}

	input := [][]interface{}{
		{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.Inf(-1), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
		{nil, math.NaN(), nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil},
	}

	for _, in := range input {
		_, err = stmt.Exec(in...)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err = stmt.Exec()
	if err != nil {
		t.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for row, in := range input {
		if !rows.Next() {
			t.Fatal("expected more results")
		}
		data := make([]interface{}, len(in))
		for i := range data {
			data[i] = new(interface{})
		}
		if err := rows.Scan(data...); err != nil {
			t.Fatal(err)
		}
		for i, d := range data {
			v := d.(*interface{})
			d = *v
			if a, b := fmt.Sprintf("%#v", d), fmt.Sprintf("%#v", in[i]); a != b {
				t.Fatalf("row %v, col %v: got %#v (%T), expected %#v (%T)", row, i, d, d, in[i], in[i])
			}
		}
	}
}

// TestCopyFromRandom inserts random rows using COPY and ensures the SELECT'd
// data is the same.
func TestCopyFromRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Disable pipelining. Without this, pipelined writes performed as part of the
	// COPY can be lost, which can then cause the COPY to fail.
	kvcoord.PipelinedWritesEnabled.Override(ctx, &s.ClusterSettings().SV, false)

	if _, err := db.Exec(`
		CREATE DATABASE d;
		CREATE TABLE IF NOT EXISTS d.t (
			id INT PRIMARY KEY,
			n INTERVAL,
			cs TEXT COLLATE en_us_u_ks_level2,
			o BOOL,
			i INT,
			i2 INT2,
			i4 INT4,
			f FLOAT,
			e DECIMAL,
			t TIME,
			ttz TIMETZ,
			ts TIMESTAMP,
			s STRING,
			b BYTES,
			u UUID,
			ip INET,
			tz TIMESTAMPTZ,
			geography GEOGRAPHY NULL,
			geometry GEOMETRY NULL,
			box2d BOX2D NULL
		);
		SET extra_float_digits = 3; -- to preserve floats entirely
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyInSchema("d", "t", "id", "n", "cs", "o", "i", "i2", "i4", "f", "e", "t", "ttz", "ts", "s", "b", "u", "ip", "tz", "geography", "geometry", "box2d"))
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(0))
	typs := []*types.T{
		types.Int,
		types.Interval,
		types.MakeCollatedString(types.String, "en_us_u_ks_level2"),
		types.Bool,
		types.Int,
		types.Int2,
		types.Int4,
		types.Float,
		types.Decimal,
		types.Time,
		types.TimeTZ,
		types.Timestamp,
		types.String,
		types.Bytes,
		types.Uuid,
		types.INet,
		types.TimestampTZ,
		types.Geography,
		types.Geometry,
		types.Box2D,
	}

	var inputs [][]interface{}

	for i := 0; i < 1000; i++ {
		row := make([]interface{}, len(typs))
		for j, t := range typs {
			var ds string
			if j == 0 {
				// Special handling for ID field
				ds = strconv.Itoa(i)
			} else {
				d := randgen.RandDatum(rng, t, false)
				ds = tree.AsStringWithFlags(d, tree.FmtBareStrings)
				switch t.Family() {
				case types.CollatedStringFamily:
					// For collated strings, we just want the raw contents in COPY.
					ds = d.(*tree.DCollatedString).Contents
				case types.FloatFamily:
					ds = strings.TrimSuffix(ds, ".0")
				}
			}
			row[j] = ds
		}
		_, err = stmt.Exec(row...)
		if err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, row)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = txn.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("SELECT * FROM d.t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	for row, in := range inputs {
		if !rows.Next() {
			t.Fatal("expected more results")
		}
		data := make([]interface{}, len(in))
		for i := range data {
			data[i] = new(interface{})
		}
		if err := rows.Scan(data...); err != nil {
			t.Fatal(err)
		}
		for i, d := range data {
			v := d.(*interface{})
			d := *v
			ds := fmt.Sprint(d)
			switch d := d.(type) {
			case []byte:
				ds = string(d)
			case time.Time:
				var dt tree.NodeFormatter
				if typs[i].Family() == types.TimeFamily {
					dt = tree.MakeDTime(timeofday.FromTimeAllow2400(d))
				} else if typs[i].Family() == types.TimeTZFamily {
					dt = tree.NewDTimeTZ(timetz.MakeTimeTZFromTimeAllow2400(d))
				} else if typs[i].Family() == types.TimestampFamily {
					dt = tree.MustMakeDTimestamp(d, time.Microsecond)
				} else {
					dt = tree.MustMakeDTimestampTZ(d, time.Microsecond)
				}
				ds = tree.AsStringWithFlags(dt, tree.FmtBareStrings)
			}
			if !reflect.DeepEqual(in[i], ds) {
				t.Fatalf("row %v, col %v: got %#v (%T), expected %#v", row, i, ds, d, in[i])
			}
		}
	}
}

// TestCopyFromBinary uses the pgx driver, which hard codes COPY ... BINARY.
func TestCopyFromBinary(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sqlDB := sqlutils.MakeSQLRunner(db)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	pgURL, cleanupGoDB := s.PGUrl(
		t, serverutils.CertsDirPrefix("StartServer"), serverutils.User(username.RootUser),
	)
	defer cleanupGoDB()
	conn, err := pgx.Connect(ctx, pgURL.String())
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec(ctx, `
		CREATE TABLE t (
			id INT8 PRIMARY KEY,
			u INT, -- NULL test
			o BOOL,
			i2 INT2,
			i4 INT4,
			i8 INT8,
			f FLOAT,
			s STRING,
			b BYTES
		);
	`); err != nil {
		t.Fatal(err)
	}

	input := [][]interface{}{{
		1,
		nil,
		true,
		int16(1),
		int32(1),
		int64(1),
		float64(1),
		"s",
		"b",
	}}
	if _, err = conn.CopyFrom(
		ctx,
		pgx.Identifier{"t"},
		[]string{"id", "u", "o", "i2", "i4", "i8", "f", "s", "b"},
		pgx.CopyFromRows(input),
	); err != nil {
		t.Fatal(err)
	}

	expect := func() [][]string {
		row := make([]string, len(input[0]))
		for i, v := range input[0] {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		return [][]string{row}
	}()
	sqlDB.CheckQueryResults(t, "SELECT * FROM t ORDER BY id", expect)
}

func TestCopyFromError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
	if err != nil {
		t.Fatal(err)
	}

	// Insert conflicting primary keys.
	for i := 0; i < 2; i++ {
		_, err = stmt.Exec(1)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = stmt.Close()
	if err == nil {
		t.Fatal("expected error")
	}

	// Make sure we can query after an error.
	var i int
	if err := db.QueryRow("SELECT 1").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != 1 {
		t.Fatalf("expected 1, got %d", i)
	}
	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestCopyFromRetries tests copy from works as expected for certain retry
// states.
func TestCopyFromRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Ensure that the COPY batch size isn't too large (this test becomes too
	// slow when metamorphic sql.CopyBatchRowSize is set to a relatively large
	// number).
	const maxCopyBatchRowSize = 1000
	if sql.CopyBatchRowSize > maxCopyBatchRowSize {
		sql.SetCopyFromBatchSize(maxCopyBatchRowSize)
	}

	// sql.CopyBatchRowSize can change depending on the metamorphic
	// randomization, so we derive all rows counts from it.
	var numRows = sql.CopyBatchRowSize * 5

	testCases := []struct {
		desc               string
		hookBefore         func(attemptNum int) error
		hookAfter          func(attemptNum int) error
		atomicEnabled      bool
		retriesEnabled     bool
		autoCommitDisabled bool
		inTxn              bool
		expectedRows       int
		expectedErr        bool
	}{
		{
			desc:           "failure in atomic transaction does not retry",
			atomicEnabled:  true,
			retriesEnabled: true,
			hookBefore: func(attemptNum int) error {
				if attemptNum == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedErr: true,
		},
		{
			desc:           "does not attempt to retry if disabled",
			atomicEnabled:  false,
			retriesEnabled: false,
			hookBefore: func(attemptNum int) error {
				if attemptNum == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedErr: true,
		},
		{
			desc:           "does not retry inside a txn",
			atomicEnabled:  true,
			retriesEnabled: true,
			inTxn:          true,
			hookBefore: func(attemptNum int) error {
				if attemptNum == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedErr: true,
		},
		{
			desc:           "retries successfully on every batch (errors before)",
			atomicEnabled:  false,
			retriesEnabled: true,
			hookBefore: func(attemptNum int) error {
				if attemptNum%2 == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedRows: numRows,
		},
		{
			// In this scenario, only the first COPY batch is auto-committed,
			// after which we inject a retryable error. The txn cannot be rolled
			// back, so the COPY then returns that error to the client, yet the
			// first batch of rows is already in the table.
			desc:           "cannot roll back committed txn (errors after)",
			atomicEnabled:  false,
			retriesEnabled: true,
			hookAfter: func(attemptNum int) error {
				if attemptNum%2 == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedRows: sql.CopyBatchRowSize,
			expectedErr:  true,
		},
		{
			// When auto commit is disabled, each COPY batch ends being
			// processed twice - after the first attempt on each batch we inject
			// a retryable error, which is then successfully retried on the
			// second attempt.
			desc:               "retries successfully on every batch (errors after, no auto commit)",
			autoCommitDisabled: true,
			atomicEnabled:      false,
			retriesEnabled:     true,
			hookAfter: func(attemptNum int) error {
				if attemptNum%2 == 1 {
					return &kvpb.TransactionRetryWithProtoRefreshError{}
				}
				return nil
			},
			expectedRows: numRows,
		},
		{
			desc:           "eventually dies on too many restarts",
			atomicEnabled:  false,
			retriesEnabled: true,
			hookBefore: func(attemptNum int) error {
				return &kvpb.TransactionRetryWithProtoRefreshError{}
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			knobs := &sql.ExecutorTestingKnobs{
				DisableAutoCommitDuringExec: tc.autoCommitDisabled,
			}
			var params base.TestServerArgs
			params.Knobs.SQLExecutor = knobs
			if tc.hookBefore != nil {
				var attemptNumber int
				knobs.CopyFromInsertBeforeBatch = func(txn *kv.Txn) error {
					if !tc.inTxn {
						// When we're not in an explicit txn, we expect that
						// all txns used by the COPY use the background QoS.
						if txn.AdmissionHeader().Priority != int32(admissionpb.UserLowPri) {
							t.Errorf(
								"unexpected QoS level %d (expected %d)",
								txn.AdmissionHeader().Priority, admissionpb.UserLowPri,
							)
						}
					}
					attemptNumber++
					return tc.hookBefore(attemptNumber)
				}
			}
			if tc.hookAfter != nil {
				var attemptNumber int
				knobs.CopyFromInsertAfterBatch = func() error {
					attemptNumber++
					return tc.hookAfter(attemptNumber)
				}
			}
			srv, db, _ := serverutils.StartServer(t, params)
			defer srv.Stopper().Stop(context.Background())
			s := srv.ApplicationLayer()

			_, err := db.Exec(
				`CREATE TABLE t (
					i INT PRIMARY KEY
				);`,
			)
			require.NoError(t, err)

			ctx := context.Background()

			// Use pgx instead of lib/pq as pgx doesn't require copy to be in a txn.
			pgURL, cleanupGoDB := s.PGUrl(
				t, serverutils.CertsDirPrefix("StartServer"), serverutils.User(username.RootUser),
			)
			defer cleanupGoDB()
			pgxConn, err := pgx.Connect(ctx, pgURL.String())
			require.NoError(t, err)
			_, err = pgxConn.Exec(ctx, "SET copy_from_atomic_enabled = $1", fmt.Sprintf("%t", tc.atomicEnabled))
			require.NoError(t, err)
			_, err = pgxConn.Exec(ctx, "SET copy_from_retries_enabled = $1", fmt.Sprintf("%t", tc.retriesEnabled))
			require.NoError(t, err)

			if err := func() error {
				var rows [][]interface{}
				for i := 0; i < numRows; i++ {
					rows = append(rows, []interface{}{i})
				}
				if !tc.inTxn {
					_, err := pgxConn.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"i"}, pgx.CopyFromRows(rows))
					return err
				}

				txn, err := pgxConn.Begin(ctx)
				if err != nil {
					return err
				}
				defer func() {
					_ = txn.Rollback(ctx)
				}()
				_, err = txn.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"i"}, pgx.CopyFromRows(rows))
				if err != nil {
					return err
				}
				return txn.Commit(ctx)
			}(); err != nil {
				assert.True(t, tc.expectedErr, "got error %+v", err)
			} else {
				assert.False(t, tc.expectedErr, "expected error but got none")
			}

			var actualRows int
			err = db.QueryRow("SELECT count(1) FROM t").Scan(&actualRows)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRows, actualRows)
		})
	}
}

// TestCopyTransaction verifies that COPY data can be used after it is done
// within a transaction.
func TestCopyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`); err != nil {
		t.Fatal(err)
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	// Note that, at least with lib/pq, this doesn't actually send a Parse msg
	// (which we wouldn't support, as we don't support Copy-in in extended
	// protocol mode). lib/pq has magic for recognizing a Copy.
	stmt, err := txn.Prepare(pq.CopyIn("t", "i"))
	if err != nil {
		t.Fatal(err)
	}

	const val = 2

	_, err = stmt.Exec(val)
	if err != nil {
		t.Fatal(err)
	}

	if err = stmt.Close(); err != nil {
		t.Fatal(err)
	}

	var i int
	if err := txn.QueryRow("SELECT i FROM d.t").Scan(&i); err != nil {
		t.Fatal(err)
	} else if i != val {
		t.Fatalf("expected 1, got %d", i)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
}

// TestCopyFromFKCheck verifies that foreign keys are checked during COPY.
func TestCopyFromFKCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	db.SetMaxOpenConns(1)
	r := sqlutils.MakeSQLRunner(db)
	r.Exec(t, `
		CREATE DATABASE d;
		SET DATABASE = d;
		CREATE TABLE p (p INT PRIMARY KEY);
		CREATE TABLE t (
		  a INT PRIMARY KEY,
		  p INT REFERENCES p(p)
		);
	`)

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = txn.Rollback() }()

	stmt, err := txn.Prepare(pq.CopyIn("t", "a", "p"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = stmt.Exec(1, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = stmt.Close()
	if !testutils.IsError(err, "foreign key violation|violates foreign key constraint") {
		t.Fatalf("expected FK error, got: %v", err)
	}
}

// TestCopyInReleasesLeases is a regression test to ensure that the execution
// of CopyIn does not retain table descriptor leases after completing by
// attempting to run a schema change after performing a copy.
func TestCopyInReleasesLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, `CREATE TABLE t (k INT8 PRIMARY KEY)`)
	tdb.Exec(t, `CREATE USER foo WITH PASSWORD 'testabc'`)
	tdb.Exec(t, `GRANT admin TO foo`)

	userURL, cleanupFn := s.PGUrl(t,
		serverutils.CertsDirPrefix(t.Name()),
		serverutils.UserPassword("foo", "testabc"),
		serverutils.ClientCerts(false),
	)
	defer cleanupFn()
	conn, err := sqltestutils.PGXConn(t, userURL)
	require.NoError(t, err)

	rowsAffected, err := conn.CopyFrom(
		ctx,
		pgx.Identifier{"t"},
		[]string{"k"},
		pgx.CopyFromRows([][]interface{}{{1}, {2}}),
	)
	require.NoError(t, err)
	require.Equal(t, int64(2), rowsAffected)

	// Prior to the bug fix which prompted this test, the below schema change
	// would hang until the leases expire. Let's make sure it finishes "soon".
	alterErr := make(chan error, 1)
	go func() {
		_, err := db.Exec(`ALTER TABLE t ADD COLUMN v INT NOT NULL DEFAULT 0`)
		alterErr <- err
	}()
	select {
	case err := <-alterErr:
		require.NoError(t, err)
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatal("alter did not complete")
	}
	require.NoError(t, conn.Close(ctx))
}

func TestMessageSizeTooBig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	url, cleanup := pgurlutils.PGUrl(t, srv.ApplicationLayer().AdvSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, "CREATE TABLE t (s STRING)")
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()
	str := randutil.RandString(rng, (2<<20)+1, "asdf")

	var sb strings.Builder
	for i := 0; i < 4; i++ {
		sb.WriteString(str)
		sb.WriteString("\n")
	}

	_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY t FROM STDIN")
	require.NoError(t, err)

	// pgx uses a 64kb buffer
	err = conn.Exec(ctx, "SET CLUSTER SETTING sql.conn.max_read_buffer_message_size = '60KiB'")
	require.NoError(t, err)

	_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY t FROM STDIN")
	require.ErrorContains(t, err, "message size 64 KiB bigger than maximum allowed message size 60 KiB")
}

// Test that a big copy results in an error and not crash.
func TestCopyExceedsSQLMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, v := range []string{"on", "off"} {
		for _, a := range []string{"on", "off"} {
			for _, f := range []string{"on", "off"} {
				t.Run(fmt.Sprintf("vector=%v/atomic=%v/fastpath=%v", v, a, f), func(t *testing.T) {
					defer log.Scope(t).Close(t)

					ctx := context.Background()
					// Sometimes startup fails with lower than 10MiB.
					params := base.TestServerArgs{
						SQLMemoryPoolSize: 10 << 20,
					}
					srv := serverutils.StartServerOnly(t, params)
					defer srv.Stopper().Stop(ctx)

					s := srv.ApplicationLayer()

					url, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), "copytest", url.User(username.RootUser))
					defer cleanup()
					var sqlConnCtx clisqlclient.Context
					conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

					err := conn.Exec(ctx, fmt.Sprintf(`SET VECTORIZE='%s'`, v))
					require.NoError(t, err)

					err = conn.Exec(ctx, fmt.Sprintf(`SET COPY_FAST_PATH_ENABLED='%s'`, f))
					require.NoError(t, err)

					err = conn.Exec(ctx, fmt.Sprintf(`SET COPY_FROM_ATOMIC_ENABLED='%s'`, a))
					require.NoError(t, err)

					err = conn.Exec(ctx, "CREATE TABLE t (s STRING)")
					require.NoError(t, err)

					rng, _ := randutil.NewTestRand()
					str := randutil.RandString(rng, 11<<20, "asdf")

					var sb strings.Builder
					for i := 0; i < 3; i++ {
						sb.WriteString(str)
						sb.WriteString("\n")
					}

					_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY t FROM STDIN")
					require.Error(t, err)
				})
			}
		}
	}
}
