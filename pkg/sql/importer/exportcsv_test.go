// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer_test

import (
	"bytes"
	"compress/gzip"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/cockroach/pkg/workload/workloadsql"
	"github.com/gogo/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

const exportFilePattern = "export*-n*.0.csv"

func setupExportableBank(t *testing.T, nodes, rows int) (*sqlutils.SQLRunner, string, func()) {
	ctx := context.Background()
	dir, cleanupDir := testutils.TempDir(t)

	tc := testcluster.StartTestCluster(t, nodes,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				ExternalIODir: dir,
			},
		},
	)
	s := tc.ApplicationLayer(0)
	tenantSettings := s.ClusterSettings()
	conn := s.SQLConn(t, serverutils.DBName("defaultdb"))
	db := sqlutils.MakeSQLRunner(conn)

	wk := bank.FromRows(rows)
	l := workloadsql.InsertsDataLoader{BatchSize: 100, Concurrency: 3}
	if _, err := workloadsql.Setup(ctx, conn, wk, l); err != nil {
		t.Fatal(err)
	}

	config.TestingSetupZoneConfigHook(tc.Stopper())
	idgen := descidgen.NewGenerator(tenantSettings, s.Codec(), s.DB())
	v, err := idgen.PeekNextUniqueDescID(ctx)
	if err != nil {
		t.Fatal(err)
	}

	last := config.ObjectID(v)
	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(5000)
	config.TestingSetZoneConfig(last+1, zoneConfig)
	db.Exec(t, "ALTER TABLE bank SCATTER")
	db.Exec(t, "SELECT 'force a scan to repopulate range cache' FROM [SELECT count(*) FROM bank]")

	return db, dir, func() {
		tc.Stopper().Stop(ctx)
		cleanupDir()
	}
}

func TestExportImportBank(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "times out")

	const nodes = 3
	db, _, cleanup := setupExportableBank(t, nodes, 100)
	defer cleanup()

	// Add some unicode to prove FmtExport works as advertised.
	db.Exec(t, "UPDATE bank SET payload = payload || '✅' WHERE id = 5")
	db.Exec(t, "UPDATE bank SET payload = NULL WHERE id % 2 = 0")

	chunkSize := 13
	baseExportDir := "userfile:///t/"
	for _, null := range []string{"", "NULL"} {
		nullAs := fmt.Sprintf(", nullas = '%s'", null)
		nullIf := fmt.Sprintf(", nullif = '%s'", null)

		t.Run("null="+null, func(t *testing.T) {
			exportDir := filepath.Join(baseExportDir, t.Name())

			var asOf string
			db.QueryRow(t, "SELECT cluster_logical_timestamp()").Scan(&asOf)

			db.Exec(t,
				fmt.Sprintf(`EXPORT INTO CSV $1
				WITH chunk_rows = $2, delimiter = '|' %s
				FROM SELECT * FROM bank AS OF SYSTEM TIME %s`, nullAs, asOf), exportDir, chunkSize,
			)

			schema := bank.FromRows(1).Tables()[0].Schema
			exportedFiles := filepath.Join(exportDir, "*")
			db.Exec(t, fmt.Sprintf("CREATE TABLE bank2 %s", schema))
			defer db.Exec(t, "DROP TABLE bank2")
			db.Exec(t, fmt.Sprintf(`IMPORT INTO bank2 CSV DATA ($1) WITH delimiter = '|'%s`, nullIf), exportedFiles)

			db.CheckQueryResults(t,
				fmt.Sprintf(`SELECT * FROM bank AS OF SYSTEM TIME %s ORDER BY id`, asOf), db.QueryStr(t, `SELECT * FROM bank2 ORDER BY id`),
			)
			db.CheckQueryResults(t,
				`SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank2]`,
				db.QueryStr(t, `SELECT fingerprint FROM [SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE bank]`),
			)
		})
	}
}

// Tests if user does not specify nullas option and imports null data, an error is raised.
func TestExportNullWithEmptyNullAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	db := sqlutils.MakeSQLRunner(conn)

	// Set up dummy accounts table with NULL value
	db.Exec(t, `
		CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);
		INSERT INTO accounts VALUES (1, NULL), (2, 8);
	`)

	// Case when `nullas` option is unspecified: expect error
	const stmtWithoutNullas = "EXPORT INTO CSV 'nodelocal://1/t' FROM SELECT * FROM accounts"
	db.ExpectErr(t, "NULL value encountered during EXPORT, "+
		"use `WITH nullas` to specify the string representation of NULL", stmtWithoutNullas)

	// Case when `nullas` option is specified: operation is successful and NULLs are encoded to "None"
	const stmtWithNullas = `EXPORT INTO CSV 'nodelocal://1/t' WITH nullas="None" FROM SELECT * FROM accounts`
	db.Exec(t, stmtWithNullas)
	contents := readFileByGlob(t, filepath.Join(dir, "t", exportFilePattern))
	require.Equal(t, "1,None\n2,8\n", string(contents))

	// Verify successful IMPORT statement `WITH nullif="None"` to complete round trip
	const importStmt = `IMPORT INTO accounts2 CSV DATA ('nodelocal://1/t/export*-n*.0.csv') WITH nullif="None"`
	db.Exec(t, `CREATE TABLE accounts2(id INT PRIMARY KEY, balance INT)`)
	db.Exec(t, importStmt)
	db.CheckQueryResults(t,
		"SELECT * FROM accounts2", db.QueryStr(t, "SELECT * FROM accounts"),
	)
}

func TestMultiNodeExportStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test stalls under race")

	nodes := 5
	exportRows := 100
	db, _, cleanup := setupExportableBank(t, nodes, exportRows*2)
	defer cleanup()

	maxTries := 10
	// we might need to retry if our table didn't actually scatter enough.
	for tries := 0; tries < maxTries; tries++ {
		chunkSize := 13
		rows := db.Query(t,
			`EXPORT INTO CSV 'nodelocal://1/t' WITH chunk_rows = $3 FROM SELECT * FROM bank WHERE id >= $1 and id < $2`,
			10, 10+exportRows, chunkSize,
		)

		files, totalRows, totalBytes := 0, 0, 0
		nodesSeen := make(map[string]bool)
		for rows.Next() {
			filename, count, bytes := "", 0, 0
			if err := rows.Scan(&filename, &count, &bytes); err != nil {
				t.Fatal(err)
			}
			files++
			if count > chunkSize {
				t.Fatalf("expected no chunk larger than %d, got %d", chunkSize, count)
			}
			totalRows += count
			totalBytes += bytes
			nodesSeen[strings.SplitN(filename, ".", 2)[0]] = true
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("unexpected error during export: %s", err.Error())
		}
		if totalRows != exportRows {
			t.Fatalf("expected %d rows, got %d", exportRows, totalRows)
		}
		if expected := exportRows / chunkSize; files < expected {
			t.Fatalf("expected at least %d files, got %d", expected, files)
		}
		if len(nodesSeen) < 2 {
			// table isn't as scattered as we expected, but we can try again.
			if tries < maxTries {
				continue
			}
			t.Fatalf("expected files from %d nodes, got %d: %v", 2, len(nodesSeen), nodesSeen)
		}
		break
	}
}

func TestExportJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t AS VALUES (1, 2)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/join' FROM SELECT * FROM t, t as u`)
}

func readFileByGlob(t *testing.T, pattern string) []byte {
	paths, err := filepath.Glob(pattern)
	require.NoError(t, err)

	require.Equal(t, 1, len(paths))

	result, err := os.ReadFile(paths[0])
	require.NoError(t, err)

	return result
}

func TestExportOrder(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/order' FROM SELECT * FROM foo ORDER BY y ASC LIMIT 2`)
	content := readFileByGlob(t, filepath.Join(dir, "order", exportFilePattern))

	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportUniqueness(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	const stmt = `EXPORT INTO CSV 'nodelocal://1/' WITH chunk_rows=$1 FROM SELECT * FROM foo`

	sqlDB.Exec(t, stmt, 2)
	dir1, err := os.ReadDir(dir)
	require.NoError(t, err)

	sqlDB.Exec(t, stmt, 2)
	dir2, err := os.ReadDir(dir)
	require.NoError(t, err)

	require.Equal(t, 2*len(dir1), len(dir2), "second export did not double the number of files")
}

func TestExportUserDefinedTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Set up some initial state for the tests.
	sqlDB.Exec(t, `
CREATE TYPE greeting AS ENUM ('hello', 'hi');
CREATE TABLE greeting_table (x greeting, y greeting);
INSERT INTO greeting_table VALUES ('hello', 'hello'), ('hi', 'hi');
`)
	tests := []struct {
		stmt     string
		expected string
	}{
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://1/%s/' FROM (SELECT 'hello':::greeting, 'hi':::greeting)",
			expected: "hello,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://1/%s/' FROM TABLE greeting_table",
			expected: "hello,hello\nhi,hi\n",
		},
		{
			stmt:     "EXPORT INTO CSV 'nodelocal://1/%s/' FROM (SELECT x, y, enum_first(x) FROM greeting_table)",
			expected: "hello,hello,hello\nhi,hi,hello\n",
		},
	}
	for i, test := range tests {
		path := fmt.Sprintf("test%d", i)
		stmt := fmt.Sprintf(test.stmt, path)

		sqlDB.Exec(t, stmt)

		// Read the dumped file.
		contents := readFileByGlob(t, filepath.Join(dir, path, exportFilePattern))

		require.Equal(t, test.expected, string(contents))
	}
}

func TestExportOrderCompressed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	close := func(c io.Closer) {
		if err := c.Close(); err != nil {
			t.Fatalf("failed to close stream, got error %s", err)
		}
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE foo (i INT PRIMARY KEY, x INT, y INT, z INT, INDEX (y))`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 12, 3, 14), (2, 22, 2, 24), (3, 32, 1, 34)`)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/order' with compression = gzip from select * from foo order by y asc limit 2`)
	compressed := readFileByGlob(t, filepath.Join(dir, "order", exportFilePattern+".gz"))

	gzipReader, err := gzip.NewReader(bytes.NewReader(compressed))
	defer close(gzipReader)

	require.NoError(t, err)

	content, err := io.ReadAll(gzipReader)
	require.NoError(t, err)

	if expected, got := "3,32,1,34\n2,22,2,24\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestExportShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/show' FROM SELECT database_name, owner FROM [SHOW DATABASES] ORDER BY database_name`)
	content := readFileByGlob(t, filepath.Join(dir, "show", exportFilePattern))

	if expected, got := "defaultdb,"+username.RootUser+"\npostgres,"+username.RootUser+"\nsystem,"+
		username.NodeUser+"\n", string(content); expected != got {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

// TestExportVectorized makes sure that SupportsVectorized check doesn't panic
// on CSVWriter processor.
func TestExportVectorized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE t(a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'http://0.1:37957/exp_1' FROM TABLE t`)
}

// TestExportFeatureFlag tests the feature flag logic that allows the EXPORT
// command to be toggled off via cluster settings.
func TestExportFeatureFlag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Feature flag is off — test that EXPORT surfaces error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.export.enabled = FALSE`)
	sqlDB.Exec(t, `CREATE TABLE feature_flags (a INT PRIMARY KEY)`)
	sqlDB.ExpectErr(t, `feature EXPORT was disabled by the database administrator`,
		`EXPORT INTO CSV 'nodelocal://1/foo/' FROM TABLE feature_flags`)

	// Feature flag is on — test that EXPORT does not error.
	sqlDB.Exec(t, `SET CLUSTER SETTING feature.export.enabled = TRUE`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/foo/' FROM TABLE feature_flags`)
}

func TestExportPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER testuser`)
	sqlDB.Exec(t, `CREATE TABLE privs (a INT)`)

	testuser := srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))
	_, err := testuser.Exec(`EXPORT INTO CSV 'nodelocal://1/privs' FROM TABLE privs`)
	require.True(t, testutils.IsError(err, "testuser does not have SELECT privilege"))

	dest := "nodelocal://1/privs_placeholder"
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.True(t, testutils.IsError(err, "testuser does not have SELECT privilege"))

	// The below SELECT GRANT hangs if we leave the user conn open.
	_ = testuser.Close()

	// Grant SELECT privilege.
	sqlDB.Exec(t, `GRANT SELECT ON TABLE privs TO testuser`)

	testuser = srv.ApplicationLayer().SQLConn(t, serverutils.User("testuser"))

	_, err = testuser.Exec(`EXPORT INTO CSV 'nodelocal://1/privs' FROM TABLE privs`)
	require.True(t, testutils.IsError(err,
		"only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access the specified nodelocal URI"))
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.True(t, testutils.IsError(err,
		"only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access the specified nodelocal URI"))

	sqlDB.Exec(t, `GRANT ADMIN TO testuser`)

	_, err = testuser.Exec(`EXPORT INTO CSV 'nodelocal://1/privs' FROM TABLE privs`)
	require.NoError(t, err)
	_, err = testuser.Exec(`EXPORT INTO CSV $1 FROM TABLE privs`, dest)
	require.NoError(t, err)
}

func TestExportTargetFileSizeSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: dir}})
	defer tc.Stopper().Stop(ctx)

	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/foo' WITH chunk_size='10KB' FROM select i, gen_random_uuid() from generate_series(1, 4000) as i;`)
	files, err := os.ReadDir(filepath.Join(dir, "foo"))
	require.NoError(t, err)
	require.Equal(t, 14, len(files))

	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/foo-compressed' WITH chunk_size='10KB',compression='gzip' FROM select i, gen_random_uuid() from generate_series(1, 4000) as i;`)
	zipFiles, err := os.ReadDir(filepath.Join(dir, "foo-compressed"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(zipFiles), 6)
}

func populateRangeCache(t *testing.T, db *gosql.DB, tableName string) {
	t.Helper()
	_, err := db.Exec("SELECT count(1) FROM " + tableName)
	require.NoError(t, err)
}

func getPGXConnAndCleanupFunc(
	ctx context.Context, t *testing.T, app serverutils.ApplicationLayerInterface,
) (*pgx.Conn, func()) {
	t.Helper()
	pgURL, cleanup := app.PGUrl(t, serverutils.CertsDirPrefix(t.Name()), serverutils.User(username.RootUser))
	pgURL.Path = "test"
	pgxConfig, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	defaultConn, err := pgx.ConnectConfig(ctx, pgxConfig)
	require.NoError(t, err)
	_, err = defaultConn.Exec(ctx, "set distsql='always'")
	require.NoError(t, err)
	return defaultConn, cleanup
}

// Test that processors either returns or retries a
// ReadWithinUncertaintyIntervalError encountered by a remote node.
//
// The following test and related helper functions is based on
// TestDrainingProcessorSwallowsUncertaintyError in pkg/sql/rowexec.
func TestProcessorEncountersUncertaintyError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We are going to EXPORT a table with 10 rows, with rows 1..5 located on node
	// 0 and nodes 6..10 on node 1.
	var (
		// trapRead is set atomically once the test wants to block a read on the
		// second node.
		trapRead    int64
		blockedRead struct {
			syncutil.Mutex
			unblockCond   *sync.Cond
			shouldUnblock bool
		}
		allowOneWrite    chan struct{}
		gotRWUIOnGateway chan struct{}
	)

	unblockRead := func() {
		blockedRead.Lock()
		// Set shouldUnblock to true to have any reads that would block return
		// an uncertainty error. Signal the cond to wake up any reads that have
		// already been blocked.
		blockedRead.shouldUnblock = true
		blockedRead.unblockCond.Signal()
		blockedRead.Unlock()
	}

	waitForUnblock := func() {
		blockedRead.Lock()
		for !blockedRead.shouldUnblock {
			blockedRead.unblockCond.Wait()
		}
		blockedRead.Unlock()
	}

	blockedRead.unblockCond = sync.NewCond(&blockedRead.Mutex)

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			<-allowOneWrite
			_, _ = io.Copy(io.Discard, r.Body)
		}
	}))
	defer s.Close()

	tc := serverutils.StartCluster(t, 3, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
			},
			ServerArgsPerNode: map[int]base.TestServerArgs{
				0: {
					Knobs: base.TestingKnobs{
						SQLExecutor: &sql.ExecutorTestingKnobs{
							DistSQLReceiverPushCallbackFactory: func(_ context.Context, query string) func(rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
								if strings.Contains(query, "EXPORT") {
									return func(row rowenc.EncDatumRow, batch coldata.Batch, meta *execinfrapb.ProducerMetadata) (rowenc.EncDatumRow, coldata.Batch, *execinfrapb.ProducerMetadata) {
										if meta != nil && meta.Err != nil {
											if testutils.IsError(meta.Err, "ReadWithinUncertaintyIntervalError") {
												close(gotRWUIOnGateway)
											}
										}
										return row, batch, meta
									}
								}
								return nil
							},
						},
					},
					UseDatabase: "test",
				},
				1: {
					Knobs: base.TestingKnobs{

						Store: &kvserver.StoreTestingKnobs{
							TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
								if atomic.LoadInt64(&trapRead) == 0 {
									return nil
								}
								// We're going to trap a read for the rows [6,10].
								req, ok := ba.GetArg(kvpb.Scan)
								if !ok {
									return nil
								}
								key := req.(*kvpb.ScanRequest).Key.String()
								if strings.Contains(key, "/6") {
									waitForUnblock()
									return kvpb.NewError(
										kvpb.NewReadWithinUncertaintyIntervalError(
											ba.Timestamp,           /* readTs */
											hlc.ClockTimestamp{},   /* localUncertaintyLimit */
											ba.Txn,                 /* txn */
											ba.Timestamp.Add(1, 0), /* valueTS */
											hlc.ClockTimestamp{} /* localTS */))
								}
								return nil
							},
						},
					},
					UseDatabase: "test",
				},
			},
		})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)

	if tc.DefaultTenantDeploymentMode().IsExternal() {
		tc.GrantTenantCapabilities(ctx, t, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanAdminRelocateRange: "true"})
	}

	origDB0 := tc.ServerConn(0)

	sqlutils.CreateTable(t, origDB0, "t",
		"x INT PRIMARY KEY",
		10, /* numRows */
		sqlutils.ToRowFn(sqlutils.RowIdxFn))

	// Split the table and move half of the rows to the 2nd node.
	_, err := origDB0.Exec(fmt.Sprintf(`
	ALTER TABLE "t" SPLIT AT VALUES (6);
	ALTER TABLE "t" EXPERIMENTAL_RELOCATE VALUES (ARRAY[%d], 0), (ARRAY[%d], 6);
	`,
		tc.Server(0).GetFirstStoreID(),
		tc.Server(1).GetFirstStoreID()))
	require.NoError(t, err)
	populateRangeCache(t, origDB0, "t")

	t.Run("after result rows are emitted returns error", func(t *testing.T) {
		// - The export is issued on node 0.
		// - Node 1 is blocked on its read.
		// - Node 0 is allowed to read and write a single file to the sink. We do this
		//   to prevent an internal retry of the RWUI error.
		// - Node 0 is then blocked writing the next file.
		// - Node 1's read is unblocked and returns a RWUI error which eventually
		//   makes it to the gateway.
		//
		// - Node 0's file write is unblocked, at which point it should see a
		//   draining output and the RWUI error should be returned to the user.

		// This is buffered so we can unblock the first write in advance.
		allowOneWrite = make(chan struct{}, 1)
		gotRWUIOnGateway = make(chan struct{})
		// Disable results buffering - we want to ensure that the server doesn't do
		// any automatic retries, and also we use the client to know when to unblock
		// the read.
		// NB: The session variable for this doesn't support SET.
		_, err := origDB0.Exec("SET CLUSTER SETTING sql.defaults.results_buffer.size = '0'")
		require.NoError(t, err)
		// Create a new connection that will use the new result buffer size.
		defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.ApplicationLayer(0))
		defer cleanup()

		atomic.StoreInt64(&trapRead, 1)
		defer func() { atomic.StoreInt64(&trapRead, 0) }()
		allowOneWrite <- struct{}{}
		exportQuery := fmt.Sprintf("EXPORT INTO CSV '%s' WITH chunk_rows = '1' FROM SELECT * FROM t", s.URL)
		rows, err := defaultConn.Query(ctx, exportQuery)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			// Now that we've read one row, unblock the read on node 1. Eventually, we expect
			// a future EmitRow to fail.
			unblockRead()
			var (
				filename string
				rowCount int
				size     int
			)
			require.NoError(t, rows.Scan(&filename, &rowCount, &size))
			// Wait for the RWUI error to reach the distsql receiver on the gateway node.
			<-gotRWUIOnGateway
			// Unblock the next external storage write. The row emit that follows this should fail.
			allowOneWrite <- struct{}{}
		}
		err = rows.Err()
		require.Error(t, err)
		require.True(t, testutils.IsError(err, "ReadWithinUncertaintyIntervalError"))
	})

	t.Run("before result rows are emitted retries", func(t *testing.T) {
		// - The export is issued on node 0.
		// - Node 1 will immediately return a RWUI error.
		// - Node 0 is blocked writing the first file to external storage until the
		//   RWUI error makes it to the gateway.
		// - Node 0's file write is unblocked, at which point it should see a
		//   draining output and the RWUI error should be retried.
		// - On retry, no RWUI error is returned, so the export succeeds.

		allowOneWrite = make(chan struct{})
		gotRWUIOnGateway = make(chan struct{})
		// Add a large result buffer as an extra hedge against getting any results row.
		// NB: The session variable for this doesn't support SET.
		_, err := origDB0.Exec("SET CLUSTER SETTING sql.defaults.results_buffer.size = '524288'")
		require.NoError(t, err)
		// Create a new connection that will use the new results buffer size.
		defaultConn, cleanup := getPGXConnAndCleanupFunc(ctx, t, tc.ApplicationLayer(0))
		defer cleanup()

		// Reads are trapped but not blocked, so node 1 should immediately return a
		// RWUI error. Node 0 will be blocked waiting on allowOneWrite.
		atomic.StoreInt64(&trapRead, 1)
		unblockRead()

		count := 0
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			exportQuery := fmt.Sprintf("EXPORT INTO CSV '%s' WITH chunk_rows = '1' FROM SELECT * FROM t", s.URL)
			rows, err := defaultConn.Query(ctx, exportQuery)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var (
					filename string
					rowCount int
					size     int
				)
				if err := rows.Scan(&filename, &rowCount, &size); err != nil {
					return err
				}
				count++
			}
			return rows.Err()
		})
		<-gotRWUIOnGateway
		// After this error, untrap reads and expect that the retry succeeds.
		atomic.StoreInt64(&trapRead, 0)
		close(allowOneWrite)
		require.NoError(t, g.Wait())
		require.Equal(t, 10, count)
	})
}
