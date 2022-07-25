package changefeedccl

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/errors"
)

func BenchmarkChangefeedEventStruct(b *testing.B) {

	testfn := func(b *testing.B) {
		server, cleanupServer := startBenchmarkServer(b)
		defer cleanupServer()

		numRows := 100000
		if testing.Short() {
			numRows = 1 << 10
		}
		bankTable := bank.FromRows(numRows).Tables()[0]
		if err := createWorkloadTable(server.DB, bankTable); err != nil {
			b.Fatal(err)
		}

		feedFactory := makeKafkaFeedFactory(server.Server, server.DB)
		cf := feed(b, feedFactory, `CREATE CHANGEFEED FOR `+bankTable.Name)
		defer closeFeed(b, cf)

		readNextMessagesHelper := func(numMessages int) error {
			return withTimeout(cf, assertPayloadsTimeout(),
				func(ctx context.Context) error {
					if _, err := readNextMessages(ctx, cf, numMessages); err != nil {
						return err
					}
					return nil
				},
			)
		}

		runWorkload(b, server.DB, bankTable, readNextMessagesHelper)
	}

	for i := 0; i < b.N; i++ {
		b.ResetTimer()
		b.StopTimer()
		b.Run("runChangefeedBenchmark", testfn)
	}
}

func startBenchmarkServer(
	b *testing.B, testOpts ...feedTestOption,
) (testServer TestServerWithSystem, cleanup func()) {
	options := makeOptions(testOpts...)
	systemServer, systemDB, clusterCleanup := startTestFullServer(b, options)
	return TestServerWithSystem{
			TestServer: TestServer{
				DB:           systemDB,
				Server:       systemServer,
				TestingKnobs: systemServer.(*server.TestServer).Cfg.TestingKnobs,
				Codec:        keys.SystemSQLCodec,
			},
			SystemServer: systemServer,
			SystemDB:     systemDB,
		}, func() {
			clusterCleanup()
		}
}

func createWorkloadTable(sqlDB *gosql.DB, table workload.Table) error {
	if _, err := sqlDB.Exec(`CREATE TABLE "` + table.Name + `" ` + table.Schema); err != nil {
		return err
	}
	return nil
}

// Inserts data in batches into the provided table. Will benchmark the
func runWorkload(
	b *testing.B, sqlDB *gosql.DB, table workload.Table, consumeMessages func(int) error,
) {
	var numRows int
	var insertStmtBuf bytes.Buffer
	var params []interface{}
	for batchIdx := 0; batchIdx < table.InitialRows.NumBatches; batchIdx++ {
		if _, err := sqlDB.Exec(`BEGIN`); err != nil {
			b.Fatal(err)
		}

		params = params[:0]
		insertStmtBuf.Reset()
		insertStmtBuf.WriteString(`INSERT INTO "` + table.Name + `" VALUES `)

		var numRowsInBatch int
		for _, row := range table.InitialRows.BatchRows(batchIdx) {
			numRows++
			numRowsInBatch++
			if len(params) != 0 {
				insertStmtBuf.WriteString(`,`)
			}
			insertStmtBuf.WriteString(`(`)
			for colIdx, datum := range row {
				if colIdx != 0 {
					insertStmtBuf.WriteString(`,`)
				}
				params = append(params, datum)
				fmt.Fprintf(&insertStmtBuf, `$%d`, len(params))
			}
			insertStmtBuf.WriteString(`)`)
		}
		if _, err := sqlDB.Exec(insertStmtBuf.String(), params...); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := sqlDB.Exec(`COMMIT;`); err != nil {
			b.Fatal(err)
		}
		if err := consumeMessages(numRowsInBatch); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}

	// Verify that all rows were inserted.
	var totalRows int
	if err := sqlDB.QueryRow(
		`SELECT count(*) FROM "` + table.Name + `"`,
	).Scan(&totalRows); err != nil {
		b.Fatal(err)
	}
	if numRows != totalRows {
		b.Fatal(errors.Errorf(`sanity check failed: expected %d rows got %d`, numRows, totalRows))
	}
}
