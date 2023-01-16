package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v4"
	"math/rand"
	"strings"

	"github.com/cockroachdb/errors"
)

var maxRows = flag.Int("max_rows", 10000, "maximum number of rows to insert per batch")
var insertTimes = flag.Int("insert_times", 3000, "amount of times max_rows is inserted")
var dbUrl = flag.String("db", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "db url")

var counter = -999999999
var increment = 10000

func main() {
	flag.Parse()

	ctx := context.Background()
	fmt.Printf("connecting to db\n")
	conn, err := pgx.Connect(ctx, *dbUrl)
	if err != nil {
		panic(errors.Newf("Unable to connect to database: %v", err))
	}
	defer conn.Close(context.Background())

	fmt.Printf("connected to db\n")

	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_table"); err != nil {
		panic(errors.Wrapf(err, "failed to truncate table"))
	}
	fmt.Printf("table dropped\n")
	if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS test_table (id INT8 PRIMARY KEY, i1 int, t1 timestamp(0), t2 timestamp(0), t3 timestamp(0), n TEXT, t varchar(2048), u varchar(512))"); err != nil {
		panic(errors.Wrapf(err, "failed to truncate table"))
	}
	fmt.Printf("table created\n")

	rng := rand.New(rand.NewSource(0))
	for i := 0; i < *insertTimes; i++ {
		fmt.Printf("beginning copy iteration %d\n", i+1)
		var sb strings.Builder
		for it := 0; it < *maxRows; it++ {
			sb.WriteString(fmt.Sprintf(
				"%d,%d,%s,%s,%s,,%s,%s\n",
				counter,
				100,
				"1970-01-01 00:00:00", "1970-01-01 00:00:00", "1970-01-01 00:00:00",
				makeLenStr(rng.Intn(2048)),
				makeLenStr(rng.Intn(512)),
			))
			counter += increment
		}
		r, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY test_table FROM STDIN CSV")
		if err != nil {
			panic(errors.Wrapf(err, "failed to copy"))
		}
		fmt.Printf("done with batch %d; copied %d rows\n", i+1, r.RowsAffected())
	}
	fmt.Printf("complete!\n")
}

func makeLenStr(i int) string {
	return strings.Repeat("a", i)
}
