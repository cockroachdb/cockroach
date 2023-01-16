package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
)

var maxRows = flag.Int("max_rows", 10000, "maximum number of rows to insert per batch")
var insertTimes = flag.Int("insert_times", 3000, "amount of times max_rows is inserted")
var dbUrl = flag.String("db", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "db url")

var counter = -999999999
var increment = 10000

func main() {
	flag.Parse()

	ctx := context.Background()
	fmt.Printf("connecting to db %s\n", *dbUrl)
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
	r, w := io.Pipe()
	go producer(bufio.NewWriter(w), rng, *insertTimes**maxRows)
	if _, err := conn.PgConn().CopyFrom(ctx, bufio.NewReaderSize(r, 4*(1<<20)), "COPY test_table FROM STDIN CSV"); err != nil {
		panic(errors.Wrapf(err, "failed to copy"))
	}
}

func producer(w io.Writer, rng *rand.Rand, n int) {
	l := log.Every(5 * time.Second)

	for i := 0; i < n; i++ {
		if _, err := fmt.Fprintf(
			w,
			"%d,%d,%s,%s,%s,,%s,%s\n",
			i,
			100,
			"1970-01-01 00:00:00", "1970-01-01 00:00:00", "1970-01-01 00:00:00",
			makeLenStr(rng.Intn(2048)),
			makeLenStr(rng.Intn(512)),
		); err != nil {
			panic(err)
		}
		if l.ShouldLog() {
			fmt.Printf("%d rows produced [%.2f%%]\n", i, float64(i)/float64(n))
		}
	}
}

func makeLenStr(i int) string {
	return strings.Repeat("a", i)
}
