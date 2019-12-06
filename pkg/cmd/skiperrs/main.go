// skiperrs connects to a postgres-compatible server with its URL specified
// as the first argument. It then streams splits stdin into SQL statements and
// executes them on the connection. Errors are printed but otherwise ignored.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/cr2pg/sqlstream"
	"github.com/lib/pq"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("usage: %s <url>\n", os.Args[0])
		os.Exit(1)
	}
	url := os.Args[1]

	connector, err := pq.NewConnector(url)
	if err != nil {
		log.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()

	stream := sqlstream.NewStream(os.Stdin)
	for {
		stmt, err := stream.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal(err)
		}
		if _, err := db.Exec(stmt.String()); err != nil {
			fmt.Println(err)
		}
	}
}
