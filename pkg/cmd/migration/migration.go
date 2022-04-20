// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

var flagInitialImportFile = flag.String(
	"import_file",
	"",
	"if set, will attempt to import this file",
)
var flagPGURL = flag.String(
	"pgurl",
	"postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable",
	"CockroachDB instance to connect to",
)
var flagServe = flag.Bool(
	"serve",
	false,
	"Whether to serve a HTTP server",
)
var flagInitialImport = flag.Bool(
	"initial-import",
	false,
	"Whether to do the initial import.",
)
var flagHTTPHostname = flag.String(
	"hostname",
	"",
	"HTTP hostname for serving data",
)
var flagHTTPPort = flag.Int(
	"http-port",
	5050,
	"HTTP Port for serving data",
)

func main() {
	flag.Parse()

	ctx := context.Background()

	conn, err := pgx.Connect(ctx, *flagPGURL)
	if err != nil {
		log.Fatal(errors.Wrap(err, "error connecting to CockroachDB"))
	}
	defer func() {
		_ = conn.Close(ctx)
	}()

	if *flagInitialImport {
		if err := initialImport(ctx, conn); err != nil {
			log.Fatal(errors.Wrap(err, "error creating initial table"))
		}
	}

	if *flagInitialImportFile != "" {
		fmt.Printf("importing file from %s\n", *flagInitialImportFile)
		f, err := os.Open(*flagInitialImportFile)
		if err != nil {
			log.Fatal(errors.Wrap(err, "error importing file"))
		}
		h, err := newFileHook(f, func() { _ = f.Close() })
		if err != nil {
			log.Fatal(errors.Wrap(err, "error init file"))
		}
		if err := attemptImport(ctx, conn, *flagPGURL, filepath.Base(*flagInitialImportFile), h); err != nil {
			log.Fatal(errors.Wrap(err, "error importing file"))
		}
	}

	if *flagServe {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			if _, err := w.Write([]byte("OK")); err != nil {
				log.Printf("could not write response: %v", err)
			}
		})
		mux.HandleFunc("/get", getHandler(*flagPGURL))
		mux.HandleFunc("/put", retryHandler(*flagPGURL))
		mux.HandleFunc("/upload", uploadFileHandler(*flagPGURL))
		mux.HandleFunc("/fix_sequence", fixSerialHandler())
		mux.HandleFunc("/sql", sqlHandler(*flagPGURL))
		fmt.Printf("serving traffic\n")
		if err := http.ListenAndServe(fmt.Sprintf("%s:%d", *flagHTTPHostname, *flagHTTPPort), mux); err != nil {
			log.Fatal(errors.Wrap(err, "error serving http"))
		}
	}
}

func initialImport(ctx context.Context, pgConn *pgx.Conn) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS import_meta (
		id STRING NOT NULL,
		ts TIMESTAMPTZ NOT NULL,
		data TEXT NOT NULL,
		PRIMARY KEY (id, ts)
		)`,
		`SET CLUSTER SETTING kv.range_merge.queue_interval = '50ms';`,
		`SET CLUSTER SETTING kv.raft_log.disable_synchronization_unsafe = true;`,
		`SET CLUSTER SETTING jobs.retention_time = '15s';`,
		`SET CLUSTER SETTING jobs.registry.interval.cancel = '180s';`,
		`SET CLUSTER SETTING jobs.registry.interval.gc = '30s';`,
		`SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`,
		`SET CLUSTER SETTING kv.range_split.by_load_merge_delay = '5s';`,
		`ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = '5';`,
		`ALTER DATABASE system CONFIGURE ZONE USING gc.ttlseconds = '5';`,
	} {
		if _, err := pgConn.Exec(ctx, stmt); err != nil {
			return errors.Wrapf(err, "error executing %s", stmt)
		}
	}
	return nil
}
