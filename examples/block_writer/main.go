// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy

// The block writer example program is a write-only workload intended to insert
// a large amount of data into cockroach quickly. This example is intended to
// trigger range splits and rebalances.
package main

import (
	"database/sql"
	"flag"
	"math/rand"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/security/securitytest"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	insertBlockStmt = `INSERT INTO blocks (block_id, writer_id, block_num, raw_bytes) VALUES ($1, $2, $3, $4)`
)

// db-url = URL of database. If none is specified, a local cluster is
// automatically started.
var dbURL = flag.String("db-url", "", "URL to connect to a running cockroach cluster.")

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 3, "Number of concurrent writers inserting money.")

// outputInterval = interval at which information is output to console.
var outputInterval = flag.Duration("output-interval", 1*time.Second, "Interval of output.")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 256, "Minimum amount of raw data written with each insertion.")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 1024, "Maximum amount of raw data written with each insertion.")

// numBlocks keeps a global count of successfully written blocks.
var numBlocks uint64

// A blockWriter writes blocks of random data into cockroach in an infinite
// loop.
type blockWriter struct {
	id         string
	blockCount uint64
	db         *sql.DB
	rand       *rand.Rand
}

func newBlockWriter(db *sql.DB) blockWriter {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	return blockWriter{
		db:   db,
		id:   uuid.NewUUID4().String(),
		rand: rand.New(source),
	}
}

// run is an infinite loop in which the blockWriter continuously attempts to
// write blocks of random data into a table in cockroach DB.
func (bw blockWriter) run() {
	for {
		blockID := bw.rand.Int63()
		blockData := bw.randomBlock()
		bw.blockCount++
		if _, err := bw.db.Exec(insertBlockStmt, blockID, bw.id, bw.blockCount, blockData); err != nil {
			log.Fatalf("error running blockwriter %s: %s", bw.id, err)
		}
		atomic.AddUint64(&numBlocks, 1)
	}
}

// randomBlock generates a slice of randomized bytes. Random data is preferred
// to prevent compression in storage.
func (bw blockWriter) randomBlock() []byte {
	blockSize := bw.rand.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(bw.rand.Int() & 0xff)
	}
	return blockData
}

// parseFlags ensures that values taken from command line flags are valid.
func parseFlags() error {
	flag.Parse()
	if *concurrency < 1 {
		return util.Errorf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}
	if max, min := *maxBlockSizeBytes, *minBlockSizeBytes; max < min {
		return util.Errorf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)", max, min)
	}
	return nil
}

// maybeStartLocalServer will start a temporary local server if no database URL has been
// provided.
func maybeStartLocalServer() *server.TestServer {
	if *dbURL != "" {
		return nil
	}
	// Start a local test server.
	security.SetReadFileFn(securitytest.Asset)
	serv := server.StartTestServer(nil)
	*dbURL = "https://root@" + serv.ServingAddr() + "?certs=test_certs"
	return serv
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
func setupDatabase() (*sql.DB, error) {
	parsedURL, err := url.Parse(*dbURL)
	if err != nil {
		return nil, err
	}

	// Remove database from parsedUrl
	q := parsedURL.Query()
	q.Del("database")
	parsedURL.RawQuery = q.Encode()

	// Open connection to server and create a database.
	db, err := sql.Open("cockroach", parsedURL.String())
	if err != nil {
		return nil, util.Errorf(err.Error())
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS datablocks"); err != nil {
		return nil, util.Errorf(err.Error())
	}
	db.Close()

	// Open connection directly to the new database.
	q.Add("database", "datablocks")
	parsedURL.RawQuery = q.Encode()
	db, err = sql.Open("cockroach", parsedURL.String())
	if err != nil {
		return nil, util.Errorf(err.Error())
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)

	// Create the initial table for storing blocks.
	if _, err := db.Exec(`DROP TABLE IF EXISTS blocks`); err != nil {
		return nil, util.Errorf(err.Error())
	}
	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS blocks (
	  block_id BIGINT NOT NULL, 
	  writer_id STRING NOT NULL, 
	  block_num BIGINT NOT NULL,
	  raw_bytes BYTES NOT NULL, 
	  PRIMARY KEY (block_id, writer_id, block_num)
	)`); err != nil {
		return nil, util.Errorf(err.Error())
	}

	return db, nil
}

func main() {
	if err := parseFlags(); err != nil {
		log.Fatal(err)
	}

	serv := maybeStartLocalServer()
	if serv != nil {
		defer serv.Stop()
	}

	db, err := setupDatabase()
	if err != nil {
		log.Fatal(err)
	}

	lastNow := time.Now()
	var lastNumDumps uint64
	writers := make([]blockWriter, *concurrency)

	for i := range writers {
		writers[i] = newBlockWriter(db)
		go writers[i].run()
	}

	for range time.Tick(*outputInterval) {
		now := time.Now()
		elapsed := time.Since(lastNow)
		dumps := atomic.LoadUint64(&numBlocks)
		log.Infof("%d dumps were executed at %.1f/second.", (dumps - lastNumDumps), float64(dumps-lastNumDumps)/elapsed.Seconds())
		lastNumDumps = dumps
		lastNow = now
	}
}
