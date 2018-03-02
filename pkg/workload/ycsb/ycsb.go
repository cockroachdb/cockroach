// Copyright 2017 The Cockroach Authors.
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
// Author: Arjun Narayan
//
// The YCSB example program is intended to simulate the workload specified by
// the Yahoo! Cloud Serving Benchmark.
package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/time/rate"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"

	// Cockroach round-robin driver.
	_ "github.com/cockroachdb/loadgen/internal/driver"
)

// SQL statements
const (
	numTableFields = 10
	fieldLength    = 100 // In characters
)

const (
	zipfS    = 0.99
	zipfIMin = 1
)

var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(),
	"Number of concurrent workers sending read/write requests.")
var workload = flag.String("workload", "B", "workload type. Choose from A-F.")
var tolerateErrors = flag.Bool("tolerate-errors", false,
	"Keep running on error. (default false)")
var duration = flag.Duration("duration", 0,
	"The duration to run. If 0, run forever.")
var writeDuration = flag.Duration("write-duration", 0,
	"The duration to perform writes. If 0, write forever.")
var verbose = flag.Bool("v", false, "Print *verbose debug output")
var drop = flag.Bool("drop", true,
	"Drop the existing table and recreate it to start from scratch")
var maxRate = flag.Uint64("max-rate", 0,
	"Maximum requency of operations (reads/writes). If 0, no limit.")
var initialLoad = flag.Uint64("initial-load", 10000,
	"Initial number of rows to sequentially insert before beginning Zipfian workload generation")
var strictPostgres = flag.Bool("strict-postgres", false,
	"Use Postgres compatible syntax, without any Cockroach specific extensions")
var json = flag.Bool("json", false,
	"Use JSONB rather than relational data")

// 7 days at 5% writes and 30k ops/s
var maxWrites = flag.Uint64("max-writes", 7*24*3600*1500,
	"Maximum number of writes to perform before halting. This is required for accurately generating keys that are uniformly distributed across the keyspace.")

var splits = flag.Int("splits", 0, "Number of splits to perform before starting normal operations")

// Mongo flags. See https://godoc.org/gopkg.in/mgo.v2#Session.SetSafe for details.
var mongoWMode = flag.String("mongo-wmode", "", "WMode for mongo session (eg: majority)")
var mongoJ = flag.Bool("mongo-j", false, "Sync journal before op return")

// Cassandra flags.
var cassandraConsistency = flag.String("cassandra-consistency", "QUORUM", "Op consistency: ANY ONE TWO THREE QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE")
var cassandraReplication = flag.Int("cassandra-replication", 1, "Replication factor for cassandra")

var readOnly int32

type database interface {
	readRow(key uint64) (bool, error)
	insertRow(key uint64, fields []string) error
	clone() database
}

// ycsbWorker independently issues reads, writes, and scans against the database.
type ycsbWorker struct {
	db database
	// An RNG used to generate random keys
	zipfR *ZipfGenerator
	// An RNG used to generate random strings for the values
	r         *rand.Rand
	readFreq  float32
	writeFreq float32
	scanFreq  float32
	hashFunc  hash.Hash64
	hashBuf   [8]byte
}

type statistic int

const (
	nonEmptyReads statistic = iota
	emptyReads
	writes
	scans
	writeErrors
	readErrors
	scanErrors
	statsLength
)

var globalStats [statsLength]uint64

type operation int

const (
	writeOp operation = iota
	readOp
	scanOp
)

func newYcsbWorker(db database, zipfR *ZipfGenerator, workloadFlag string) *ycsbWorker {
	source := rand.NewSource(int64(time.Now().UnixNano()))
	var readFreq, writeFreq, scanFreq float32

	switch workloadFlag {
	case "A", "a":
		readFreq = 0.5
		writeFreq = 0.5
	case "B", "b":
		readFreq = 0.95
		writeFreq = 0.05
	case "C", "c":
		readFreq = 1.0
	case "D", "d":
		readFreq = 0.95
		writeFreq = 0.95
		panic("Workload D not implemented yet")
		// TODO(arjun) workload D (read latest) requires modifying the RNG to
		// skew to the latest keys, so not done yet.
	case "E", "e":
		scanFreq = 0.95
		writeFreq = 0.05
		panic("Workload E (scans) not implemented yet")
	case "F", "f":
		writeFreq = 1.0
	}
	r := rand.New(source)
	return &ycsbWorker{
		db:        db,
		r:         r,
		zipfR:     zipfR,
		readFreq:  readFreq,
		writeFreq: writeFreq,
		scanFreq:  scanFreq,
		hashFunc:  fnv.New64(),
	}
}

func (yw *ycsbWorker) hashKey(key uint64) uint64 {
	yw.hashBuf = [8]byte{} // clear hashBuf
	binary.PutUvarint(yw.hashBuf[:], key)
	yw.hashFunc.Reset()
	if _, err := yw.hashFunc.Write(yw.hashBuf[:]); err != nil {
		panic(err)
	}
	// The Go sql driver interface does not support having the high-bit set in
	// uint64 values!
	return yw.hashFunc.Sum64() & math.MaxInt64
}

// Keys are chosen by first drawing from a Zipf distribution and hashing the
// drawn value, so that not all hot keys are close together.
// See YCSB paper section 5.3 for a complete description of how keys are chosen.
func (yw *ycsbWorker) nextReadKey() uint64 {
	var hashedKey uint64
	key := yw.zipfR.Uint64()
	hashedKey = yw.hashKey(key)
	if *verbose {
		fmt.Printf("reader: %d -> %d\n", key, hashedKey)
	}
	return hashedKey
}

func (yw *ycsbWorker) nextWriteKey() uint64 {
	key := yw.zipfR.IMaxHead()
	hashedKey := yw.hashKey(key)
	if *verbose {
		fmt.Printf("writer: %d -> %d\n", key, hashedKey)
	}
	return hashedKey
}

// runLoader inserts n rows in parallel across numWorkers, with
// row_id = i*numWorkers + thisWorkerNum for i = 0...(n-1)
func (yw *ycsbWorker) runLoader(n uint64, numWorkers int, thisWorkerNum int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := uint64(thisWorkerNum + zipfIMin); i < n; i += uint64(numWorkers) {
		hashedKey := yw.hashKey(i)
		if err := yw.insertRow(hashedKey, false); err != nil {
			if *verbose {
				fmt.Printf("error loading row %d: %s\n", i, err)
			}
			atomic.AddUint64(&globalStats[writeErrors], 1)
		} else if *verbose {
			fmt.Printf("loaded %d -> %d\n", i, hashedKey)
		}
	}
}

// runWorker is an infinite loop in which the ycsbWorker reads and writes
// random data into the table in proportion to the op frequencies.
func (yw *ycsbWorker) runWorker(errCh chan<- error, wg *sync.WaitGroup, limiter *rate.Limiter) {
	defer wg.Done()

	for {
		if limiter != nil {
			if err := limiter.Wait(context.Background()); err != nil {
				panic(err)
			}
		}

		switch yw.chooseOp() {
		case readOp:
			if err := yw.readRow(); err != nil {
				atomic.AddUint64(&globalStats[readErrors], 1)
				errCh <- err
			}
		case writeOp:
			if atomic.LoadUint64(&globalStats[writes]) > *maxWrites {
				break
			}
			key := yw.nextWriteKey()
			if err := yw.insertRow(key, true); err != nil {
				errCh <- err
				atomic.AddUint64(&globalStats[writeErrors], 1)
			}
		case scanOp:
			if err := yw.scanRows(); err != nil {
				atomic.AddUint64(&globalStats[scanErrors], 1)
				errCh <- err
			}
		}
	}
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Gnerate a random string of alphabetic characters.
func (yw *ycsbWorker) randString(length int) string {
	str := make([]byte, length)
	for i := range str {
		str[i] = letters[yw.r.Intn(len(letters))]
	}
	return string(str)
}

func (yw *ycsbWorker) insertRow(key uint64, increment bool) error {
	fields := make([]string, numTableFields)
	for i := 0; i < len(fields); i++ {
		fields[i] = yw.randString(fieldLength)
	}
	if err := yw.db.insertRow(key, fields); err != nil {
		return err
	}

	if increment {
		if err := yw.zipfR.IncrementIMax(); err != nil {
			return err
		}
	}
	atomic.AddUint64(&globalStats[writes], 1)
	return nil
}

func (yw *ycsbWorker) readRow() error {
	empty, err := yw.db.readRow(yw.nextReadKey())
	if err != nil {
		return err
	}
	if !empty {
		atomic.AddUint64(&globalStats[nonEmptyReads], 1)
		return nil
	}
	atomic.AddUint64(&globalStats[emptyReads], 1)
	return nil
}

func (yw *ycsbWorker) scanRows() error {
	atomic.AddUint64(&globalStats[scans], 1)
	return errors.Errorf("not implemented yet")
}

// Choose an operation in proportion to the frequencies.
func (yw *ycsbWorker) chooseOp() operation {
	p := yw.r.Float32()
	if atomic.LoadInt32(&readOnly) == 0 && p <= yw.writeFreq {
		return writeOp
	}
	p -= yw.writeFreq
	// If both scanFreq and readFreq are 0 default to readOp if we've reached
	// this point because readOnly is true.
	if p <= yw.scanFreq {
		return scanOp
	}
	return readOp
}

type cockroach struct {
	db        *sql.DB
	readStmt  *sql.Stmt
	writeStmt *sql.Stmt
}

const (
	createStatement = 0
	readStatement   = 1
	writeStatement  = 2
)

var relationalStrategy = [3]string{
	`CREATE TABLE IF NOT EXISTS ycsb.usertable (
		ycsb_key BIGINT PRIMARY KEY NOT NULL,
		FIELD1 TEXT,
		FIELD2 TEXT,
		FIELD3 TEXT,
		FIELD4 TEXT,
		FIELD5 TEXT,
		FIELD6 TEXT,
		FIELD7 TEXT,
		FIELD8 TEXT,
		FIELD9 TEXT,
		FIELD10 TEXT
	)`,
	`SELECT * FROM ycsb.usertable WHERE ycsb_key = $1`,
	`INSERT INTO ycsb.usertable VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
}

var jsonbStrategy = [3]string{
	`CREATE TABLE IF NOT EXISTS ycsb.usertable (
		ycsb_key BIGINT PRIMARY KEY NOT NULL,
		FIELD JSONB
	)`,
	`SELECT * FROM ycsb.usertable WHERE ycsb_key = $1`,
	`INSERT INTO ycsb.usertable VALUES ($1,
		json_build_object(
			'field1',  $2:::text,
			'field2',  $3:::text,
			'field3',  $4:::text,
			'field4',  $5:::text,
			'field5',  $6:::text,
			'field6',  $7:::text,
			'field7',  $8:::text,
			'field8',  $9:::text,
			'field9',  $10:::text,
			'field10', $11:::text
		))`,
}

func (c *cockroach) readRow(key uint64) (bool, error) {
	res, err := c.readStmt.Query(key)
	if err != nil {
		return false, err
	}
	var rowsFound int
	for res.Next() {
		rowsFound++
	}
	if *verbose {
		fmt.Printf("reader found %d rows for key %d\n", rowsFound, key)
	}
	if err := res.Close(); err != nil {
		return false, err
	}
	return rowsFound == 0, nil
}

func (c *cockroach) insertRow(key uint64, fields []string) error {
	args := make([]interface{}, 1+len(fields))
	args[0] = key
	for i, s := range fields {
		args[i+1] = s
	}
	_, err := c.writeStmt.Exec(args...)
	return err
}

func (c *cockroach) clone() database {
	return c
}

func setupCockroach(dbURLs []string) (database, error) {
	// Open connection to server and create a database.
	db, err := sql.Open("cockroach", strings.Join(dbURLs, " "))
	if err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS ycsb"); err != nil {
		if *verbose {
			fmt.Printf("Failed to create the database, attempting to continue... %s\n",
				err)
		}
	}

	if *strictPostgres {
		// Since we use absolute paths (ycsb.usertable), create a Postgres schema
		// to make the absolute paths work.
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS ycsb"); err != nil {
			if *verbose {
				fmt.Printf("Failed to create schema: %s\n", err)
			}
			// Do not fail on this error, as we want strict postgres mode to
			// also work on Cockroach, and Cockroach doesn't have CREATE SCHEMA.
		}
	}

	if *drop {
		if *verbose {
			fmt.Println("Dropping the table")
		}
		if _, err := db.Exec("DROP TABLE IF EXISTS ycsb.usertable"); err != nil {
			if *verbose {
				fmt.Printf("Failed to drop the table: %s\n", err)
			}
			return nil, err
		}
	}

	schema := relationalStrategy
	if *json {
		schema = jsonbStrategy
	}

	if _, err := db.Exec(schema[createStatement]); err != nil {
		return nil, err
	}

	readStmt, err := db.Prepare(schema[readStatement])
	if err != nil {
		return nil, err
	}

	writeStmt, err := db.Prepare(schema[writeStatement])
	if err != nil {
		return nil, err
	}

	if *splits > 0 {
		// NB: We only need ycsbWorker.hashKey, so passing nil for the database and
		// ZipfGenerator is ok.
		w := newYcsbWorker(nil, nil, *workload)
		splitPoints := make([]uint64, *splits)
		for i := 0; i < *splits; i++ {
			splitPoints[i] = w.hashKey(uint64(i))
		}
		sort.Slice(splitPoints, func(i, j int) bool {
			return splitPoints[i] < splitPoints[j]
		})

		type pair struct {
			lo, hi int
		}
		splitCh := make(chan pair, *concurrency)
		splitCh <- pair{0, len(splitPoints)}
		doneCh := make(chan struct{})

		var wg sync.WaitGroup
		for i := 0; i < *concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					p, ok := <-splitCh
					if !ok {
						break
					}
					m := (p.lo + p.hi) / 2
					split := splitPoints[m]
					if _, err := db.Exec(`ALTER TABLE ycsb.usertable SPLIT AT VALUES ($1)`, split); err != nil {
						log.Fatal(err)
					}
					// NB: the split+1 expression below guarantees our scatter range
					// touches both sides of the split.
					if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE ycsb.usertable SCATTER FROM (%d) TO (%d)`,
						splitPoints[p.lo], split+1)); err != nil {
						// SCATTER can collide with normal replicate queue operations and
						// fail spuriously, so only print the error.
						log.Print(err)
					}
					doneCh <- struct{}{}
					go func() {
						if p.lo < m {
							splitCh <- pair{p.lo, m}
						}
						if m+1 < p.hi {
							splitCh <- pair{m + 1, p.hi}
						}
					}()
				}
			}()
		}

		for i := 0; i < len(splitPoints); i++ {
			<-doneCh
			if (i+1)%1000 == 0 {
				fmt.Printf("%d splits\n", i+1)
			}
		}
		close(splitCh)
		wg.Wait()
	}

	return &cockroach{db: db, readStmt: readStmt, writeStmt: writeStmt}, nil
}

type mongoBlock struct {
	Key    int64 `bson:"_id"`
	Fields []string
}

type mongo struct {
	kv *mgo.Collection
}

func (m *mongo) readRow(key uint64) (bool, error) {
	var b mongoBlock
	if err := m.kv.Find(bson.M{"_id": key}).One(&b); err != nil {
		if err == mgo.ErrNotFound {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (m *mongo) insertRow(key uint64, fields []string) error {
	return m.kv.Insert(&mongoBlock{
		Key:    int64(key),
		Fields: fields,
	})
}

func (m *mongo) clone() database {
	return &mongo{
		// NB: Whoa!
		kv: m.kv.Database.Session.Copy().DB(m.kv.Database.Name).C(m.kv.Name),
	}
}

func setupMongo(dbURLs []string) (database, error) {
	// NB: the Mongo driver automatically detects the other nodes in the
	// cluster. We just have to specify the first one.
	session, err := mgo.Dial(dbURLs[0])
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	session.SetSafe(&mgo.Safe{WMode: *mongoWMode, J: *mongoJ})

	kv := session.DB("ycsb").C("kv")
	if *drop {
		// Intentionally ignore the error as we can't tell if the collection
		// doesn't exist.
		_ = kv.DropCollection()
	}
	return &mongo{kv: kv}, nil
}

type cassandra struct {
	session *gocql.Session
}

func (c *cassandra) readRow(key uint64) (bool, error) {
	var k uint64
	var fields [10]string
	if err := c.session.Query(
		`SELECT * FROM ycsb.usertable WHERE ycsb_key = ? LIMIT 1`,
		key).Scan(&k, &fields[0], &fields[1], &fields[2], &fields[3],
		&fields[4], &fields[5], &fields[6], &fields[7], &fields[8], &fields[9]); err != nil {
		if err == gocql.ErrNotFound {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (c *cassandra) insertRow(key uint64, fields []string) error {
	const stmt = "INSERT INTO ycsb.usertable " +
		"(ycsb_key, field1, field2, field3, field4, field5, field6, field7, field8, field9, field10) " +
		"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); "
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i := 0; i < len(fields); i++ {
		args[i+1] = fields[i]
	}
	return c.session.Query(stmt, args...).Exec()
}

func (c *cassandra) clone() database {
	return c
}

func setupCassandra(dbURLs []string) (database, error) {
	hosts := make([]string, 0, len(dbURLs))
	for _, u := range dbURLs {
		p, err := url.Parse(u)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, p.Host)
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	if *drop {
		_ = s.Query(`DROP KEYSPACE ycsb`).RetryPolicy(nil).Exec()
	}

	createKeyspace := fmt.Sprintf(`
CREATE KEYSPACE IF NOT EXISTS ycsb WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : %d
};`, *cassandraReplication)

	const createTable = `
CREATE TABLE IF NOT EXISTS ycsb.usertable (
  ycsb_key BIGINT,
	FIELD1 BLOB,
	FIELD2 BLOB,
	FIELD3 BLOB,
	FIELD4 BLOB,
	FIELD5 BLOB,
	FIELD6 BLOB,
	FIELD7 BLOB,
	FIELD8 BLOB,
	FIELD9 BLOB,
	FIELD10 BLOB,
  PRIMARY KEY(ycsb_key)
);`

	if err := s.Query(createKeyspace).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := s.Query(createTable).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	return &cassandra{session: s}, nil
}

// setupDatabase performs initial setup for the example, creating a database
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped if the -drop flag was specified.
func setupDatabase(dbURLs []string) (database, error) {
	parsedURL, err := url.Parse(dbURLs[0])
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "ycsb"

	switch parsedURL.Scheme {
	case "postgres", "postgresql":
		return setupCockroach(dbURLs)
	case "mongodb":
		return setupMongo(dbURLs)
	case "cassandra":
		return setupCassandra(dbURLs)
	default:
		return nil, fmt.Errorf("unsupported database: %s", parsedURL.Scheme)
	}
}

var usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s <db URL>\n\n", os.Args[0])
	flag.PrintDefaults()
}

func snapshotStats() (s [statsLength]uint64) {
	for i := 0; i < int(statsLength); i++ {
		s[i] = atomic.LoadUint64(&globalStats[i])
	}
	return s
}

type atomicTime struct {
	ptr unsafe.Pointer
}

func (t *atomicTime) set(v time.Time) {
	atomic.StorePointer(&t.ptr, unsafe.Pointer(&v))
}

func (t *atomicTime) get() time.Time {
	return *(*time.Time)(atomic.LoadPointer(&t.ptr))
}

func main() {
	flag.Usage = usage
	flag.Parse()
	if *verbose {
		fmt.Fprintf(os.Stdout, "Starting YCSB load generator\n")
	}

	dbURLs := []string{"postgres://root@localhost:26257/ycsb?sslmode=disable"}
	if args := flag.Args(); len(args) >= 1 {
		dbURLs = args
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1",
			concurrency)
	}

	db, err := setupDatabase(dbURLs)

	if err != nil {
		log.Fatalf("Setting up database failed: %s", err)
	}
	if *verbose {
		fmt.Printf("Database setup complete. Loading...\n")
	}

	lastNow := time.Now()
	var lastOpsCount uint64
	var lastStats [statsLength]uint64

	zipfR, err := NewZipfGenerator(zipfIMin, *initialLoad, zipfS, *verbose)
	if err != nil {
		panic(err)
	}

	workers := make([]*ycsbWorker, *concurrency)
	for i := range workers {
		workers[i] = newYcsbWorker(db.clone(), zipfR, *workload)
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	errCh := make(chan error)
	tick := time.Tick(1 * time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	var start atomicTime
	var startOpsCount uint64
	var numErr int
	start.set(time.Now())

	go func() {
		loadStart := time.Now()
		var wg sync.WaitGroup
		for i, n := 0, len(workers); i < n; i++ {
			wg.Add(1)
			go workers[i].runLoader(*initialLoad, n, i, &wg)
		}
		wg.Wait()
		fmt.Printf("Loading complete: %.1fs\n", time.Since(loadStart).Seconds())

		// Reset the start time and stats.
		lastNow = time.Now()
		start.set(lastNow)
		atomic.StoreUint64(&startOpsCount, 0)
		for i := 0; i < int(statsLength); i++ {
			atomic.StoreUint64(&globalStats[i], 0)
		}
		lastOpsCount = 0
		for i := range lastStats {
			lastStats[i] = 0
		}

		wg = sync.WaitGroup{}
		for i := range workers {
			wg.Add(1)
			go workers[i].runWorker(errCh, &wg, limiter)
		}

		go func() {
			wg.Wait()
			done <- syscall.Signal(0)
		}()

		if *duration > 0 {
			go func() {
				time.Sleep(*duration)
				done <- syscall.Signal(0)
			}()
		}

		if *writeDuration > 0 {
			go func() {
				time.Sleep(*writeDuration)
				atomic.StoreInt32(&readOnly, 1)
			}()
		}
	}()

	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else if *verbose {
				log.Print(err)
			}
			continue

		case <-tick:
			now := time.Now()
			elapsed := now.Sub(lastNow)

			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans]
			if i%20 == 0 {
				fmt.Printf("elapsed______ops/sec__reads/empty/errors___writes/errors____scans/errors\n")
			}
			fmt.Printf("%7s %12.1f %19s %15s %15s\n",
				time.Duration(time.Since(start.get()).Seconds()+0.5)*time.Second,
				float64(opsCount-lastOpsCount)/elapsed.Seconds(),
				fmt.Sprintf("%d / %d / %d",
					stats[nonEmptyReads]-lastStats[nonEmptyReads],
					stats[emptyReads]-lastStats[emptyReads],
					stats[readErrors]-lastStats[readErrors]),
				fmt.Sprintf("%d / %d",
					stats[writes]-lastStats[writes],
					stats[writeErrors]-lastStats[writeErrors]),
				fmt.Sprintf("%d / %d",
					stats[scans]-lastStats[scans],
					stats[scanErrors]-lastStats[scanErrors]))
			lastStats = stats
			lastOpsCount = opsCount
			lastNow = now
			i++

		case <-done:
			stats := snapshotStats()
			opsCount := stats[writes] + stats[emptyReads] +
				stats[nonEmptyReads] + stats[scans] - atomic.LoadUint64(&startOpsCount)
			elapsed := time.Since(start.get()).Seconds()
			fmt.Printf("\nelapsed__ops/sec(total)__errors(total)\n")
			fmt.Printf("%6.1fs %14.1f %14d\n",
				time.Since(start.get()).Seconds(),
				float64(opsCount)/elapsed, numErr)
			return
		}
	}
}
