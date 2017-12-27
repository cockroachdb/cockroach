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

package main

import (
	"bytes"
	"crypto/sha1"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"hash"
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
	"testing"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/codahale/hdrhistogram"
	"github.com/gocql/gocql"
	"github.com/tylertreat/hdrhistogram-writer"

	// Cockroach round-robin driver.
	_ "github.com/cockroachdb/loadgen/internal/driver"
)

var readPercent = flag.Int("read-percent", 0, "Percent (0-100) of operations that are reads of existing keys")
var cycleLength = flag.Int64("cycle-length", math.MaxInt64, "Number of keys repeatedly accessed by each writer")

// concurrency = number of concurrent insertion processes.
var concurrency = flag.Int("concurrency", 2*runtime.NumCPU(), "Number of concurrent writers inserting blocks")

// batch = number of blocks to insert in a single SQL statement.
var batch = flag.Int("batch", 1, "Number of blocks to insert in a single SQL statement")

var splits = flag.Int("splits", 0, "Number of splits to perform before starting normal operations")

var tolerateErrors = flag.Bool("tolerate-errors", false, "Keep running on error")

var maxRate = flag.Float64("max-rate", 0, "Maximum frequency of operations (reads/writes). If 0, no limit.")

var tableName = flag.String("table", "kv", "Name of table to use")

// Minimum and maximum size of inserted blocks.
var minBlockSizeBytes = flag.Int("min-block-bytes", 1, "Minimum amount of raw data written with each insertion")
var maxBlockSizeBytes = flag.Int("max-block-bytes", 2, "Maximum amount of raw data written with each insertion")

var maxOps = flag.Uint64("max-ops", 0, "Maximum number of blocks to read/write")
var duration = flag.Duration("duration", 0, "The duration to run. If 0, run forever.")
var writeSeq = flag.Int64("write-seq", 0, "Initial write sequence value.")
var seqSeed = flag.Int64("seed", time.Now().UnixNano(), "Key hash seed.")
var sequential = flag.Bool("sequential", false, "Pick keys sequentially instead of randomly.")
var drop = flag.Bool("drop", false, "Clear the existing data before starting.")

// Output in HdrHistogram Plotter format. See https://hdrhistogram.github.io/HdrHistogram/plotFiles.html
var histFile = flag.String("hist-file", "", "Write histogram data to file for HdrHistogram Plotter, or stdout if - is specified.")

// Mongo flags. See https://godoc.org/gopkg.in/mgo.v2#Session.SetSafe for details.
var mongoWMode = flag.String("mongo-wmode", "", "WMode for mongo session (eg: majority)")
var mongoJ = flag.Bool("mongo-j", false, "Sync journal before op return")

// Cassandra flags.
var cassandraConsistency = flag.String("cassandra-consistency", "QUORUM", "Op consistency: ANY ONE TWO THREE QUORUM ALL LOCAL_QUORUM EACH_QUORUM LOCAL_ONE")
var cassandraReplication = flag.Int("cassandra-replication", 1, "Replication factor for cassandra")

// numOps keeps a global count of successful operations.
var numOps uint64

const (
	minLatency = 100 * time.Microsecond
	maxLatency = 10 * time.Second
)

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type sequence struct {
	val  int64
	seed int64
}

func (s *sequence) write() int64 {
	return (atomic.AddInt64(&s.val, 1) - 1) % *cycleLength
}

// read returns the last key index that has been written. Note that the returned
// index might not actually have been written yet, so a read operation cannot
// require that the key is present.
func (s *sequence) read() int64 {
	return atomic.LoadInt64(&s.val) % *cycleLength
}

// generator generates read and write keys. Read keys may not yet exist and write
// keys may already exist.
type generator interface {
	writeKey() int64
	readKey() int64
	rand() *rand.Rand
}

type hashGenerator struct {
	seq    *sequence
	random *rand.Rand
	hasher hash.Hash
	buf    [sha1.Size]byte
}

func newHashGenerator(seq *sequence) *hashGenerator {
	return &hashGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
		hasher: sha1.New(),
	}
}

func (g *hashGenerator) hash(v int64) int64 {
	binary.BigEndian.PutUint64(g.buf[:8], uint64(v))
	binary.BigEndian.PutUint64(g.buf[8:16], uint64(g.seq.seed))
	g.hasher.Reset()
	_, _ = g.hasher.Write(g.buf[:16])
	g.hasher.Sum(g.buf[:0])
	return int64(binary.BigEndian.Uint64(g.buf[:8]))
}

func (g *hashGenerator) writeKey() int64 {
	return g.hash(g.seq.write())
}

func (g *hashGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.hash(g.random.Int63n(v))
}

func (g *hashGenerator) rand() *rand.Rand {
	return g.random
}

type sequentialGenerator struct {
	seq    *sequence
	random *rand.Rand
}

func newSequentialGenerator(seq *sequence) *sequentialGenerator {
	return &sequentialGenerator{
		seq:    seq,
		random: rand.New(rand.NewSource(int64(time.Now().UnixNano()))),
	}
}

func (g *sequentialGenerator) writeKey() int64 {
	return g.seq.write()
}

func (g *sequentialGenerator) readKey() int64 {
	v := g.seq.read()
	if v == 0 {
		return 0
	}
	return g.random.Int63n(v)
}

func (g *sequentialGenerator) rand() *rand.Rand {
	return g.random
}

func randomBlock(r *rand.Rand) []byte {
	blockSize := r.Intn(*maxBlockSizeBytes-*minBlockSizeBytes) + *minBlockSizeBytes
	blockData := make([]byte, blockSize)
	for i := range blockData {
		blockData[i] = byte(r.Int() & 0xff)
	}
	return blockData
}

type database interface {
	read(count int, g generator) error
	write(count int, g generator) error
	clone() database
}

type blocker struct {
	db      database
	gen     generator
	latency struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
	}
}

func newBlocker(db database, gen generator) *blocker {
	b := &blocker{
		db:  db,
		gen: gen,
	}
	b.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	return b
}

// run is an infinite loop in which the blocker continuously attempts to
// read / write blocks of random data into a table in cockroach DB.
func (b *blocker) run(errCh chan<- error, wg *sync.WaitGroup, limiter *rate.Limiter) {
	defer wg.Done()

	for {
		// Limit how quickly the load generator sends requests based on --max-rate.
		if limiter != nil {
			if err := limiter.Wait(context.Background()); err != nil {
				panic(err)
			}
		}

		start := time.Now()
		var err error
		if b.gen.rand().Intn(100) < *readPercent {
			err = b.db.read(*batch, b.gen)
		} else {
			err = b.db.write(*batch, b.gen)
		}
		if err != nil {
			errCh <- err
			continue
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency)
		b.latency.Lock()
		if err := b.latency.Current.RecordValue(elapsed.Nanoseconds()); err != nil {
			log.Fatal(err)
		}
		b.latency.Unlock()
		v := atomic.AddUint64(&numOps, uint64(*batch))
		if *maxOps > 0 && v >= *maxOps {
			return
		}
	}
}

type cockroach struct {
	db        *sql.DB
	readStmt  *sql.Stmt
	writeStmt *sql.Stmt
}

func (c *cockroach) read(count int, g generator) error {
	args := make([]interface{}, count)
	for i := 0; i < count; i++ {
		args[i] = g.readKey()
	}
	rows, err := c.readStmt.Query(args...)
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

func (c *cockroach) write(count int, g generator) error {
	const argCount = 2
	args := make([]interface{}, argCount*count)
	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = g.writeKey()
		args[j+1] = randomBlock(g.rand())
	}
	_, err := c.writeStmt.Exec(args...)
	return err
}

func (c *cockroach) clone() database {
	return c
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func setupCockroach(dbURLs []string) (database, error) {
	// Open connection to server and create a database.
	db, dbErr := sql.Open("cockroach", strings.Join(dbURLs, " "))
	if dbErr != nil {
		return nil, dbErr
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS test"); err != nil {
		return nil, err
	}

	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(*concurrency + 1)
	db.SetMaxIdleConns(*concurrency + 1)

	if *drop {
		if _, err := db.Exec(`DROP TABLE IF EXISTS test.` + *tableName); err != nil {
			return nil, err
		}
	}

	if _, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS test.` + *tableName + ` (
	  k BIGINT NOT NULL PRIMARY KEY,
	  v BYTES NOT NULL
	)`); err != nil {
		return nil, err
	}

	if *splits > 0 {
		r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
		g := newHashGenerator(&sequence{val: *writeSeq, seed: *seqSeed})

		splitPoints := make([]int64, *splits)
		for i := 0; i < *splits; i++ {
			splitPoints[i] = g.hash(r.Int63())
		}
		sort.Sort(int64Slice(splitPoints))

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
					if _, err := db.Exec(`ALTER TABLE test.`+*tableName+` SPLIT AT VALUES ($1)`, split); err != nil {
						log.Fatal(err)
					}
					// NB: the split+1 expression below guarantees our scatter range
					// touches both sides of the split.
					if _, err := db.Exec(fmt.Sprintf(`ALTER TABLE test.`+*tableName+` SCATTER FROM (%d) TO (%d)`,
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

	var buf bytes.Buffer
	buf.WriteString(`SELECT k, v FROM test.` + *tableName + ` WHERE k IN (`)
	for i := 0; i < *batch; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `$%d`, i+1)
	}
	buf.WriteString(`)`)
	readStmt, err := db.Prepare(buf.String())
	if err != nil {
		return nil, err
	}

	buf.Reset()
	buf.WriteString(`UPSERT INTO test.` + *tableName + ` (k, v) VALUES`)

	for i := 0; i < *batch; i++ {
		j := i * 2
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, ` ($%d, $%d)`, j+1, j+2)
	}

	writeStmt, err := db.Prepare(buf.String())
	if err != nil {
		return nil, err
	}

	return &cockroach{db: db, readStmt: readStmt, writeStmt: writeStmt}, nil
}

type mongoBlock struct {
	Key   int64 `bson:"_id"`
	Value []byte
}

type mongo struct {
	kv *mgo.Collection
}

func (m *mongo) read(count int, g generator) error {
	// TODO(peter): implement multi-key reads.
	if count != 1 {
		log.Fatalf("unsupported count: %d", count)
	}
	var b mongoBlock
	// These queries are functionally the same, but the one using the $eq
	// operator is ~30% slower!
	//
	// if err := m.kv.Find(bson.M{"_id": bson.M{"$eq": key}}).One(&b); err != nil {
	if err := m.kv.Find(bson.M{"_id": g.readKey()}).One(&b); err != nil {
		if err == mgo.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (m *mongo) write(count int, g generator) error {
	docs := make([]interface{}, count)
	for i := 0; i < count; i++ {
		docs[i] = &mongoBlock{
			Key:   g.writeKey(),
			Value: randomBlock(g.rand()),
		}
	}

	return m.kv.Insert(docs...)
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

	kv := session.DB("test").C(*tableName)
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

func (c *cassandra) read(count int, g generator) error {
	// TODO(peter): implement multi-key reads.
	if count != 1 {
		log.Fatalf("unsupported count: %d", count)
	}
	var v []byte
	if err := c.session.Query(
		`SELECT v FROM test.`+*tableName+` WHERE k = ? LIMIT 1`,
		g.readKey()).Scan(&v); err != nil {
		if err == gocql.ErrNotFound {
			return nil
		}
		return err
	}
	return nil
}

func (c *cassandra) write(count int, g generator) error {
	insertBlockStmt := `INSERT INTO test.` + *tableName + ` (k, v) VALUES (?, ?); `

	var buf bytes.Buffer
	buf.WriteString("BEGIN BATCH ")
	const argCount = 2
	args := make([]interface{}, argCount*count)

	for i := 0; i < count; i++ {
		j := i * argCount
		args[j+0] = g.writeKey()
		args[j+1] = randomBlock(g.rand())
		buf.WriteString(insertBlockStmt)
	}

	buf.WriteString("APPLY BATCH;")
	return c.session.Query(buf.String(), args...).Exec()
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
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	s, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	if *drop {
		_ = s.Query(`DROP KEYSPACE test`).RetryPolicy(nil).Exec()
	}

	createKeyspace := fmt.Sprintf(`
CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {
  'class' : 'SimpleStrategy',
  'replication_factor' : %d
};`, *cassandraReplication)

	createTable := `
CREATE TABLE IF NOT EXISTS test.` + *tableName + `(
  k BIGINT,
  v BLOB,
  PRIMARY KEY(k)
);`

	if err := s.Query(createKeyspace).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	if err := s.Query(createTable).RetryPolicy(nil).Exec(); err != nil {
		log.Fatal(err)
	}
	return &cassandra{session: s}, nil
}

// setupDatabase performs initial setup for the example, creating a database and
// with a single table. If the desired table already exists on the cluster, the
// existing table will be dropped.
func setupDatabase(dbURLs []string) (database, error) {
	parsedURL, err := url.Parse(dbURLs[0])
	if err != nil {
		return nil, err
	}
	parsedURL.Path = "test"

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
	fmt.Fprintf(os.Stderr, "  %s [<db URL> ...]\n\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = usage
	flag.Parse()

	dbURLs := []string{"postgres://root@localhost:26257/test?sslmode=disable"}
	if args := flag.Args(); len(args) >= 1 {
		dbURLs = args
	}

	if *concurrency < 1 {
		log.Fatalf("Value of 'concurrency' flag (%d) must be greater than or equal to 1", *concurrency)
	}

	if max, min := *maxBlockSizeBytes, *minBlockSizeBytes; max < min {
		log.Fatalf("Value of 'max-block-bytes' (%d) must be greater than or equal to value of 'min-block-bytes' (%d)", max, min)
	}

	if *sequential && *splits > 0 {
		log.Fatalf("'sequential' and 'splits' cannot both be enabled")
	}

	var db database
	{
		var err error
		for err == nil || *tolerateErrors {
			db, err = setupDatabase(dbURLs)
			if err == nil {
				break
			}
			if !*tolerateErrors {
				log.Fatal(err)
			}
		}
	}

	var limiter *rate.Limiter
	if *maxRate > 0 {
		// Create a limiter using maxRate specified on the command line and
		// with allowed burst of 1 at the maximum allowed rate.
		limiter = rate.NewLimiter(rate.Limit(*maxRate), 1)
	}

	lastNow := time.Now()
	start := lastNow
	var lastOps uint64
	writers := make([]*blocker, *concurrency)

	seq := &sequence{val: *writeSeq, seed: *seqSeed}
	errCh := make(chan error)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		if *sequential {
			writers[i] = newBlocker(db.clone(), newSequentialGenerator(seq))
		} else {
			writers[i] = newBlocker(db.clone(), newHashGenerator(seq))
		}
		go writers[i].run(errCh, &wg, limiter)
	}

	var numErr int
	tick := time.Tick(time.Second)
	done := make(chan os.Signal, 3)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

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

	defer func() {
		// Output results that mimic Go's built-in benchmark format.

		benchmarkName := strings.Join([]string{
			"BenchmarkLoadgenKV",
			fmt.Sprintf("readPercent=%d", *readPercent),
			fmt.Sprintf("splits=%d", *splits),
			fmt.Sprintf("concurrency=%d", *concurrency),
			fmt.Sprintf("duration=%s", *duration),
		}, "/")

		result := testing.BenchmarkResult{
			N: int(numOps),
			T: time.Since(start),
		}
		fmt.Printf("%s\t%s\n", benchmarkName, result)
	}()

	cumLatency := hdrhistogram.New(minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	for i := 0; ; {
		select {
		case err := <-errCh:
			numErr++
			if !*tolerateErrors {
				log.Fatal(err)
			} else {
				log.Print(err)
			}
			continue

		case <-tick:
			var h *hdrhistogram.Histogram
			for _, w := range writers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				if h == nil {
					h = m
				} else {
					h.Merge(m)
				}
			}

			cumLatency.Merge(h)
			p50 := h.ValueAtQuantile(50)
			p95 := h.ValueAtQuantile(95)
			p99 := h.ValueAtQuantile(99)
			pMax := h.ValueAtQuantile(100)

			now := time.Now()
			elapsed := now.Sub(lastNow)
			ops := atomic.LoadUint64(&numOps)
			if i%20 == 0 {
				fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			}
			i++
			fmt.Printf("%8s %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f\n",
				time.Duration(time.Since(start).Seconds()+0.5)*time.Second,
				numErr,
				float64(ops-lastOps)/elapsed.Seconds(),
				float64(ops)/time.Since(start).Seconds(),
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			lastOps = ops
			lastNow = now

		case <-done:
			for _, w := range writers {
				w.latency.Lock()
				m := w.latency.Merge()
				w.latency.Rotate()
				w.latency.Unlock()
				cumLatency.Merge(m)
			}

			avg := cumLatency.Mean()
			p50 := cumLatency.ValueAtQuantile(50)
			p95 := cumLatency.ValueAtQuantile(95)
			p99 := cumLatency.ValueAtQuantile(99)
			pMax := cumLatency.ValueAtQuantile(100)

			ops := atomic.LoadUint64(&numOps)
			elapsed := time.Since(start).Seconds()
			fmt.Println("\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
			fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f\n\n",
				time.Since(start).Seconds(), numErr,
				ops, float64(ops)/elapsed,
				time.Duration(avg).Seconds()*1000,
				time.Duration(p50).Seconds()*1000,
				time.Duration(p95).Seconds()*1000,
				time.Duration(p99).Seconds()*1000,
				time.Duration(pMax).Seconds()*1000)
			if *histFile == "-" {
				if err := histwriter.WriteDistribution(cumLatency, nil, 1, os.Stdout); err != nil {
					fmt.Printf("failed to write histogram to stdout: %v\n", err)
				}
			} else if *histFile != "" {
				if err := histwriter.WriteDistributionFile(cumLatency, nil, 1, *histFile); err != nil {
					fmt.Printf("failed to write histogram file: %v\n", err)
				}
			}
			return
		}
	}
}
