// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/tracing"
)

type keyValue struct {
	key   []byte
	value []byte
}

type kvInterface interface {
	insert(rows, run int) error
	update(rows, run int) error
	del(rows, run int) error
	scan(rows, run int) error

	prep(rows int, initData bool) error
	done()
}

// kvNative uses the native client package to implement kvInterface.
type kvNative struct {
	db     *client.DB
	epoch  int
	prefix string
	doneFn func()
}

func newKVNative(b *testing.B) kvInterface {
	enableTracing := tracing.Disable()
	s := server.StartTestServer(b)

	// TestServer.DB() returns the TxnCoordSender wrapped client. But that isn't
	// a fair comparison with SQL as we want these client requests to be sent
	// over the network.
	sender, err := client.NewSender(
		rpc.NewContext(&base.Context{
			User:       security.NodeUser,
			SSLCA:      filepath.Join(security.EmbeddedCertsDir, security.EmbeddedCACert),
			SSLCert:    filepath.Join(security.EmbeddedCertsDir, "node.crt"),
			SSLCertKey: filepath.Join(security.EmbeddedCertsDir, "node.key"),
		}, nil, s.Stopper()),
		s.ServingAddr())
	if err != nil {
		b.Fatal(err)
	}

	return &kvNative{
		db: client.NewDB(sender),
		doneFn: func() {
			s.Stop()
			enableTracing()
		},
	}
}

func (kv *kvNative) insert(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	pErr := kv.db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Put(fmt.Sprintf("%s%06d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(b)
	})
	return pErr.GoError()
}

func (kv *kvNative) update(rows, run int) error {
	pErr := kv.db.Txn(func(txn *client.Txn) *roachpb.Error {
		// Read all values in a batch.
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Get(fmt.Sprintf("%s%06d", kv.prefix, i))
		}
		if pErr := txn.Run(b); pErr != nil {
			return pErr
		}
		// Now add one to each value and add as puts to write batch.
		wb := txn.NewBatch()
		for i, result := range b.Results {
			v := result.Rows[0].ValueInt()
			wb.Put(fmt.Sprintf("%s%06d", kv.prefix, i), v+1)
		}
		if pErr := txn.CommitInBatch(wb); pErr != nil {
			return pErr
		}
		return nil
	})
	return pErr.GoError()
}

func (kv *kvNative) del(rows, run int) error {
	pErr := kv.db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Del(fmt.Sprintf("%s%06d", kv.prefix, i))
		}
		return txn.CommitInBatch(b)
	})
	return pErr.GoError()
}

func (kv *kvNative) scan(rows, run int) error {
	var kvs []client.KeyValue
	pErr := kv.db.Txn(func(txn *client.Txn) *roachpb.Error {
		var pErr *roachpb.Error
		kvs, pErr = txn.Scan(fmt.Sprintf("%s%06d", kv.prefix, 0), fmt.Sprintf("%s%06d", kv.prefix, rows), int64(rows))
		return pErr
	})
	if len(kvs) != rows {
		return util.Errorf("expected %d rows; got %d", rows, len(kvs))
	}
	return pErr.GoError()
}

func (kv *kvNative) prep(rows int, initData bool) error {
	kv.epoch++
	kv.prefix = fmt.Sprintf("%d/", kv.epoch)
	if !initData {
		return nil
	}
	pErr := kv.db.Txn(func(txn *client.Txn) *roachpb.Error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%06d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(b)
	})
	return pErr.GoError()
}

func (kv *kvNative) done() {
	kv.doneFn()
}

// kvSQL is a SQL-based implementation of the KV interface.
type kvSQL struct {
	db     *sql.DB
	doneFn func()
}

func newKVSQL(b *testing.B) kvInterface {
	enableTracing := tracing.Disable()
	s := server.StartTestServer(b)
	pgURL, cleanupURL := sqlutils.PGUrl(b, s, security.RootUser, "benchmarkCockroach")
	pgURL.Path = "bench"
	db, err := sql.Open("postgres", pgURL.String())
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	kv := &kvSQL{}
	kv.db = db
	kv.doneFn = func() {
		db.Close()
		cleanupURL()
		s.Stop()
		enableTracing()
	}
	return kv
}

func (kv *kvSQL) insert(rows, run int) error {
	firstRow := rows * run
	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.kv VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "('%06d', %d)", i+firstRow, i)
	}
	_, err := kv.db.Exec(buf.String())
	return err
}

func (kv *kvSQL) update(rows, run int) error {
	var buf bytes.Buffer
	buf.WriteString(`UPDATE bench.kv SET v = v + 1 WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `'%06d'`, j)
	}
	buf.WriteString(`)`)
	_, err := kv.db.Exec(buf.String())
	return err
}

func (kv *kvSQL) del(rows, run int) error {
	var buf bytes.Buffer
	buf.WriteString(`DELETE FROM bench.kv WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `'%06d'`, j)
	}
	buf.WriteString(`)`)
	_, err := kv.db.Exec(buf.String())
	return err
}

func (kv *kvSQL) scan(count, run int) error {
	rows, err := kv.db.Query(fmt.Sprintf("SELECT * FROM bench.kv LIMIT %d", count))
	if err != nil {
		return err
	}
	n := 0
	for rows.Next() {
		n++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if n != count {
		return util.Errorf("unexpected result count: %d (expected %d)", n, count)
	}
	return nil
}

func (kv *kvSQL) prep(rows int, initData bool) error {
	if _, err := kv.db.Exec(`DROP TABLE IF EXISTS bench.kv`); err != nil {
		return err
	}
	if _, err := kv.db.Exec(`CREATE TABLE IF NOT EXISTS bench.kv (k STRING PRIMARY KEY, v INT)`); err != nil {
		return err
	}
	if !initData {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteString(`INSERT INTO bench.kv VALUES `)
	for i := 0; i < rows; i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, "('%06d', %d)", i, i)
	}
	_, err := kv.db.Exec(buf.String())
	return err
}

func (kv *kvSQL) done() {
	kv.doneFn()
}

func runKVBenchmark(b *testing.B, typ, op string, rows int) {
	var kv kvInterface
	switch typ {
	case "Native":
		kv = newKVNative(b)
	case "SQL":
		kv = newKVSQL(b)
	default:
		b.Fatalf("unknown implementation: %s", typ)
	}
	defer kv.done()

	var opFn func(int, int) error
	switch op {
	case "insert":
		opFn = kv.insert
	case "update":
		opFn = kv.update
	case "delete":
		opFn = kv.del
	case "scan":
		opFn = kv.scan
	}

	if err := kv.prep(rows, op != "insert"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := opFn(rows, i); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkKVInsert1_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "insert", 1)
}

func BenchmarkKVInsert1_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "insert", 1)
}

func BenchmarkKVInsert10_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "insert", 10)
}

func BenchmarkKVInsert10_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "insert", 10)
}

func BenchmarkKVInsert100_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "insert", 100)
}

func BenchmarkKVInsert100_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "insert", 100)
}

func BenchmarkKVInsert1000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "insert", 1000)
}

func BenchmarkKVInsert1000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "insert", 1000)
}

func BenchmarkKVInsert10000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "insert", 10000)
}

func BenchmarkKVInsert10000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "insert", 10000)
}

func BenchmarkKVUpdate1_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "update", 1)
}

func BenchmarkKVUpdate1_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "update", 1)
}

func BenchmarkKVUpdate10_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "update", 10)
}

func BenchmarkKVUpdate10_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "update", 10)
}

func BenchmarkKVUpdate100_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "update", 100)
}

func BenchmarkKVUpdate100_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "update", 100)
}

func BenchmarkKVUpdate1000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "update", 1000)
}

func BenchmarkKVUpdate1000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "update", 1000)
}

func BenchmarkKVUpdate10000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "update", 10000)
}

func BenchmarkKVUpdate10000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "update", 10000)
}

func BenchmarkKVDelete1_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "delete", 1)
}

func BenchmarkKVDelete1_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "delete", 1)
}

func BenchmarkKVDelete10_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "delete", 10)
}

func BenchmarkKVDelete10_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "delete", 10)
}

func BenchmarkKVDelete100_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "delete", 100)
}

func BenchmarkKVDelete100_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "delete", 100)
}

func BenchmarkKVDelete1000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "delete", 1000)
}

func BenchmarkKVDelete1000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "delete", 1000)
}

func BenchmarkKVDelete10000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "delete", 10000)
}

func BenchmarkKVDelete10000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "delete", 10000)
}

func BenchmarkKVScan1_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "scan", 1)
}

func BenchmarkKVScan1_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "scan", 1)
}

func BenchmarkKVScan10_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "scan", 10)
}

func BenchmarkKVScan10_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "scan", 10)
}

func BenchmarkKVScan100_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "scan", 100)
}

func BenchmarkKVScan100_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "scan", 100)
}

func BenchmarkKVScan1000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "scan", 1000)
}

func BenchmarkKVScan1000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "scan", 1000)
}

func BenchmarkKVScan10000_Native(b *testing.B) {
	runKVBenchmark(b, "Native", "scan", 10000)
}

func BenchmarkKVScan10000_SQL(b *testing.B) {
	runKVBenchmark(b, "SQL", "scan", 10000)
}
