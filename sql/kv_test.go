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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/pkg/errors"
)

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
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{})

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
			s.Stopper().Stop()
			enableTracing()
		},
	}
}

func (kv *kvNative) insert(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Put(fmt.Sprintf("%s%06d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(b)
	})
	return err
}

func (kv *kvNative) update(rows, run int) error {
	perm := rand.Perm(rows)
	err := kv.db.Txn(func(txn *client.Txn) error {
		// Read all values in a batch.
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Get(fmt.Sprintf("%s%06d", kv.prefix, perm[i]))
		}
		if err := txn.Run(b); err != nil {
			return err
		}
		// Now add one to each value and add as puts to write batch.
		wb := txn.NewBatch()
		for i, result := range b.Results {
			v := result.Rows[0].ValueInt()
			wb.Put(fmt.Sprintf("%s%06d", kv.prefix, perm[i]), v+1)
		}
		return txn.CommitInBatch(wb)
	})
	return err
}

func (kv *kvNative) del(rows, run int) error {
	firstRow := rows * run
	lastRow := rows * (run + 1)
	err := kv.db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		for i := firstRow; i < lastRow; i++ {
			b.Del(fmt.Sprintf("%s%06d", kv.prefix, i))
		}
		return txn.CommitInBatch(b)
	})
	return err
}

func (kv *kvNative) scan(rows, run int) error {
	var kvs []client.KeyValue
	err := kv.db.Txn(func(txn *client.Txn) error {
		var err error
		kvs, err = txn.Scan(fmt.Sprintf("%s%06d", kv.prefix, 0), fmt.Sprintf("%s%06d", kv.prefix, rows), int64(rows))
		return err
	})
	if len(kvs) != rows {
		return errors.Errorf("expected %d rows; got %d", rows, len(kvs))
	}
	return err
}

func (kv *kvNative) prep(rows int, initData bool) error {
	kv.epoch++
	kv.prefix = fmt.Sprintf("%d/", kv.epoch)
	if !initData {
		return nil
	}
	err := kv.db.Txn(func(txn *client.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%06d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(b)
	})
	return err
}

func (kv *kvNative) done() {
	kv.doneFn()
}

// kvSQL is a SQL-based implementation of the KV interface.
type kvSQL struct {
	db     *gosql.DB
	doneFn func()
}

func newKVSQL(b *testing.B) kvInterface {
	enableTracing := tracing.Disable()
	s, db, _ := serverutils.StartServer(
		b, base.TestServerArgs{UseDatabase: "bench"})

	if _, err := db.Exec(`CREATE DATABASE IF NOT EXISTS bench`); err != nil {
		b.Fatal(err)
	}

	kv := &kvSQL{}
	kv.db = db
	kv.doneFn = func() {
		s.Stopper().Stop()
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
	perm := rand.Perm(rows)
	var buf bytes.Buffer
	buf.WriteString(`UPDATE bench.kv SET v = v + 1 WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `'%06d'`, perm[j])
	}
	buf.WriteString(`)`)
	_, err := kv.db.Exec(buf.String())
	return err
}

func (kv *kvSQL) del(rows, run int) error {
	firstRow := rows * run
	var buf bytes.Buffer
	buf.WriteString(`DELETE FROM bench.kv WHERE k IN (`)
	for j := 0; j < rows; j++ {
		if j > 0 {
			buf.WriteString(", ")
		}
		fmt.Fprintf(&buf, `'%06d'`, j+firstRow)
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
		return errors.Errorf("unexpected result count: %d (expected %d)", n, count)
	}
	return nil
}

func (kv *kvSQL) prep(rows int, initData bool) error {
	if _, err := kv.db.Exec(`DROP TABLE IF EXISTS bench.kv`); err != nil {
		return err
	}
	schema := `
CREATE TABLE IF NOT EXISTS bench.kv (
  k STRING PRIMARY KEY,
  v INT,
  FAMILY (k, v)
)
`
	if _, err := kv.db.Exec(schema); err != nil {
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

	if err := kv.prep(rows, op != "insert" && op != "delete"); err != nil {
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
