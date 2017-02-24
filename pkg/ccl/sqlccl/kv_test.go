// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type kvInterface interface {
	insert(rows, run int) error

	prep(rows int, initData bool) error
	done()
}

type kvWriteBatch struct {
	db     *client.DB
	epoch  int
	prefix string
	doneFn func()
}

func newKVWriteBatch(b *testing.B) kvInterface {
	enableTracing := tracing.Disable()
	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{})

	// TestServer.KVClient() returns the TxnCoordSender wrapped client. But that
	// isn't a fair comparison with SQL as we want these client requests to be
	// sent over the network.
	rpcContext := s.RPCContext()

	conn, err := rpcContext.GRPCDial(s.ServingAddr())
	if err != nil {
		b.Fatal(err)
	}

	return &kvWriteBatch{
		db: client.NewDB(client.NewSender(conn), rpcContext.LocalClock),
		doneFn: func() {
			s.Stopper().Stop()
			enableTracing()
		},
	}
}

func (kv *kvWriteBatch) insert(rows, run int) error {
	var batch engine.RocksDBBatchBuilder

	firstRow := rows * run
	lastRow := rows * (run + 1)
	for i := firstRow; i < lastRow; i++ {
		ts := hlc.Timestamp{WallTime: 1}
		key := engine.MVCCKey{Key: []byte(fmt.Sprintf("%s%08d", kv.prefix, i)), Timestamp: ts}
		var v roachpb.Value
		v.SetInt(int64(i))
		batch.Put(key, v.RawBytes)
	}
	data := batch.Finish()

	startKey := fmt.Sprintf("%s%08d", kv.prefix, firstRow)
	endKey := fmt.Sprintf("%s%08d", kv.prefix, lastRow)

	err := kv.db.WriteBatch(context.TODO(), startKey, endKey, data)
	if err != nil {
		panic(err)
	}
	return err
}

func (kv *kvWriteBatch) prep(rows int, initData bool) error {
	kv.epoch++
	kv.prefix = fmt.Sprintf("%d/", kv.epoch)
	if !initData {
		return nil
	}
	err := kv.db.Txn(context.TODO(), func(txn *client.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(b)
	})
	return err
}

func (kv *kvWriteBatch) done() {
	kv.doneFn()
}

func runKVBenchmark(b *testing.B, typ, op string, rows int) {
	var kv kvInterface
	switch typ {
	case "WriteBatch":
		kv = newKVWriteBatch(b)
	default:
		b.Fatalf("unknown implementation: %s", typ)
	}
	defer kv.done()

	var opFn func(int, int) error
	switch op {
	case "insert":
		opFn = kv.insert
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

// This benchmark mirrors the ones in sql_test, but can't live beside them
// because of ccl.
func BenchmarkKVInsert10000_WriteBatch(b *testing.B) {
	if !storage.ProposerEvaluatedKVEnabled() {
		b.Skip("command WriteBatch is not allowed without proposer evaluated KV")
	}
	runKVBenchmark(b, "WriteBatch", "insert", 10000)
}
