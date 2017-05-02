// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

type kvInterface interface {
	Insert(rows, run int) error

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
			s.Stopper().Stop(context.TODO())
			enableTracing()
		},
	}
}

func (kv *kvWriteBatch) Insert(rows, run int) error {
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

	return kv.db.WriteBatch(context.TODO(), startKey, endKey, data)
}

func (kv *kvWriteBatch) prep(rows int, initData bool) error {
	kv.epoch++
	kv.prefix = fmt.Sprintf("%d/", kv.epoch)
	if !initData {
		return nil
	}
	return kv.db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		for i := 0; i < rows; i++ {
			b.Put(fmt.Sprintf("%s%08d", kv.prefix, i), i)
		}
		return txn.CommitInBatch(ctx, b)
	})
}

func (kv *kvWriteBatch) done() {
	kv.doneFn()
}

func BenchmarkKV(b *testing.B) {
	for _, opFn := range []func(kvInterface, int, int) error{
		kvInterface.Insert,
	} {
		opName := runtime.FuncForPC(reflect.ValueOf(opFn).Pointer()).Name()
		opName = strings.TrimPrefix(opName, "github.com/cockroachdb/cockroach/pkg/ccl/sqlccl.kvInterface.")
		for _, rows := range []int{10000} {
			for _, kvFn := range []func(*testing.B) kvInterface{
				newKVWriteBatch,
			} {
				kvTyp := runtime.FuncForPC(reflect.ValueOf(kvFn).Pointer()).Name()
				kvTyp = strings.TrimPrefix(kvTyp, "github.com/cockroachdb/cockroach/pkg/ccl/sqlccl.newKV")
				for _, initData := range []bool{false} { // TODO(dan): add true
					b.Run(fmt.Sprintf("%s%d_%s initData=%t", opName, rows, kvTyp, initData), func(b *testing.B) {
						kv := kvFn(b)
						defer kv.done()

						if err := kv.prep(rows, initData); err != nil {
							b.Fatal(err)
						}
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if err := opFn(kv, rows, i); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
					})
				}
			}
		}
	}
}
