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
// permissions and limitations under the License.

package main

import (
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var flagNumTxns = flag.Int("txns", 100, "number of transactions to write")
var flagNumIntentsPerTxn = flag.Int("intents", 0, "number of (fictional) intents per transactions")

func main() {
	flag.Parse()

	targetStatus := [3]roachpb.TransactionStatus{
		roachpb.COMMITTED,
		roachpb.COMMITTED,
		roachpb.COMMITTED,
	}
	var _ = targetStatus

	ctx, cancel := context.WithCancel(context.Background())

	cockroachBin := func() string {
		bin := "./cockroach"
		if _, err := os.Stat(bin); os.IsNotExist(err) {
			bin = "cockroach"
		} else if err != nil {
			panic(err)
		}
		return bin
	}()

	const numNodes = 1

	perNodeCfg := localcluster.MakePerNodeFixedPortsCfg(numNodes)

	cfg := localcluster.ClusterConfig{
		DataDir:     "cockroach-data-gcpressurizer",
		Binary:      cockroachBin,
		NumNodes:    numNodes,
		NumWorkers:  numNodes,
		AllNodeArgs: flag.Args(),
		DB:          "gcpressurizer",
		PerNodeCfg:  perNodeCfg,
	}

	c := localcluster.LocalCluster{
		Cluster: localcluster.New(cfg),
	}

	cleanupOnce := func() func() {
		var once sync.Once
		return func() {
			once.Do(func() {
				c.Close()
			})
		}
	}()
	defer cleanupOnce()

	log.SetExitFunc(func(code int) {
		cleanupOnce()
		os.Exit(code)
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		s := <-signalCh
		log.Infof(context.Background(), "signal received: %v", s)
		cancel()
		cleanupOnce()
		os.Exit(1)
	}()

	c.Start(context.Background())

	time.Sleep(2 * time.Second)

	if _, err := c.Nodes[0].DB().Exec("SET CLUSTER SETTING trace.debug.enable = true"); err != nil {
		log.Fatal(ctx, err)
	}

	const (
		txnsPerBatch = 100
	)

	var seq uint64
	randKey := func() roachpb.Key {
		seq++
		var key roachpb.Key
		key = encoding.EncodeUvarintAscending(key, 999)
		key = encoding.EncodeUvarintAscending(key, seq)
		return keys.MakeFamilyKey(key, 0) // really just encodes another Uvarint zero
	}

	// One year ago.
	pastTS := timeutil.Now().Add(-24 * time.Hour * 365).UnixNano()

	t := timeutil.NewTimer()
	t.Reset(time.Second)

	conn, err := c.Nodes[0].Conn()
	if err != nil {
		log.Fatal(ctx, err)
	}
	kv, err := roachpb.NewKVDebugClient(conn).EvalEngine(ctx)
	if err != nil {
		log.Fatal(ctx, err)
	}

	{
		tmp := filepath.Join(cfg.DataDir, "zone")
		if err := ioutil.WriteFile(tmp, []byte("range_max_bytes: 6710886400\nnum_replicas: 1\ngc:\n  ttlseconds: 600\n"), 0755); err != nil {
			log.Fatal(ctx, err)
		}
		if _, _, err := c.ExecCLI(ctx, 0, []string{"zone", "set", ".default", "--file", tmp}); err != nil {
			log.Fatal(ctx, err)
		}
		_ = os.Remove(tmp)
	}

	time.Sleep(5 * time.Second)

	send := func(b client.Batch) (*roachpb.BatchResponse, *roachpb.Error) {
		var ba roachpb.BatchRequest
		ba.Header = b.Header
		ba.Requests = b.RawRequests()
		if err := kv.Send(&ba); err != nil {
			return nil, roachpb.NewError(err)
		}
		br, err := kv.Recv()
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		var pErr *roachpb.Error
		pErr, br.Error = br.Error, nil
		return br, pErr
	}

	var cTotalIntents int
	var cTotalTxns int

	var txnB client.Batch
	flushAndlog := func() {
		if _, pErr := send(txnB); pErr != nil {
			log.Fatal(ctx, pErr)
		}
		txnB = client.Batch{}
		log.Infof(ctx, "txns: %d (total intents %d)", cTotalTxns, cTotalIntents)
	}

	for cTotalTxns < *flagNumTxns {
		numTxnsInBatch := *flagNumTxns
		if numTxnsInBatch > txnsPerBatch {
			numTxnsInBatch = txnsPerBatch
		}
		for i := 0; i < numTxnsInBatch; i++ {
			numIntents := *flagNumIntentsPerTxn
			if numIntents == 0 {
				numIntents = 1
			}
			intents := make([]roachpb.Span, numIntents)
			var b client.Batch
			for s := range intents {
				intents[s] = roachpb.Span{
					Key: randKey(),
				}
				// intents[s].EndKey = append(roachpb.Key(nil), intents[s].Key...)
				// intents[s].EndKey = append(intents[s].EndKey, randKey()...)
				b.Put(intents[s].Key, "garbage day")

			}
			if *flagNumIntentsPerTxn > 0 {
				cTotalIntents += len(intents)
			}
			txn := roachpb.MakeTransaction(
				"test",
				intents[0].Key,
				roachpb.NormalUserPriority,
				enginepb.SERIALIZABLE,
				hlc.Timestamp{WallTime: pastTS},
				500*time.Millisecond.Nanoseconds(),
			)

			if *flagNumIntentsPerTxn > 0 {
				txn.Intents = intents
				b.Header.Txn = &txn
				b.Header.Timestamp = txn.Timestamp

				if _, pErr := send(b); pErr != nil {
					log.Fatal(ctx, pErr)
				}
			}

			txn.Status = targetStatus[i%len(targetStatus)]
			txnB.Put(keys.TransactionKey(txn.Key, txn.ID), &txn)

			cTotalTxns++

			select {
			case <-t.C:
				t.Read = true
				t.Reset(time.Second)
				flushAndlog()
			default:
			}
		}
	}
	flushAndlog()

	//close(signalCh)

	time.Sleep(time.Hour)
}
