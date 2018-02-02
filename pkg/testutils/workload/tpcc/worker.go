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

package main

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codahale/hdrhistogram"
	"github.com/pkg/errors"
)

type worker struct {
	idx       int
	db        *sql.DB
	warehouse int
	latency   struct {
		sync.Mutex
		*hdrhistogram.WindowedHistogram
		byOp []*hdrhistogram.WindowedHistogram
	}
}

func clampLatency(d, min, max time.Duration) time.Duration {
	if d < min {
		return min
	}
	if d > max {
		return max
	}
	return d
}

type txType int

const (
	newOrderType txType = iota
	paymentType
	orderStatusType
	deliveryType
	stockLevelType
)
const nTxTypes = 5

type tpccTx interface {
	run(db *sql.DB, wID int) (interface{}, error)
}

type tx struct {
	tpccTx
	weight     int    // percent likelihood that each transaction type is run
	name       string // display name
	numOps     uint64
	keyingTime int     // keying time in seconds, see 5.2.5.7
	thinkTime  float64 // minimum mean of think time distribution, 5.2.5.7
}

// Keep this in the same order as the const type enum above, since it's used as a map from tx type
// to struct.
var txs = [...]tx{
	newOrderType: {
		tpccTx: newOrder{}, name: "newOrder",
		keyingTime: 18,
		thinkTime:  12,
	},
	paymentType: {
		tpccTx: payment{}, name: "payment",
		keyingTime: 3,
		thinkTime:  12,
	},
	orderStatusType: {
		tpccTx: orderStatus{}, name: "orderStatus",
		keyingTime: 2,
		thinkTime:  10,
	},
	deliveryType: {
		tpccTx: delivery{}, name: "delivery",
		keyingTime: 2,
		thinkTime:  5,
	},
	stockLevelType: {
		tpccTx: stockLevel{}, name: "stockLevel",
		keyingTime: 2,
		thinkTime:  5,
	},
}

var totalWeight int

func initializeMix() {
	nameToTx := make(map[string]txType)
	for i, tx := range txs {
		nameToTx[tx.name] = txType(i)
	}

	items := strings.Split(*mix, ",")
	for _, item := range items {
		kv := strings.Split(item, "=")
		if len(kv) != 2 {
			log.Fatalf("Invalid mix %s: %s is not a k=v pair", *mix, item)
		}
		txName, weightStr := kv[0], kv[1]

		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			log.Fatalf("Invalid percentage mix %s: %s is not an integer", *mix, weightStr)
		}

		txIdx, ok := nameToTx[txName]
		if !ok {
			log.Fatalf("Invalid percentage mix %s: no such transaction %s", *mix, txName)
		}

		txs[txIdx].weight = weight
		totalWeight += weight
	}
	if *verbose {
		scaleFactor := 100.0 / float64(totalWeight)

		fmt.Printf("Running with mix ")
		for _, tx := range txs {
			fmt.Printf("%s=%.0f%% ", tx.name, float64(tx.weight)*scaleFactor)
		}
		fmt.Printf("\n")
	}
}

func newWorker(i int, db *sql.DB, wg *sync.WaitGroup) *worker {
	wg.Add(1)
	w := &worker{idx: i, db: db}
	w.latency.WindowedHistogram = hdrhistogram.NewWindowed(1,
		minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)

	w.latency.byOp = make([]*hdrhistogram.WindowedHistogram, nTxTypes)
	for i := 0; i < nTxTypes; i++ {
		w.latency.byOp[i] = hdrhistogram.NewWindowed(1,
			minLatency.Nanoseconds(), maxLatency.Nanoseconds(), 1)
	}
	return w
}

func (w *worker) run(errCh chan<- error, warehouseID int) {
	for {
		transactionType := rand.Intn(totalWeight)
		weightSum := 0
		var i int
		var t tx
		for i, t = range txs {
			weightSum += t.weight
			if transactionType < weightSum {
				break
			}
		}
		if *noWait {
			warehouseID = rand.Intn(*warehouses)
		} else {
			time.Sleep(time.Duration(t.keyingTime) * time.Second)
		}

		start := time.Now()
		if _, err := t.run(w.db, warehouseID); err != nil {
			errCh <- errors.Wrapf(err, "error in %s", t.name)
			continue
		}
		elapsed := clampLatency(time.Since(start), minLatency, maxLatency).Nanoseconds()
		w.latency.Lock()
		if err := w.latency.Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		if err := w.latency.byOp[i].Current.RecordValue(elapsed); err != nil {
			log.Fatal(err)
		}
		w.latency.Unlock()
		atomic.AddUint64(&txs[i].numOps, 1)
		v := atomic.AddUint64(&numOps, 1)
		if *maxOps > 0 && v >= *maxOps {
			return
		}

		if !*noWait {
			// 5.2.5.4: Think time is taken independently from a negative exponential
			// distribution. Think time = -log(r) * u, where r is a uniform random number
			// between 0 and 1 and u is the mean think time per operation.
			// Each distribution is truncated at 10 times its mean value.
			thinkTime := -math.Log(rand.Float64()) * float64(t.thinkTime)
			if thinkTime > (t.thinkTime * 10) {
				thinkTime = t.thinkTime * 10
			}
			time.Sleep(time.Duration(thinkTime) * time.Second)
		}
	}
}
