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

package tpcc

import (
	"context"
	gosql "database/sql"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/pkg/errors"
)

const (
	numWorkersPerWarehouse = 10
)

type worker struct {
	config    *tpcc
	hists     *workload.Histograms
	idx       int
	db        *gosql.DB
	warehouse int

	deckPerm []int
	permIdx  int
}

type tpccTx interface {
	run(config *tpcc, db *gosql.DB, wID int) (interface{}, error)
}

type tx struct {
	tpccTx
	weight     int     // percent likelihood that each transaction type is run
	name       string  // display name
	keyingTime int     // keying time in seconds, see 5.2.5.7
	thinkTime  float64 // minimum mean of think time distribution, 5.2.5.7
}

var allTxs = [...]tx{
	{
		tpccTx: newOrder{}, name: "newOrder",
		keyingTime: 18,
		thinkTime:  12,
	},
	{
		tpccTx: payment{}, name: "payment",
		keyingTime: 3,
		thinkTime:  12,
	},
	{
		tpccTx: orderStatus{}, name: "orderStatus",
		keyingTime: 2,
		thinkTime:  10,
	},
	{
		tpccTx: delivery{}, name: "delivery",
		keyingTime: 2,
		thinkTime:  5,
	},
	{
		tpccTx: stockLevel{}, name: "stockLevel",
		keyingTime: 2,
		thinkTime:  5,
	},
}

func initializeMix(config *tpcc) error {
	config.txs = append([]tx(nil), allTxs[0:]...)
	nameToTx := make(map[string]int)
	for i, tx := range config.txs {
		nameToTx[tx.name] = i
	}

	items := strings.Split(config.mix, `,`)
	totalWeight := 0
	for _, item := range items {
		kv := strings.Split(item, `=`)
		if len(kv) != 2 {
			return errors.Errorf(`Invalid mix %s: %s is not a k=v pair`, config.mix, item)
		}
		txName, weightStr := kv[0], kv[1]

		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			return errors.Errorf(
				`Invalid percentage mix %s: %s is not an integer`, config.mix, weightStr)
		}

		i, ok := nameToTx[txName]
		if !ok {
			return errors.Errorf(
				`Invalid percentage mix %s: no such transaction %s`, config.mix, txName)
		}

		config.txs[i].weight = weight
		totalWeight += weight
	}

	config.deck = make([]int, 0, totalWeight)
	for i, t := range config.txs {
		for j := 0; j < t.weight; j++ {
			config.deck = append(config.deck, i)
		}
	}

	return nil
}

func (w *worker) run(ctx context.Context) error {
	// 5.2.4.2: the required mix is achieved by selecting each new transaction
	// uniformly at random from a deck whose content guarantees the required
	// transaction mix. Each pass through a deck must be made in a different
	// uniformly random order.
	if w.permIdx == len(w.deckPerm) {
		rand.Shuffle(len(w.deckPerm), func(i, j int) {
			w.deckPerm[i], w.deckPerm[j] = w.deckPerm[j], w.deckPerm[i]
		})
		w.permIdx = 0
	}
	// Move through our permutation slice until its exhausted, using each value to
	// to index into our deck of transactions, which contains indexes into the
	// txs slice.
	opIdx := w.deckPerm[w.permIdx]
	t := w.config.txs[opIdx]
	w.permIdx++

	warehouseID := w.warehouse
	if !w.config.doWaits {
		warehouseID = rand.Intn(w.config.warehouses)
	} else {
		time.Sleep(time.Duration(t.keyingTime) * time.Second)
	}

	start := timeutil.Now()
	if _, err := t.run(w.config, w.db, warehouseID); err != nil {
		return errors.Wrapf(err, "error in %s", t.name)
	}
	w.hists.Get(t.name).Record(timeutil.Since(start))

	if w.config.doWaits {
		// 5.2.5.4: Think time is taken independently from a negative exponential
		// distribution. Think time = -log(r) * u, where r is a uniform random number
		// between 0 and 1 and u is the mean think time per operation.
		// Each distribution is truncated at 10 times its mean value.
		thinkTime := -math.Log(rand.Float64()) * t.thinkTime
		if thinkTime > (t.thinkTime * 10) {
			thinkTime = t.thinkTime * 10
		}
		time.Sleep(time.Duration(thinkTime) * time.Second)
	}
	return nil
}
