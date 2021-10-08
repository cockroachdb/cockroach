// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/exp/rand"
)

const (
	numWorkersPerWarehouse = 10
	numConnsPerWarehouse   = 2
)

// tpccTX is an interface for running a TPCC transaction.
type tpccTx interface {
	// run executes the TPCC transaction against the given warehouse ID.
	run(ctx context.Context, wID int) (interface{}, error)
}

type createTxFn func(ctx context.Context, config *tpcc, mcp *workload.MultiConnPool) (tpccTx, error)

// txInfo stores high-level information about the TPCC transactions. The create
// function is used to create an object that implements tpccTx.
type txInfo struct {
	name        string // display name
	constructor createTxFn
	keyingTime  int     // keying time in seconds, see 5.2.5.7
	thinkTime   float64 // minimum mean of think time distribution, 5.2.5.7
	weight      int     // percent likelihood that each transaction type is run
}

var allTxs = [...]txInfo{
	{
		name:        "newOrder",
		constructor: createNewOrder,
		keyingTime:  18,
		thinkTime:   12,
	},
	{
		name:        "payment",
		constructor: createPayment,
		keyingTime:  3,
		thinkTime:   12,
	},
	{
		name:        "orderStatus",
		constructor: createOrderStatus,
		keyingTime:  2,
		thinkTime:   10,
	},
	{
		name:        "delivery",
		constructor: createDelivery,
		keyingTime:  2,
		thinkTime:   5,
	},
	{
		name:        "stockLevel",
		constructor: createStockLevel,
		keyingTime:  2,
		thinkTime:   5,
	},
}

type txCounter struct {
	// success and error count the number of successes and failures, respectively,
	// for the given tx.
	success, error prometheus.Counter
}

type txCounters map[string]txCounter

func setupTPCCMetrics(reg prometheus.Registerer) txCounters {
	m := txCounters{}
	f := promauto.With(reg)
	for _, tx := range allTxs {
		m[tx.name] = txCounter{
			success: f.NewCounter(
				prometheus.CounterOpts{
					Namespace: histogram.PrometheusNamespace,
					Subsystem: tpccMeta.Name,
					Name:      fmt.Sprintf("%s_success_total", tx.name),
					Help:      fmt.Sprintf("The total number of successful %s transactions.", tx.name),
				},
			),
			error: f.NewCounter(
				prometheus.CounterOpts{
					Namespace: histogram.PrometheusNamespace,
					Subsystem: tpccMeta.Name,
					Name:      fmt.Sprintf("%s_error_total", tx.name),
					Help:      fmt.Sprintf("The total number of error %s transactions.", tx.name),
				}),
		}
	}
	return m
}

func initializeMix(config *tpcc) error {
	config.txInfos = append([]txInfo(nil), allTxs[0:]...)
	nameToTx := make(map[string]int)
	for i, tx := range config.txInfos {
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

		config.txInfos[i].weight = weight
		totalWeight += weight
	}

	config.deck = make([]int, 0, totalWeight)
	for i, t := range config.txInfos {
		for j := 0; j < t.weight; j++ {
			config.deck = append(config.deck, i)
		}
	}

	return nil
}

type worker struct {
	config *tpcc
	// txs maps 1-to-1 with config.txInfos.
	txs       []tpccTx
	hists     *histogram.Histograms
	warehouse int

	deckPerm []int
	permIdx  int

	counters txCounters
}

func newWorker(
	ctx context.Context,
	config *tpcc,
	mcp *workload.MultiConnPool,
	hists *histogram.Histograms,
	counters txCounters,
	warehouse int,
) (*worker, error) {
	w := &worker{
		config:    config,
		txs:       make([]tpccTx, len(config.txInfos)),
		hists:     hists,
		warehouse: warehouse,
		deckPerm:  append([]int(nil), config.deck...),
		permIdx:   len(config.deck),
		counters:  counters,
	}
	for i := range w.txs {
		var err error
		w.txs[i], err = config.txInfos[i].constructor(ctx, config, mcp)
		if err != nil {
			return nil, err
		}
	}
	return w, nil
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
	// txInfos / txs slices.
	opIdx := w.deckPerm[w.permIdx]
	txInfo := &w.config.txInfos[opIdx]
	tx := w.txs[opIdx]
	w.permIdx++

	warehouseID := w.warehouse
	// Wait out the entire keying and think time even if the context is
	// expired. This prevents all workers from immediately restarting when
	// the workload's ramp period expires, which can overload a cluster.
	time.Sleep(time.Duration(float64(txInfo.keyingTime) * float64(time.Second) * w.config.waitFraction))

	// Run transactions with a background context because we don't want to
	// cancel them when the context expires. Instead, let them finish normally
	// but don't account for them in the histogram.
	start := timeutil.Now()
	if _, err := tx.run(context.Background(), warehouseID); err != nil {
		w.counters[txInfo.name].error.Inc()
		return errors.Wrapf(err, "error in %s", txInfo.name)
	}
	if ctx.Err() == nil {
		elapsed := timeutil.Since(start)
		// NB: this histogram *should* be named along the lines of
		// `txInfo.name+"_success"` but we already rely on the names and shouldn't
		// change them now.
		w.hists.Get(txInfo.name).Record(elapsed)
	}
	w.counters[txInfo.name].success.Inc()

	// 5.2.5.4: Think time is taken independently from a negative exponential
	// distribution. Think time = -log(r) * u, where r is a uniform random number
	// between 0 and 1 and u is the mean think time per operation.
	// Each distribution is truncated at 10 times its mean value.
	thinkTime := -math.Log(rand.Float64()) * txInfo.thinkTime
	if thinkTime > (txInfo.thinkTime * 10) {
		thinkTime = txInfo.thinkTime * 10
	}
	time.Sleep(time.Duration(thinkTime * float64(time.Second) * w.config.waitFraction))
	return ctx.Err()
}
