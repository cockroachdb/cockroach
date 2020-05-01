// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ledger

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
)

type worker struct {
	config *ledger
	hists  *histogram.Histograms
	db     *gosql.DB

	rng      *rand.Rand
	deckPerm []int
	permIdx  int
}

type ledgerTx interface {
	run(config *ledger, db *gosql.DB, rng *rand.Rand) (interface{}, error)
}

type tx struct {
	ledgerTx
	weight int    // percent likelihood that each transaction type is run
	name   string // display name
}

var allTxs = [...]tx{
	{
		ledgerTx: balance{}, name: "balance",
	},
	{
		ledgerTx: withdrawal{}, name: "withdrawal",
	},
	{
		ledgerTx: deposit{}, name: "deposit",
	},
	{
		ledgerTx: reversal{}, name: "reversal",
	},
}

func initializeMix(config *ledger) error {
	config.txs = append([]tx(nil), allTxs[0:]...)
	nameToTx := make(map[string]int, len(allTxs))
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

	start := timeutil.Now()
	if _, err := t.run(w.config, w.db, w.rng); err != nil {
		return errors.Wrapf(err, "error in %s", t.name)
	}
	elapsed := timeutil.Since(start)
	w.hists.Get(t.name).Record(elapsed)
	return nil
}
