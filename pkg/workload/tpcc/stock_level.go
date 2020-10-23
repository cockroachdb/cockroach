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

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
	"golang.org/x/exp/rand"
)

// 2.8 The Stock-Level Transaction
//
// The Stock-Level business transaction determines the number of recently sold
// items that have a stock level below a specified threshold. It represents a
// heavy read-only database transaction with a low frequency of execution, a
// relaxed response time requirement, and relaxed consistency requirements.

// 2.8.2.3 states:
// Full serializability and repeatable reads are not required for the
// Stock-Level business transaction. All data read must be committed and no
// older than the most recently committed data prior to the time this business
// transaction was initiated. All other ACID properties must be maintained.
// TODO(jordan): can we take advantage of this?

type stockLevelData struct {
	// This data must all be returned by the transaction. See 2.8.3.4.
	dID       int
	threshold int
	lowStock  int
}

type stockLevel struct {
	config *tpcc
	mcp    *workload.MultiConnPool
	sr     workload.SQLRunner

	selectDNextOID    workload.StmtHandle
	countRecentlySold workload.StmtHandle
}

var _ tpccTx = &stockLevel{}

func createStockLevel(
	ctx context.Context, config *tpcc, mcp *workload.MultiConnPool,
) (tpccTx, error) {
	s := &stockLevel{
		config: config,
		mcp:    mcp,
	}

	s.selectDNextOID = s.sr.Define(`
		SELECT d_next_o_id
		FROM district
		WHERE d_w_id = $1 AND d_id = $2`,
	)

	// Count the number of recently sold items that have a stock level below
	// the threshold.
	// TODO(radu): we use count(DISTINCT s_i_id) because DISTINCT inside
	// aggregates was not supported by the optimizer. This can be cleaned up.
	s.countRecentlySold = s.sr.Define(`
		SELECT count(*) FROM (
			SELECT DISTINCT s_i_id
			FROM order_line
			JOIN stock
			ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			WHERE ol_w_id = $1
				AND ol_d_id = $2
				AND ol_o_id BETWEEN $3 - 20 AND $3 - 1
				AND s_quantity < $4
		)`,
	)

	if err := s.sr.Init(ctx, "stock-level", mcp, config.connFlags); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *stockLevel) run(ctx context.Context, wID int) (interface{}, error) {
	rng := rand.New(rand.NewSource(uint64(timeutil.Now().UnixNano())))

	// 2.8.1.2: The threshold of minimum quantity in stock is selected at random
	// within [10..20].
	d := stockLevelData{
		threshold: int(randInt(rng, 10, 20)),
		dID:       rng.Intn(10) + 1,
	}

	tx, err := s.mcp.Get().BeginEx(ctx, s.config.txOpts)
	if err != nil {
		return nil, err
	}
	if err := crdb.ExecuteInTx(
		ctx, (*workload.PgxTx)(tx),
		func() error {
			var dNextOID int
			if err := s.selectDNextOID.QueryRowTx(
				ctx, tx, wID, d.dID,
			).Scan(&dNextOID); err != nil {
				return err
			}

			// Count the number of recently sold items that have a stock level below
			// the threshold.
			return s.countRecentlySold.QueryRowTx(
				ctx, tx, wID, d.dID, dNextOID, d.threshold,
			).Scan(&d.lowStock)
		}); err != nil {
		return nil, err
	}
	return d, nil
}
