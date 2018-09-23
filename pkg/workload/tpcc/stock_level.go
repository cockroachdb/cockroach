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
	gosql "database/sql"
	"fmt"
	"math/rand"

	"context"

	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

type stockLevel struct{}

var _ tpccTx = stockLevel{}

func (s stockLevel) run(
	ctx context.Context, config *tpcc, db *gosql.DB, wID int,
) (interface{}, error) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	// 2.8.1.2: The threshold of minimum quantity in stock is selected at random
	// within [10..20].
	d := stockLevelData{
		threshold: randInt(rng, 10, 20),
		dID:       rng.Intn(10) + 1,
	}

	if err := crdb.ExecuteTx(
		ctx,
		db,
		config.txOpts,
		func(tx *gosql.Tx) error {
			// This is the only join in the application, so we don't need to worry about
			// this setting persisting incorrectly across queries.
			// Note that this is not needed (and doesn't do anything) when the
			// optimizer is on. We still set it for when the optimizer is disabled
			// or when running against older versions of CRDB.
			if _, err := tx.Exec(`set experimental_force_lookup_join=true`); err != nil {
				return err
			}

			var dNextOID int
			if err := tx.QueryRowContext(ctx, fmt.Sprintf(`
				SELECT d_next_o_id
				FROM district
				WHERE d_w_id = %[1]d AND d_id = %[2]d`,
				wID, d.dID),
			).Scan(&dNextOID); err != nil {
				return err
			}

			// Count the number of recently sold items that have a stock level below
			// the threshold.
			// Note: we don't use count(DISTINCT s_i_id) because DISTINCT inside
			// aggregates is not yet supported by the optimizer.
			return tx.QueryRowContext(ctx, fmt.Sprintf(`
			  SELECT count(*) FROM (
			  	SELECT DISTINCT s_i_id
			  	FROM order_line
			  	JOIN stock
			  	ON s_i_id=ol_i_id AND s_w_id=ol_w_id
			  	WHERE ol_w_id = %[1]d
			  	  AND ol_d_id = %[2]d
			  	  AND ol_o_id BETWEEN %[3]d - 20 AND %[3]d - 1
			  	  AND s_quantity < %[4]d
			  )`,
				wID, d.dID, dNextOID, d.threshold),
			).Scan(&d.lowStock)
		}); err != nil {
		return nil, err
	}
	return d, nil
}
