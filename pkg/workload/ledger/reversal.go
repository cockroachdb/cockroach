// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ledger

import (
	"context"
	gosql "database/sql"
	"math/rand"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
)

type reversal struct{}

var _ ledgerTx = reversal{}

func (reversal) run(config *ledger, db *gosql.DB, rng *rand.Rand) (interface{}, error) {
	err := crdb.ExecuteTx(
		context.Background(),
		db,
		nil, /* txopts */
		func(tx *gosql.Tx) error {
			// TODO(nvanbenschoten): complete this transaction
			// if we want to support this operation.
			return nil
		})
	return nil, err
}
