// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach-go/crdb"
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
