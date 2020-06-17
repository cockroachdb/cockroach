// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlliveness

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

type Claim struct {
	name  string
	epoch int64
}

// OptionalNodeLivenessI is the interface used in OptionalNodeLiveness.
type Liveness interface {
	Self() (Liveness, error)
	GetLiveNames() []Liveness
	IsLive(string) (bool, error)
}

type SqlLiveness struct {
	db   *kv.DB
	ex   sqlutil.InternalExecutor
	name string
	// Epoch is a monotonically-increasing value for node liveness. It
	// may be incremented if the liveness record expires (current time
	// is later than the expiration timestamp).
	Epoch int64

	// The timestamp at which this liveness record expires. The logical part of
	// this timestamp is zero.
	Expiration hlc.Timestamp
}

func (l *SqlLiveness) GetLiveClaims(ctx context.Context) []Claim {
	return l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		row, err := l.ex.QueryRowEx(
			ctx, "get-expired-ids", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.sqlliveness SET epoch = epoch + 1
			WHERE expiration <= $1 RETURNING name, epoch`
			r.ID(), operatingStatus,
		)
		if err != nil {
			return errors.Wrap(err, "could not query jobs table")
		}
		if row == nil {
			// TODO handle this more gracefully
			panic("we have lost our liveness record")
		}
		epoch := int64(*row[0].(*tree.DInt))
	})
}
