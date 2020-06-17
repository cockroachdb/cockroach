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
	"github.com/pkg/errors"
)

type Claim struct {
	Name  string
	Epoch int64
}

// OptionalNodeLivenessI is the interface used in OptionalNodeLiveness.
type ClaimManager interface {
	CreateClaim(context.Context, string, time.Duration) (*Claim, error)
	ExtendClaim(context.Context, *Claim, time.Duration) (*time.Time, error)
	GetLiveClaims(context.Context) ([]*Claim, error)
}

type SqlLiveness struct {
	db *kv.DB
	ex sqlutil.InternalExecutor
}

func NewSqlLiveness(db *kv.DB, ex sqlutil.InternalExecutor) ClaimManager {
	return &SqlLiveness{
		db: db,
		ex: ex,
	}
}

func (l *SqlLiveness) GetLiveClaims(ctx context.Context) ([]*Claim, error) {
	var res []*Claim
	var rows []tree.Datums
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		rows, err = l.ex.QueryEx(
			ctx, "get-expired-ids", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.sqlliveness
			SET epoch = CASE WHEN expiration <= now() THEN epoch + 1 ELSE epoch END
			WHERE epoch NOT NULL RETURNING name, epoch`,
		)
		return err
	}); err != nil {
		return nil, errors.Wrapf(err, "update sqlliveness claims")
	}
	res = make([]*Claim, len(rows))
	for i, r := range rows {
		res[i] = &Claim{
			Name:  string(*r[0].(*tree.DString)),
			Epoch: int64(*r[1].(*tree.DInt)),
		}
	}
	return res, nil
}

var (
	MissingClaimErr = errors.New("")
)

func (l *SqlLiveness) CreateClaim(
	ctx context.Context, name string, d time.Duration,
) (*Claim, error) {
	if err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := l.ex.QueryRowEx(
			ctx, "extend-claim", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`INSERT INTO system.sqlliveness VALUES ($1, 0, now() + $2)`,
			name, d.Microseconds(),
		)
		if err != nil {
			return errors.Wrapf(err, "create claim for name %s)", name)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &Claim{Name: name, Epoch: 0}, nil
}

func (l *SqlLiveness) ExtendClaim(
	ctx context.Context, c *Claim, d time.Duration,
) (*time.Time, error) {
	var res *time.Time
	err := l.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		row, err := l.ex.QueryRowEx(
			ctx, "extend-claim", txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			`UPDATE system.sqlliveness SET expiration = now() + '$1 microsecond'
			WHERE name = $2 AND epoch = $3 RETURNING expiration`,
			d.Microseconds(), c.Name, c.Epoch,
		)
		if err != nil {
			return errors.Wrapf(err, "extend liveness claim (%s, %d)", c.Name, c.Epoch)
		}
		if row == nil {
			return errors.Wrapf(MissingClaimErr, "claim (%s, %d)", c.Name, c.Epoch)
		}
		res = &row[0].(*tree.DTimestampTZ).Time
		return nil
	})
	return res, err
}
