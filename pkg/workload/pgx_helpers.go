// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package workload

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

// MultiConnPool maintains a set of pgx ConnPools (to different servers).
type MultiConnPool struct {
	Pools []*pgxpool.Pool
	// Atomic counter used by Get().
	counter uint32
}

// MultiConnPoolCfg encapsulates the knobs passed to NewMultiConnPool.
type MultiConnPoolCfg struct {
	// MaxTotalConnections is the total maximum number of connections across all
	// pools.
	MaxTotalConnections int

	// MaxConnsPerPool is the maximum number of connections in any single pool.
	// Limiting this is useful especially for prepared statements, which are
	// prepared on each connection inside a pool (serially).
	// If 0, there is no per-pool maximum (other than the total maximum number of
	// connections which still applies).
	MaxConnsPerPool int

	PrepareStatementsFn func(context.Context, *pgx.Conn) error
}

// NewMultiConnPool creates a new MultiConnPool.
//
// Each URL gets one or more pools, and each pool has at most MaxConnsPerPool
// connections.
//
// The pools have approximately the same number of max connections, adding up to
// MaxTotalConnections.
func NewMultiConnPool(
	ctx context.Context, cfg MultiConnPoolCfg, urls ...string,
) (*MultiConnPool, error) {
	m := &MultiConnPool{}
	connsPerURL := distribute(cfg.MaxTotalConnections, len(urls))
	maxConnsPerPool := cfg.MaxConnsPerPool
	if maxConnsPerPool == 0 {
		maxConnsPerPool = cfg.MaxTotalConnections
	}

	var warmupConns [][]*pgxpool.Conn
	for i := range urls {
		connCfg, err := pgxpool.ParseConfig(urls[i])
		if err != nil {
			return nil, err
		}

		connCfg.AfterConnect = cfg.PrepareStatementsFn

		connsPerPool := distributeMax(connsPerURL[i], maxConnsPerPool)
		for _, numConns := range connsPerPool {
			connCfg.MaxConns = int32(numConns)
			p, err := pgxpool.ConnectConfig(ctx, connCfg)
			if err != nil {
				return nil, err
			}

			warmupConns = append(warmupConns, make([]*pgxpool.Conn, numConns))
			m.Pools = append(m.Pools, p)
		}
	}

	// "Warm up" the pools so we don't have to establish connections later (which
	// would affect the observed latencies of the first requests, especially when
	// prepared statements are used). We do this by
	// acquiring connections (in parallel), then releasing them back to the
	// pool.
	var g errgroup.Group
	// Limit concurrent connection establishment. Allowing this to run
	// at maximum parallelism would trigger syn flood protection on the
	// host, which combined with any packet loss could cause Acquire to
	// return an error and fail the whole function. The value 100 is
	// chosen because it is less than the default value for SOMAXCONN
	// (128).
	sem := make(chan struct{}, 100)
	for i, p := range m.Pools {
		p := p
		conns := warmupConns[i]
		for j := range conns {
			j := j
			sem <- struct{}{}
			g.Go(func() error {
				var err error
				conns[j], err = p.Acquire(ctx)
				<-sem
				return err
			})
		}
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	for i := range m.Pools {
		for _, c := range warmupConns[i] {
			c.Release()
		}
	}

	return m, nil
}

// Get returns one of the pools, in round-robin manner.
func (m *MultiConnPool) Get() *pgxpool.Pool {
	if len(m.Pools) == 1 {
		return m.Pools[0]
	}
	i := atomic.AddUint32(&m.counter, 1) - 1
	return m.Pools[i%uint32(len(m.Pools))]
}

// Close closes all the pools.
func (m *MultiConnPool) Close() {
	for _, p := range m.Pools {
		p.Close()
	}
}

// distribute returns a slice of <num> integers that add up to <total> and are
// within +/-1 of each other.
func distribute(total, num int) []int {
	res := make([]int, num)
	for i := range res {
		// Use the average number of remaining connections.
		div := len(res) - i
		res[i] = (total + div/2) / div
		total -= res[i]
	}
	return res
}

// distributeMax returns a slice of integers that are at most `max` and add up
// to <total>. The slice is as short as possible and the values are within +/-1
// of each other.
func distributeMax(total, max int) []int {
	return distribute(total, (total+max-1)/max)
}

// GetPreparedStatementsCallback creates the pgconn.AfterConnect callback to
// prepare statements.
func GetPreparedStatementsCallback(
	name string, stmts []string, preparedStmts map[string]*pgconn.StatementDescription,
) func(context.Context, *pgx.Conn) error {
	var lock = syncutil.RWMutex{}
	prepareStmtsFn := func(ctx context.Context, conn *pgx.Conn) error {
		for i, stmt := range stmts {
			stmtName := fmt.Sprintf("%s-%d", name, i)
			ps, err := conn.Prepare(ctx, stmtName, stmt)
			if err != nil {
				return err
			}
			// It doesn't matter which PreparedStatement we return, they should
			// contain the same information.
			if _, ok := preparedStmts[stmt]; !ok {
				// Don't allow concurrent writes to map.
				lock.Lock()
				preparedStmts[stmt] = ps
				lock.Unlock()
			}
		}
		return nil
	}
	return prepareStmtsFn
}

// ExecuteInTx runs fn inside tx. This method is primarily intended for internal
// use. See other packages for higher-level, framework-specific ExecuteTx()
// functions.
//
// *WARNING*: It is assumed that no statements have been executed on the
// supplied Tx. ExecuteInTx will only retry statements that are performed within
// the supplied closure (fn). Any statements performed on the tx before
// ExecuteInTx is invoked will *not* be re-run if the transaction needs to be
// retried.
//
// fn is subject to the same restrictions as the fn passed to ExecuteTx.
//
// This is taken from github.com/cockroachdb/cockroach-go/crdb/common.go.
// and duplicated to be used with pgx.Tx instead of crdb.Tx.
// TODO(richardjcai): We can likely get rid of the copy used in common.go.
func ExecuteInTx(ctx context.Context, tx pgx.Tx, fn func() error) (err error) {
	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = tx.Commit(ctx)
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = tx.Rollback(ctx)
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if _, err = tx.Exec(ctx, "SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		released := false
		err = fn()
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if _, err = tx.Exec(ctx, "RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart.
		if !errIsRetryable(err) {
			if released {
				err = newAmbiguousCommitError(err)
			}
			return err
		}

		if _, retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
			return newTxnRestartError(retryErr, err)
		}
	}
}

func errIsRetryable(err error) bool {
	// We look for either:
	//  - the standard PG errcode SerializationFailureError:40001 or
	//  - the Cockroach extension errcode RetriableError:CR000. This extension
	//    has been removed server-side, but support for it has been left here for
	//    now to maintain backwards compatibility.
	code := errCode(err)
	return code == "CR000" || code == "40001"
}

func errCode(err error) string {
	switch t := errorCause(err).(type) {
	case *pq.Error:
		return string(t.Code)

	case errWithSQLState:
		return t.SQLState()

	default:
		return ""
	}
}

// errorCause returns the original cause of the error, if possible. An
// error has a proximate cause if it's type is compatible with Go's
// errors.Unwrap() or pkg/errors' Cause(); the original cause is the
// end of the causal chain.
func errorCause(err error) error {
	for err != nil {
		if c, ok := err.(interface{ Cause() error }); ok {
			err = c.Cause()
		} else if c, ok := err.(interface{ Unwrap() error }); ok {
			err = c.Unwrap()
		} else {
			break
		}
	}
	return err
}

type txError struct {
	cause error
}

// Error implements the error interface.
func (e *txError) Error() string { return e.cause.Error() }

// Cause implements the pkg/errors causer interface.
func (e *txError) Cause() error { return e.cause }

// Unwrap implements the go error causer interface.
func (e *txError) Unwrap() error { return e.cause }

// AmbiguousCommitError represents an error that left a transaction in an
// ambiguous state: unclear if it committed or not.
type AmbiguousCommitError struct {
	txError
}

func newAmbiguousCommitError(err error) *AmbiguousCommitError {
	return &AmbiguousCommitError{txError{cause: err}}
}

// TxnRestartError represents an error when restarting a transaction. `cause` is
// the error from restarting the txn and `retryCause` is the original error which
// triggered the restart.
type TxnRestartError struct {
	txError
	retryCause error
	msg        string
}

func newTxnRestartError(err error, retryErr error) *TxnRestartError {
	const msgPattern = "restarting txn failed. ROLLBACK TO SAVEPOINT " +
		"encountered error: %s. Original error: %s."
	return &TxnRestartError{
		txError:    txError{cause: err},
		retryCause: retryErr,
		msg:        fmt.Sprintf(msgPattern, err, retryErr),
	}
}

// Error implements the error interface.
func (e *TxnRestartError) Error() string { return e.msg }

// RetryCause returns the error that caused the transaction to be restarted.
func (e *TxnRestartError) RetryCause() error { return e.retryCause }

// errWithSQLState is implemented by pgx (pgconn.PgError).
//
// TODO(andrei): Add this method to pq.Error and stop depending on lib/pq.
type errWithSQLState interface {
	SQLState() string
}
