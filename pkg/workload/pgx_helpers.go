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
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/marusama/semaphore"
	"golang.org/x/sync/errgroup"
)

// MultiConnPool maintains a set of pgx ConnPools (to different servers).
type MultiConnPool struct {
	Pools []*pgxpool.Pool
	// Atomic counter used by Get().
	counter uint32

	mu struct {
		syncutil.RWMutex
		// preparedStatements is a map from name to SQL. The statements in the map
		// are prepared whenever a new connection is acquired from the pool.
		preparedStatements map[string]string
	}
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
}

// pgxLogger implements the pgx.Logger interface.
type pgxLogger struct {
	tracelog.TraceLog
}

var _ tracelog.Logger = pgxLogger{}

// Log implements the pgx.Logger interface.
func (p pgxLogger) Log(
	ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{},
) {
	if ctx.Err() != nil {
		// Don't log anything from pgx if the context was canceled by the workload
		// runner. It would result in spam at the end of every workload.
		return
	}
	if strings.Contains(msg, "restart transaction") {
		// Our workloads have a lot of contention, so "restart transaction" messages
		// are expected and noisy.
		return
	}
	// data may contain error with "restart transaction" -- skip those as well.
	if data != nil {
		ev := data["err"]
		if err, ok := ev.(error); ok && strings.Contains(err.Error(), "restart transaction") {
			return
		}
	}
	log.Infof(ctx, "pgx logger [%s]: %s logParams=%v", level.String(), msg, data)
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
	m.mu.preparedStatements = map[string]string{}

	connsPerURL := distribute(cfg.MaxTotalConnections, len(urls))
	maxConnsPerPool := cfg.MaxConnsPerPool
	if maxConnsPerPool == 0 {
		maxConnsPerPool = cfg.MaxTotalConnections
	}

	for i := range urls {
		connsPerPool := distributeMax(connsPerURL[i], maxConnsPerPool)
		for _, numConns := range connsPerPool {
			poolCfg, err := pgxpool.ParseConfig(urls[i])
			if err != nil {
				return nil, err
			}
			poolCfg.MaxConnLifetime = 5 * time.Minute
			poolCfg.MaxConnLifetimeJitter = 30 * time.Second
			poolCfg.MaxConns = int32(numConns)
			poolCfg.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
				m.mu.RLock()
				defer m.mu.RUnlock()
				for name, sql := range m.mu.preparedStatements {
					// Note that calling `Prepare` with a name that has already been
					// prepared is idempotent and short-circuits before doing any
					// communication to the server.
					if _, err := conn.Prepare(ctx, name, sql); err != nil {
						log.Warningf(ctx, "error preparing statement. name=%s sql=%s %v", name, sql, err)
						return false
					}
				}
				return true
			}

			// Disable the automatic prepared statement cache. We've seen a lot of
			// churn in this cache since workloads create many of different queries.
			connCfg := poolCfg.ConnConfig
			connCfg.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
			connCfg.StatementCacheCapacity = 0
			connCfg.DescriptionCacheCapacity = 0
			connCfg.Tracer = &pgxLogger{
				TraceLog: tracelog.TraceLog{
					LogLevel: tracelog.LogLevelWarn,
				},
			}
			p, err := pgxpool.NewWithConfig(ctx, poolCfg)
			if err != nil {
				return nil, err
			}

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
	sem := semaphore.New(100)

	warmupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var numConns int
	for _, p := range m.Pools {
		numConns += int(p.Config().MaxConns)
	}

	numWarmupConns := len(m.Pools) * numConns
	warmupConns := make(chan struct{}, numWarmupConns)

	for _, p := range m.Pools {
		p := p
		for j := 0; j < int(p.Config().MaxConns); j++ {
			g.Go(func() error {
				if err := sem.Acquire(warmupCtx, 1); err != nil {
					warmupConns <- struct{}{}
					return err
				}
				conn, err := p.Acquire(warmupCtx)
				sem.Release(1)
				warmupConns <- struct{}{}
				<-warmupCtx.Done()
				conn.Release()
				return err
			})
		}
	}
	for i := 0; i < numWarmupConns; i++ {
		<-warmupConns
	}
	cancel()
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return m, nil
}

// AddPreparedStatement adds the given sql statement to the map of
// statements that will be prepared when a new connection is retrieved
// from the pool.
func (m *MultiConnPool) AddPreparedStatement(name string, statement string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.preparedStatements[name] = statement
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
